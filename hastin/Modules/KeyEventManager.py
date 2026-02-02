import csv
import threading
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from rich import box
from rich.align import Align
from rich.console import Group
from rich.style import Style
from rich.table import Table
from sqlparse import format as sqlformat
from textual.widgets import Button

from hastin.DataTypes import (
    HotkeyCommands,
    ProcesslistThread,
)
from hastin.Modules.Functions import (
    format_bytes,
    format_number,
    format_query,
)
from hastin.Modules.ManualException import ManualException
from hastin.Modules.Queries import PostgresQueries
from hastin.Widgets.CommandModal import CommandModal
from hastin.Widgets.CommandScreen import CommandScreen
from hastin.Widgets.ThreadScreen import ThreadScreen

if TYPE_CHECKING:
    from hastin.App import HastinApp


class KeyEventManager:
    """This module manages all keyboard event processing.

    This includes both immediate key event handling and background command execution
    in threads.
    """

    def __init__(self, app: "HastinApp"):
        """Initialize the KeyEventManager.

        Args:
            app: Reference to the main HastinApp instance
        """
        self.app = app

        # Debouncing to prevent rapid key presses from overwhelming the system
        self.last_key_time = {}
        self.default_debounce_interval = timedelta(milliseconds=50)

        # Custom debounce intervals for specific keys that trigger expensive operations
        self.key_debounce_intervals = {
            "left_square_bracket": timedelta(milliseconds=100),  # Replay backward
            "right_square_bracket": timedelta(milliseconds=100),  # Replay forward
            "space": timedelta(milliseconds=300),  # Start worker
            "minus": timedelta(milliseconds=300),  # Remove tab (destructive)
        }

    async def process_key_event(self, key: str) -> None:
        """Process a keyboard event and execute the corresponding action.

        This method handles all keyboard shortcuts and commands in the application,
        from panel switching to data filtering to command execution.

        Args:
            key: The key that was pressed
        """
        tab = self.app.tab_manager.active_tab
        if not tab:
            return

        # Apply debouncing to prevent rapid key presses
        now = datetime.now().astimezone()
        debounce_interval = self.key_debounce_intervals.get(key, self.default_debounce_interval)
        last_time = self.last_key_time.get(key, datetime.min.replace(tzinfo=UTC))

        if now - last_time < debounce_interval:
            return  # Key press is too soon, ignore it

        self.last_key_time[key] = now

        screen_data = None
        hastin = tab.hastin

        # Validate key is a valid command (excluding special keys)
        if key not in self.app.command_manager.exclude_keys:
            if not self.app.command_manager.get_commands(hastin.replay_file, hastin.connection_source).get(key):
                self.app.notify(
                    f"Key [$highlight]{key}[/$highlight] is not a valid command",
                    severity="warning",
                )
                return

            # Prevent commands from being run if the secondary connection is processing a query already
            if hastin.secondary_db_connection and hastin.secondary_db_connection.is_running_query:
                self.app.notify("There's already a command running - please wait for it to finish")
                return

            if not hastin.main_db_connection.is_connected() and not hastin.replay_file:
                self.app.notify("You must be connected to a host to use commands")
                return

        if self.app.tab_manager.loading_hostgroups:
            self.app.notify("You can't run commands while hosts are connecting as a hostgroup")
            return

        # Panel switching commands (1-4)
        if key == "1":
            self.app.toggle_panel(hastin.panels.dashboard.name)

        elif key == "2":
            self.app.tab_manager.active_tab.processlist_datatable.clear()
            self.app.toggle_panel(hastin.panels.processlist.name)

        elif key == "3":
            self.app.toggle_panel(hastin.panels.graphs.name)
            self.app.update_graphs(tab.metric_graph_tabs.get_pane(tab.metric_graph_tabs.active).name)

        elif key == "4":
            # Replication panel
            if hastin.replay_file and not hastin.replication_status:
                self.app.notify("This replay file has no replication data")
                return

            if not any(
                [
                    hastin.replica_manager.available_replicas,
                    hastin.replication_status,
                ]
            ):
                self.app.notify("Replication panel has no data to display")
                return

            self.app.toggle_panel(hastin.panels.replication.name)
            tab.toggle_entities_displays()

            if hastin.panels.replication.visible:
                if hastin.replica_manager.available_replicas:
                    # No loading animation necessary for replay mode
                    if not hastin.replay_file:
                        tab.replicas_loading_indicator.display = True
                        tab.replicas_title.update(
                            f"[$white][b]Loading [$highlight]{len(hastin.replica_manager.available_replicas)}"
                            "[/$highlight] replicas...\n"
                        )

                tab.toggle_replication_panel_components()
            else:
                tab.remove_replication_panel_components()

        elif key == "5":
            # Locks panel (pg_locks)
            self.app.toggle_panel(hastin.panels.locks.name)
            if hasattr(tab, "locks_datatable"):
                tab.locks_datatable.clear()

        elif key == "6":
            # Statements panel (pg_stat_statements) - optional
            if not hastin.has_pg_stat_statements:
                self.app.notify(
                    "Statements panel requires pg_stat_statements extension. "
                    "Install and enable it in shared_preload_libraries."
                )
                return

            self.app.toggle_panel(hastin.panels.statements.name)
            if hasattr(tab, "statements_datatable"):
                tab.statements_datatable.clear()

        elif key == "7":
            # PgBouncer panel (combined mode only)
            if not hastin.has_pgbouncer:
                self.app.notify(
                    "PgBouncer panel requires --pgbouncer-host connection. "
                    "Use --pgbouncer-host to enable combined PostgreSQL + PgBouncer monitoring."
                )
                return

            self.app.toggle_panel(hastin.panels.pgbouncer.name)

        # Tab management commands
        elif key == "grave_accent":
            self.app.tab_manager.setup_host_tab(tab)

        elif key == "space":
            if not tab.worker or not tab.worker.is_running:
                if tab.worker_timer:
                    tab.worker_timer.stop()
                self.app.run_worker_main(tab.id)

        elif key == "plus":
            new_tab = await self.app.tab_manager.create_tab(tab_name="New Tab")
            self.app.tab_manager.switch_tab(new_tab.id)
            self.app.tab_manager.setup_host_tab(new_tab)

        elif key == "equals_sign":

            def command_get_input(tab_name):
                tab.manual_tab_name = tab_name
                self.app.tab_manager.rename_tab(tab, tab_name)

            self.app.app.push_screen(
                CommandModal(
                    command=HotkeyCommands.rename_tab,
                    message="What would you like to rename the tab to?",
                ),
                command_get_input,
            )

        elif key == "minus":
            if len(self.app.tab_manager.tabs) == 1:
                self.app.notify("Removing all tabs is not permitted", severity="error")
            else:
                if not self.app.tab_manager.active_tab:
                    self.app.notify("No active tab to remove", severity="error")
                    return

                await self.app.tab_manager.remove_tab(tab)
                await self.app.tab_manager.disconnect_tab(tab=tab, update_topbar=False)

                self.app.notify(
                    f"Tab [$highlight]{tab.name}[/$highlight] [$white]has been removed",
                    severity="success",
                )
                self.app.tab_manager.tabs.pop(tab.id, None)

        # Replay control commands
        elif key == "left_square_bracket":
            if hastin.replay_file:
                self.app.query_one("#back_button", Button).press()

        elif key == "right_square_bracket":
            if hastin.replay_file:
                self.app.query_one("#forward_button", Button).press()

        # Tab navigation
        elif key == "ctrl+a" or key == "ctrl+d":
            if key == "ctrl+a":
                self.app.tab_manager.host_tabs.action_previous_tab()
            elif key == "ctrl+d":
                self.app.tab_manager.host_tabs.action_next_tab()

        # Display toggle commands
        elif key == "a":
            if hastin.show_additional_query_columns:
                hastin.show_additional_query_columns = False
                self.app.notify("Processlist will now hide additional columns")
            else:
                hastin.show_additional_query_columns = True
                self.app.notify("Processlist will now show additional columns")

            self.app.force_refresh_for_replay(need_current_data=True)

        # Filter commands
        elif key == "c":
            hastin.user_filter = None
            hastin.db_filter = None
            hastin.host_filter = None
            hastin.query_time_filter = None
            hastin.query_filter = None

            self.app.force_refresh_for_replay(need_current_data=True)

            self.app.notify("Cleared all filters", severity="success")

        # Database operation commands
        elif key == "d":
            self.execute_command_in_thread(key=key)

        elif key == "D":
            await self.app.tab_manager.disconnect_tab(tab)

        elif key == "E":
            processlist = hastin.processlist_threads_snapshot or hastin.processlist_threads
            if processlist:
                # Extract headers from the first entry's thread_data
                first_entry = next(iter(processlist.values()))
                headers = first_entry.thread_data.keys()

                # Generate the filename with a timestamp prefix
                timestamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
                filename = f"processlist-{timestamp}.csv"

                # Write the CSV to a file
                with open(filename, "w", newline="") as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=headers)

                    # Write the headers and rows
                    writer.writeheader()
                    for process_thread in processlist.values():
                        writer.writerow(process_thread.thread_data)

                self.app.notify(
                    f"Processlist has been exported to CSV file [$highlight]{filename}",
                    severity="success",
                    timeout=10,
                )
            else:
                self.app.notify("There's no processlist data to export", severity="warning")

        elif key == "f":

            def command_get_input(filter_data):
                # Unpack the data from the modal
                filters_mapping = {
                    "User": "user_filter",
                    "Host": "host_filter",
                    "Database": "db_filter",
                    "Minimum Query Time": "query_time_filter",
                    "Partial Query Text": "query_filter",
                }

                filters = dict(zip(filters_mapping.keys(), filter_data, strict=False))

                # Apply filters and notify the user for each valid input
                for filter_name, filter_value in filters.items():
                    if filter_value:
                        if filter_name == "Minimum Query Time":
                            filter_value = int(filter_value)

                        setattr(hastin, filters_mapping[filter_name], filter_value)
                        self.app.notify(
                            f"[b]{filter_name}[/b]: [$b_highlight]{filter_value}[/$b_highlight]",
                            title="Filter applied",
                            severity="success",
                        )

                # Refresh data after applying filters
                self.app.force_refresh_for_replay(need_current_data=True)

            self.app.app.push_screen(
                CommandModal(
                    command=HotkeyCommands.thread_filter,
                    message="Filter threads by field(s)",
                    processlist_data=hastin.processlist_threads_snapshot,
                    host_cache_data=hastin.host_cache,
                ),
                command_get_input,
            )

        elif key == "i":
            if hastin.show_idle_threads:
                hastin.show_idle_threads = False
                hastin.sort_by_time_descending = True

                self.app.notify("Processlist will now hide idle threads")
            else:
                hastin.show_idle_threads = True
                hastin.sort_by_time_descending = False

                self.app.notify("Processlist will now show idle threads")

        elif key == "k":

            def command_get_input(data):
                self.execute_command_in_thread(key=key, additional_data=data)

            self.app.app.push_screen(
                CommandModal(
                    command=HotkeyCommands.thread_kill_by_parameter,
                    message="Kill thread(s)",
                    processlist_data=hastin.processlist_threads_snapshot,
                ),
                command_get_input,
            )

        elif key == "l":
            # Show recent deadlocks from pg_stat_activity/pg_locks
            self.execute_command_in_thread(key=key)

        elif key == "M":

            def command_get_input(filter_data):
                panel = filter_data

                widget = None
                if panel == "processlist":
                    widget = tab.processlist_datatable
                elif panel == "graphs":
                    widget = tab.metric_graph_tabs
                elif panel == "locks":
                    widget = getattr(tab, "locks_datatable", None)
                elif panel == "statements":
                    widget = getattr(tab, "statements_datatable", None)

                if widget:
                    self.app.screen.maximize(widget)

            panel_options = [
                (panel.display_name, panel.name)
                for panel in tab.hastin.panels.get_all_panels()
                if panel.visible and panel.name not in ["dashboard"]
            ]

            self.app.app.push_screen(
                CommandModal(
                    command=HotkeyCommands.maximize_panel,
                    maximize_panel_options=panel_options,
                    message="Maximize a Panel",
                ),
                command_get_input,
            )

        elif key == "p":
            if hastin.replay_file:
                self.app.query_one("#pause_button", Button).press()
            else:
                if not hastin.pause_refresh:
                    hastin.pause_refresh = True
                    self.app.notify(f"Refresh is paused! Press [$b_highlight]{key}[/$b_highlight] again to resume")
                else:
                    hastin.pause_refresh = False
                    self.app.notify("Refreshing has resumed", severity="success")

        elif key == "q":
            self.app.app.exit()

        elif key == "r":

            def command_get_input(refresh_interval):
                hastin.refresh_interval = refresh_interval

                self.app.notify(
                    f"Refresh interval set to [$b_highlight]{refresh_interval}[/$b_highlight] second(s)",
                    severity="success",
                )

            self.app.app.push_screen(
                CommandModal(HotkeyCommands.refresh_interval, message="Refresh Interval"),
                command_get_input,
            )

        elif key == "R":
            hastin.metric_manager.reset()

            self.app.update_graphs(tab.metric_graph_tabs.get_pane(tab.metric_graph_tabs.active).name)
            self.app.notify("Metrics have been reset", severity="success")

        elif key == "s":
            if hastin.sort_by_time_descending:
                hastin.sort_by_time_descending = False
                self.app.notify("Processlist will now sort threads by time in ascending order")
            else:
                hastin.sort_by_time_descending = True
                self.app.notify("Processlist will now sort threads by time in descending order")

            self.app.force_refresh_for_replay(need_current_data=True)

        elif key == "S":
            if hastin.replay_file:
                self.app.query_one("#seek_button", Button).press()
            else:
                # Toggle Statistics/s section in Dashboard
                hastin.show_statistics_per_second = not hastin.show_statistics_per_second
                if hastin.show_statistics_per_second:
                    self.app.notify("Statistics/s section is now visible")
                else:
                    self.app.notify("Statistics/s section is now hidden")

        elif key == "t":

            def command_get_input(data):
                self.execute_command_in_thread(key=key, additional_data=data)

            self.app.app.push_screen(
                CommandModal(
                    command=HotkeyCommands.show_thread,
                    message="Thread Details",
                    processlist_data=hastin.processlist_threads_snapshot,
                ),
                command_get_input,
            )

        elif key == "T":
            # Show only threads with active transactions
            if hastin.show_trxs_only:
                hastin.show_trxs_only = False
                hastin.show_idle_threads = False
                self.app.notify("Processlist will no longer only show threads with active transactions")
            else:
                hastin.show_trxs_only = True
                hastin.show_idle_threads = True
                self.app.notify("Processlist will only show threads with active transactions")

            self.app.force_refresh_for_replay(need_current_data=True)

        elif key == "u":
            self.execute_command_in_thread(key=key)

        elif key == "V":
            global_variable_changes = tab.replay_manager.fetch_all_global_variable_changes()

            if global_variable_changes:
                table = Table(
                    box=box.SIMPLE_HEAVY,
                    show_edge=False,
                    style="table_border",
                )
                table.add_column("Timestamp")
                table.add_column("Variable")
                table.add_column("Old Value", overflow="fold")
                table.add_column("New Value", overflow="fold")

                for (
                    timestamp,
                    variable,
                    old_value,
                    new_value,
                ) in global_variable_changes:
                    table.add_row(
                        f"[dark_gray]{timestamp}",
                        f"[light_blue]{variable}",
                        old_value,
                        new_value,
                    )

                screen_data = Group(
                    Align.center(
                        "[b light_blue]Global Variable Changes[/b light_blue] "
                        f"([b highlight]{table.row_count}[/b highlight])\n"
                    ),
                    table,
                )
            else:
                self.app.notify("There are no global variable changes in this replay")

        elif key == "v":

            def command_get_input(input_variable):
                table_grid = Table.grid()
                table_counter = 1
                variable_counter = 1
                row_counter = 1
                variable_num = 1
                all_tables = []
                tables = {}
                display_global_variables = {}

                for variable, value in hastin.global_variables.items():
                    if input_variable == "all":
                        display_global_variables[variable] = hastin.global_variables[variable]
                    else:
                        if input_variable and input_variable in variable:
                            display_global_variables[variable] = hastin.global_variables[variable]

                max_num_tables = 1 if len(display_global_variables) <= 50 else 2

                # Create the number of tables we want
                while table_counter <= max_num_tables:
                    tables[table_counter] = Table(box=box.HORIZONTALS, show_header=False, style="table_border")
                    tables[table_counter].add_column("")
                    tables[table_counter].add_column("")

                    table_counter += 1

                # Calculate how many global_variables per table
                row_per_count = len(display_global_variables) // max_num_tables

                # Loop global_variables
                for variable, value in display_global_variables.items():
                    tables[variable_num].add_row(f"[label]{variable}", str(value))

                    if variable_counter == row_per_count and row_counter != max_num_tables:
                        row_counter += 1
                        variable_counter = 0
                        variable_num += 1

                    variable_counter += 1

                # Put all the variable data from dict into an array
                all_tables = [table_data for table_data in tables.values() if table_data]

                # Add the data into a single tuple for add_row
                if display_global_variables:
                    table_grid.add_row(*all_tables)
                    screen_data = Align.center(table_grid)

                    self.app.app.push_screen(
                        CommandScreen(
                            hastin.connection_status,
                            hastin.app_version,
                            hastin.host_with_port,
                            screen_data,
                        )
                    )
                else:
                    if input_variable:
                        self.app.notify(
                            f"No variable(s) found that match [$b_highlight]{input_variable}[/$b_highlight]"
                        )

            self.app.app.push_screen(
                CommandModal(
                    HotkeyCommands.variable_search,
                    message="Specify a variable to wildcard search",
                ),
                command_get_input,
            )

        elif key == "Z":
            # Table sizes in PostgreSQL
            self.execute_command_in_thread(key=key)

        elif key == "z":
            if hastin.host_cache:
                table = Table(
                    box=box.SIMPLE_HEAVY,
                    show_edge=False,
                    style="table_border",
                )
                table.add_column("Host/IP")
                table.add_column("Hostname (if resolved)")

                for ip, addr in hastin.host_cache.items():
                    if ip:
                        table.add_row(ip, addr)

                screen_data = Group(
                    Align.center(
                        f"[b light_blue]Host Cache[/b light_blue] "
                        f"([b highlight]{len(hastin.host_cache)}[/b highlight])\n"
                    ),
                    table,
                )
            else:
                self.app.notify("There are currently no hosts resolved")

        if screen_data:
            self.app.app.push_screen(
                CommandScreen(
                    hastin.connection_status,
                    hastin.app_version,
                    hastin.host_with_port,
                    screen_data,
                )
            )

    def execute_command_in_thread(self, key: str, additional_data=None) -> None:
        """Execute a command in a background thread.

        This method creates a daemon thread to run the command without blocking
        the UI, using call_from_thread to safely interact with Textual.

        Args:
            key: The command key that was pressed
            additional_data: Optional additional data for the command
        """

        def _run_command():
            """Internal worker function that executes in a background thread."""
            self._execute_command(key, additional_data)

        thread = threading.Thread(target=_run_command, daemon=True)
        thread.start()

    def _execute_command(self, key: str, additional_data=None) -> None:
        """Internal implementation of command execution."""
        tab = self.app.tab_manager.active_tab
        hastin = tab.hastin

        # These are the screens to display we use for the commands
        def show_command_screen():
            self.app.app.push_screen(
                CommandScreen(
                    hastin.connection_status,
                    hastin.app_version,
                    hastin.host_with_port,
                    screen_data,
                )
            )

        def show_thread_screen():
            self.app.app.push_screen(
                ThreadScreen(
                    connection_status=hastin.connection_status,
                    app_version=hastin.app_version,
                    host=hastin.host_with_port,
                    thread_table=thread_table,
                    user_thread_attributes_table=None,
                    query=formatted_query,
                    explain_data=explain_data,
                    explain_json_data=explain_json_data,
                    explain_failure=explain_failure,
                    transaction_history_table=None,
                )
            )

        self.app.call_from_thread(tab.spinner.show)

        try:
            if key == "d":
                # List databases in PostgreSQL
                tables = {}
                all_tables = []

                hastin.secondary_db_connection.execute(PostgresQueries.databases)
                databases = hastin.secondary_db_connection.fetchall()
                db_count = len(databases)

                # Determine how many tables to provide data
                max_num_tables = 1 if db_count <= 20 else 3

                # Calculate how many databases per table
                row_per_count = db_count // max_num_tables if max_num_tables > 0 else db_count

                # Create dictionary of tables
                for table_counter in range(1, max_num_tables + 1):
                    table_box = box.HORIZONTALS
                    if max_num_tables == 1:
                        table_box = None

                    tables[table_counter] = Table(box=table_box, show_header=False, style="table_border")
                    tables[table_counter].add_column("")

                # Loop over databases
                db_counter = 1
                table_counter = 1

                for database in databases:
                    tables[table_counter].add_row(database["datname"])
                    db_counter += 1

                    if db_counter > row_per_count and table_counter < max_num_tables:
                        table_counter += 1
                        db_counter = 1

                # Collect table data into an array
                all_tables = [table_data for table_data in tables.values() if table_data]

                table_grid = Table.grid()
                table_grid.add_row(*all_tables)

                screen_data = Group(
                    Align.center(f"[b light_blue]Databases[/b light_blue] ([b highlight]{db_count}[/b highlight])\n"),
                    Align.center(table_grid),
                )

                self.app.call_from_thread(show_command_screen)

            elif key == "k":
                # Kill thread(s) in PostgreSQL using pg_terminate_backend or pg_cancel_backend
                (
                    kill_by_id,
                    kill_by_username,
                    kill_by_host,
                    kill_by_age_range,
                    age_range_lower_limit,
                    age_range_upper_limit,
                    kill_by_query_text,
                    include_sleeping_queries,
                ) = additional_data

                if kill_by_id:
                    try:
                        success = hastin.secondary_db_connection.terminate_backend(int(kill_by_id))
                        if success:
                            self.app.notify(
                                f"Terminated backend PID [$b_highlight]{kill_by_id}[/$b_highlight]",
                                severity="success",
                            )
                        else:
                            self.app.notify(
                                f"Failed to terminate PID [$b_highlight]{kill_by_id}[/$b_highlight]",
                                severity="error",
                            )
                    except ManualException as e:
                        self.app.notify(e.reason, title="Error terminating backend", severity="error")
                else:
                    threads_killed = 0
                    states_to_kill = ["active"]

                    if include_sleeping_queries:
                        states_to_kill.extend(["idle", "idle in transaction"])

                    # Make a copy of the threads snapshot to avoid modification during next refresh polling
                    threads = hastin.processlist_threads_snapshot.copy()

                    for thread_id, thread in threads.items():
                        thread: ProcesslistThread
                        try:
                            if (
                                thread.state in states_to_kill
                                and (not kill_by_username or kill_by_username == thread.user)
                                and (not kill_by_host or kill_by_host == thread.host)
                                and (
                                    not kill_by_age_range
                                    or age_range_lower_limit <= thread.time <= age_range_upper_limit
                                )
                                and (
                                    not kill_by_query_text or kill_by_query_text in (thread.formatted_query.code or "")
                                )
                            ):
                                success = hastin.secondary_db_connection.terminate_backend(int(thread_id))
                                if success:
                                    threads_killed += 1
                        except ManualException as e:
                            self.app.notify(
                                e.reason,
                                title=f"Error terminating PID {thread_id}",
                                severity="error",
                            )

                    if threads_killed:
                        self.app.notify(f"Terminated [$highlight]{threads_killed}[/$highlight] backend(s)")
                    else:
                        self.app.notify("No backends were terminated")

            elif key == "l":
                # Show blocking queries / lock waits
                hastin.secondary_db_connection.execute(PostgresQueries.blocked_queries)
                blocked_queries = hastin.secondary_db_connection.fetchall()

                if blocked_queries:
                    header_style = Style(bold=True)
                    table = Table(box=box.SIMPLE_HEAVY, style="table_border", show_edge=False)
                    table.add_column("Blocked PID", header_style=header_style)
                    table.add_column("Blocked User", header_style=header_style)
                    table.add_column("Blocked Query", header_style=header_style, max_width=40, overflow="fold")
                    table.add_column("Blocking PID", header_style=header_style)
                    table.add_column("Blocking User", header_style=header_style)
                    table.add_column("Blocking Query", header_style=header_style, max_width=40, overflow="fold")

                    for row in blocked_queries:
                        table.add_row(
                            str(row.get("blocked_pid", "")),
                            row.get("blocked_user", ""),
                            (row.get("blocked_query", "") or "")[:100],
                            str(row.get("blocking_pid", "")),
                            row.get("blocking_user", ""),
                            (row.get("blocking_query", "") or "")[:100],
                        )

                    screen_data = Group(
                        Align.center(
                            f"[b light_blue]Blocked Queries[/b light_blue] "
                            f"([b highlight]{len(blocked_queries)}[/b highlight])\n"
                        ),
                        Align.center(table),
                    )
                else:
                    screen_data = Align.center("[green]No blocked queries detected[/green]")

                self.app.call_from_thread(show_command_screen)

            elif key == "t":
                # Thread details for PostgreSQL
                formatted_query = ""
                explain_failure = ""
                explain_data = ""
                explain_json_data = ""

                thread_table = Table(box=None, show_header=False)
                thread_table.add_column("")
                thread_table.add_column("", overflow="fold")

                thread_id = additional_data
                thread_data: ProcesslistThread = hastin.processlist_threads_snapshot.get(thread_id)
                if not thread_data:
                    self.app.notify(
                        f"PID [$highlight]{thread_id}[/$highlight] was not found",
                        severity="error",
                    )
                    tab.spinner.hide()
                    return

                thread_table.add_row("[label]PID", thread_id)
                thread_table.add_row("[label]User", thread_data.user or "")
                thread_table.add_row("[label]Host", thread_data.host or "")
                thread_table.add_row("[label]Database", thread_data.db or "")
                thread_table.add_row("[label]Application", thread_data.application or "")
                thread_table.add_row("[label]State", thread_data.state or "")
                thread_table.add_row("[label]Duration", str(timedelta(seconds=thread_data.time)).zfill(8))
                thread_table.add_row("[label]Wait Event Type", thread_data.wait_event_type or "")
                thread_table.add_row("[label]Wait Event", thread_data.wait_event or "")
                thread_table.add_row("[label]Backend Type", thread_data.backend_type or "")

                if thread_data.formatted_query and thread_data.formatted_query.code:
                    query = sqlformat(thread_data.formatted_query.code, reindent_aligned=True)
                    query_db = thread_data.db

                    formatted_query = format_query(query, minify=False)

                    if query_db:
                        try:
                            # Try to explain the query
                            hastin.secondary_db_connection.execute(f"EXPLAIN {thread_data.formatted_query.code}")
                            explain_data = hastin.secondary_db_connection.fetchall()

                            hastin.secondary_db_connection.execute(
                                f"EXPLAIN (FORMAT JSON) {thread_data.formatted_query.code}"
                            )
                            explain_fetched_json_data = hastin.secondary_db_connection.fetchone()
                            if explain_fetched_json_data:
                                explain_json_data = str(explain_fetched_json_data.get("QUERY PLAN", ""))
                        except ManualException as e:
                            explain_failure = f"[b][indian_red]EXPLAIN ERROR:[/b] [indian_red]{e.reason}"

                self.app.call_from_thread(show_thread_screen)

            elif key == "u":
                # User statistics for PostgreSQL
                title = "Database Users"

                hastin.secondary_db_connection.execute(PostgresQueries.user_stats)
                users = hastin.secondary_db_connection.fetchall()

                header_style = Style(bold=True)
                table = Table(
                    header_style="b",
                    box=box.SIMPLE_HEAVY,
                    show_edge=False,
                    style="table_border",
                )
                table.add_column("User", header_style=header_style)
                table.add_column("Active", header_style=header_style)
                table.add_column("Idle", header_style=header_style)
                table.add_column("Idle in Txn", header_style=header_style)
                table.add_column("Total", header_style=header_style)

                for user in users:
                    table.add_row(
                        user.get("usename", ""),
                        str(user.get("active", 0)),
                        str(user.get("idle", 0)),
                        str(user.get("idle_in_transaction", 0)),
                        str(user.get("total", 0)),
                    )

                screen_data = Group(
                    Align.center(f"[b light_blue]{title} ([highlight]{len(users)}[/highlight])\n"),
                    Align.center(table),
                )

                self.app.call_from_thread(show_command_screen)

            elif key == "Z":
                # Table sizes in PostgreSQL
                hastin.secondary_db_connection.execute(PostgresQueries.table_sizes)
                table_sizes_data = hastin.secondary_db_connection.fetchall()

                header_style = Style(bold=True)
                table = Table(
                    header_style="b",
                    box=box.SIMPLE_HEAVY,
                    show_edge=False,
                    style="table_border",
                )
                table.add_column("Table", header_style=header_style)
                table.add_column("Total Size", header_style=header_style)
                table.add_column("Table Size", header_style=header_style)
                table.add_column("Index Size", header_style=header_style)
                table.add_column("Rows (est.)", header_style=header_style)

                for row in table_sizes_data:
                    table.add_row(
                        f"[dark_gray]{row.get('schemaname', '')}[/dark_gray].{row.get('tablename', '')}",
                        format_bytes(row.get("total_size", 0)),
                        format_bytes(row.get("table_size", 0)),
                        format_bytes(row.get("index_size", 0)),
                        format_number(row.get("row_estimate", 0)),
                    )

                screen_data = Group(
                    Align.center(
                        f"[b light_blue]Table Sizes[/b light_blue] ([highlight]{len(table_sizes_data)}[/highlight])\n"
                    ),
                    Align.center(table),
                )

                self.app.call_from_thread(show_command_screen)

        except ManualException as e:
            self.app.notify(
                e.reason,
                title=f"Error running command '{key}'",
                severity="error",
                timeout=10,
            )

        self.app.call_from_thread(tab.spinner.hide)
