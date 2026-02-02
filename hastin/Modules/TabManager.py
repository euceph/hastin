import copy
import os
import uuid

from textual.app import App
from textual.containers import (
    Center,
    Container,
    Horizontal,
    ScrollableContainer,
    VerticalScroll,
)
from textual.content import Content
from textual.timer import Timer
from textual.widgets import (
    Button,
    DataTable,
    Label,
    LoadingIndicator,
    ProgressBar,
    Sparkline,
    Static,
    Switch,
    TabbedContent,
    TabPane,
    Tabs,
)
from textual.widgets import Tab as TabWidget
from textual.worker import Worker

import hastin.Modules.MetricManager as MetricManager
from hastin.DataTypes import ConnectionSource, ConnectionStatus, Panels
from hastin.Hastin import Hastin
from hastin.Modules.ArgumentParser import Config, HostGroupMember
from hastin.Modules.ManualException import ManualException
from hastin.Modules.ReplayManager import ReplayManager
from hastin.Widgets.SpinnerWidget import SpinnerWidget
from hastin.Widgets.TabSetupModal import TabSetupModal
from hastin.Widgets.TopBar import TopBar


class Tab:
    def __init__(
        self,
        id: str,
        name: str,
        hastin: Hastin = None,
        manual_tab_name: str = None,
        replay_manager: ReplayManager = None,
    ):
        self.id = id
        self.name = name
        self.hastin = hastin
        self.manual_tab_name = manual_tab_name
        self.replay_manager = replay_manager

        self.worker: Worker = None
        self.worker_timer: Timer = None
        self.worker_cancel_error: ManualException = None

        self.replay_manual_control: bool = False

        self.replicas_worker: Worker = None
        self.replicas_worker_timer: Timer = None

    def save_references_to_components(self):
        app = self.hastin.app

        self.main_container = app.query_one("#main_container", VerticalScroll)
        self.metric_graph_tabs = app.query_one("#metric_graph_tabs", TabbedContent)
        self.loading_indicator = app.query_one("#loading_indicator", LoadingIndicator)
        self.sparkline = app.query_one("#panel_dashboard_queries_qps", Sparkline)
        self.panel_dashboard = app.query_one("#panel_dashboard", Container)
        self.panel_graphs = app.query_one("#panel_graphs", Container)
        self.panel_replication = app.query_one("#panel_replication", Container)
        self.panel_processlist = app.query_one("#panel_processlist", Container)
        self.panel_locks = app.query_one("#panel_locks", Container)
        self.panel_statements = app.query_one("#panel_statements", Container)
        self.panel_pgbouncer = app.query_one("#panel_pgbouncer", Container)

        # PgBouncer panels
        self.panel_pgbouncer_dashboard = app.query_one("#panel_pgbouncer_dashboard", Container)
        self.panel_pgbouncer_pools = app.query_one("#panel_pgbouncer_pools", Container)
        self.panel_pgbouncer_clients = app.query_one("#panel_pgbouncer_clients", Container)
        self.panel_pgbouncer_servers = app.query_one("#panel_pgbouncer_servers", Container)

        self.spinner = app.query_one("#spinner", SpinnerWidget)
        self.spinner.hide()

        self.processlist_title = app.query_one("#processlist_title", Label)
        self.processlist_datatable = app.query_one("#processlist_data", DataTable)
        self.locks_title = app.query_one("#locks_title", Label)
        self.locks_datatable = app.query_one("#locks_datatable", DataTable)
        self.statements_title = app.query_one("#statements_title", Label)
        self.statements_datatable = app.query_one("#statements_datatable", DataTable)

        self.dashboard_replay_container = app.query_one("#dashboard_replay_container", Container)
        self.dashboard_replay_progressbar = app.query_one("#dashboard_replay_progressbar", ProgressBar)
        self.dashboard_replay_start_end = app.query_one("#dashboard_replay_start_end", Static)
        self.dashboard_replay = app.query_one("#dashboard_replay", Static)
        self.dashboard_section_1 = app.query_one("#dashboard_section_1", Static)
        self.dashboard_section_2 = app.query_one("#dashboard_section_2", Static)
        self.dashboard_section_3 = app.query_one("#dashboard_section_3", Static)
        self.dashboard_section_4 = app.query_one("#dashboard_section_4", Static)
        self.dashboard_section_5 = app.query_one("#dashboard_section_5", Static)
        self.dashboard_section_6 = app.query_one("#dashboard_section_6", Static)

        self.replicas_grid = app.query_one("#replicas_grid", Container)
        self.replicas_container = app.query_one("#replicas_container", Container)
        self.replicas_title = app.query_one("#replicas_title", Label)
        self.replicas_loading_indicator = app.query_one("#replicas_loading_indicator", LoadingIndicator)

        self.replication_title = app.query_one("#replication_title", Label)
        self.replication_container = app.query_one("#replication_container", Container)
        self.replication_variables = app.query_one("#replication_variables", Label)
        self.replication_status = app.query_one("#replication_status", Static)
        self.replication_thread_applier_container = app.query_one(
            "#replication_thread_applier_container", ScrollableContainer
        )
        self.replication_thread_applier = app.query_one("#replication_thread_applier", Static)

    def get_panel_widget(self, panel_name: str) -> Container:
        return getattr(self, f"panel_{panel_name}")

    def refresh_replay_dashboard_section(self):
        if not self.hastin.replay_file:
            return

        min_timestamp = self.replay_manager.min_replay_timestamp
        max_timestamp = self.replay_manager.max_replay_timestamp
        current_timestamp = self.replay_manager.current_replay_timestamp

        # Highlight if the max timestamp matches the current timestamp
        max_timestamp = (
            f"[b][$green]{max_timestamp}[/b][$green]" if max_timestamp == current_timestamp else max_timestamp
        )

        # Update the dashboard title with the timestamp of the replay event
        self.dashboard_replay.update(
            f"[b]Replay[/b] ([$dark_gray]{os.path.basename(self.hastin.replay_file)}[/$dark_gray])"
        )
        self.dashboard_replay_start_end.update(
            f"{min_timestamp} [$b_highlight]<-[/$b_highlight] "
            f"[$b_light_blue]{current_timestamp}[/$b_light_blue] [$b_highlight]->[/$b_highlight] "
            f"{max_timestamp}"
        )

        # Update the progress bar with the current replay progress
        if self.replay_manager.current_replay_id == self.replay_manager.min_replay_id:
            current_position = 0
        else:
            current_position = self.replay_manager.current_replay_id - self.replay_manager.min_replay_id + 1

        self.dashboard_replay_progressbar.update(
            progress=current_position,
            total=self.replay_manager.total_replay_rows,
        )

    def toggle_entities_displays(self):
        if self.hastin.system_utilization:
            self.dashboard_section_6.display = True
            if not self.metric_graph_tabs.get_tab("graph_tab_system").display:
                self.metric_graph_tabs.show_tab("graph_tab_system")
                # Switch to System tab since it's the first/default tab
                self.metric_graph_tabs.active = "graph_tab_system"
        else:
            self.dashboard_section_6.display = False
            if self.metric_graph_tabs.get_tab("graph_tab_system").display:
                self.metric_graph_tabs.hide_tab("graph_tab_system")

        # PgBouncer mode: show PgBouncer graphs, hide PostgreSQL-specific graphs
        if self.hastin.connection_source == ConnectionSource.pgbouncer:
            # Hide all PostgreSQL-specific graph tabs
            pg_tabs = [
                "graph_tab_pg_transactions",
                "graph_tab_pg_tuples",
                "graph_tab_pg_block_io",
                "graph_tab_pg_cache",
                "graph_tab_pg_connections",
                "graph_tab_pg_checkpoints",
                "graph_tab_pg_temp_files",
                "graph_tab_replication_lag",
            ]
            for tab_name in pg_tabs:
                try:
                    if self.metric_graph_tabs.get_tab(tab_name).display:
                        self.metric_graph_tabs.hide_tab(tab_name)
                except Exception:
                    pass  # Tab may not exist

            # Show PgBouncer-specific graph tabs
            pgbouncer_tabs = [
                "graph_tab_pgbouncer_connections",
                "graph_tab_pgbouncer_traffic",
            ]
            for tab_name in pgbouncer_tabs:
                try:
                    if not self.metric_graph_tabs.get_tab(tab_name).display:
                        self.metric_graph_tabs.show_tab(tab_name)
                except Exception:
                    pass  # Tab may not exist
            return

        # Show replication section in dashboard if replication data is available
        pg_sources = (ConnectionSource.postgresql, ConnectionSource.rds, ConnectionSource.aurora)
        if self.hastin.connection_source in pg_sources:
            if self.hastin.replication_status and not self.hastin.panels.replication.visible:
                self.dashboard_section_5.display = True
            else:
                self.dashboard_section_5.display = False

            if self.hastin.replication_status:
                if not self.metric_graph_tabs.get_tab("graph_tab_replication_lag").display:
                    self.metric_graph_tabs.show_tab("graph_tab_replication_lag")
            else:
                if self.metric_graph_tabs.get_tab("graph_tab_replication_lag").display:
                    self.metric_graph_tabs.hide_tab("graph_tab_replication_lag")

    def toggle_metric_graph_tabs_display(self):
        self.main_container.display = True

        # Hide/show the tabs that are available for the current connection source
        for metric_instance in self.hastin.metric_manager.metrics.__dict__.values():
            tab_name = f"graph_tab_{metric_instance.tab_name}"
            try:
                if self.hastin.connection_source in metric_instance.connection_source:
                    self.metric_graph_tabs.show_tab(tab_name)
                else:
                    self.metric_graph_tabs.hide_tab(tab_name)
            except Exception:
                pass  # Tab may not exist yet

        # Only show the replay section if we're in replay mode
        if self.hastin.replay_file:
            if not self.dashboard_replay_container.display:
                self.dashboard_replay_container.display = True
        else:
            if self.dashboard_replay_container.display:
                self.dashboard_replay_container.display = False

        # Loop the metrics and update the graph switch values based on the tab's metric data so each tab can have
        # its own set of visible metrics
        for metric_instance_name, metric_instance in self.hastin.metric_manager.metrics.__dict__.items():
            for metric, metric_data in metric_instance.__dict__.items():
                if (
                    isinstance(metric_data, MetricManager.MetricData)
                    and metric_data.graphable
                    and metric_data.create_switch
                ):
                    switch = self.hastin.app.query_one(
                        f"#switch_container_{metric_instance.tab_name} #{metric_instance_name}-{metric}", Switch
                    )
                    switch.value = metric_data.visible

    def toggle_replication_panel_components(self):
        app = self.hastin.app

        def toggle_container_display(selector: str, container: Container, items):
            container.display = bool(items)
            for component in app.query(selector):
                component.display = self.id in component.id

        toggle_container_display(
            ".replica_container", self.replicas_container, self.hastin.replica_manager.available_replicas
        )

    def remove_replication_panel_components(self):
        components = [
            f".replica_container_{self.id}",
        ]
        for component in components:
            for container in self.hastin.app.query(component):
                container.remove()

    def layout_graphs(self):
        # PostgreSQL graph layouts - can be customized as needed
        pass

        self.graph_system_cpu.styles.width = "50%"
        self.graph_system_network.styles.width = "50%"
        self.graph_system_memory.styles.width = "50%"
        self.graph_system_disk_io.styles.width = "50%"


class TabManager:
    def __init__(self, app: App, config: Config):
        self.app = app
        self.config = config

        self.active_tab: Tab = None
        self.tabs: dict[str, Tab] = {}

        self.host_tabs = self.app.query_one("#host_tabs", Tabs)

        self.loading_hostgroups: bool = False
        self.last_replay_time: int = 0

        self.topbar = self.app.query_one(TopBar)

    def update_connection_status(self, tab: Tab, connection_status: ConnectionStatus):
        tab.hastin.connection_status = connection_status
        self.update_topbar(tab=tab)
        self.rename_tab(tab)

    def update_topbar(self, tab: Tab):
        hastin = tab.hastin

        if hastin.daemon_mode:
            return

        if tab.id == self.active_tab.id:
            if hastin.connection_status:
                self.topbar.connection_status = hastin.connection_status
                self.topbar.host = hastin.host_with_port

                if (
                    hastin.record_for_replay
                    and tab.replay_manager
                    and hastin.connection_status != ConnectionStatus.disconnected
                ):
                    self.topbar.replay_file_size = tab.replay_manager.replay_file_size
                else:
                    self.topbar.replay_file_size = None
            else:
                self.topbar.replay_file_size = None
                self.topbar.connection_status = None
                self.topbar.host = ""

    def generate_tab_id(self) -> str:
        tab_id = str(uuid.uuid4()).replace("-", "")
        if tab_id[0].isdigit():
            tab_id = "a" + tab_id
        return tab_id

    async def create_ui_widgets(self):
        if self.config.daemon_mode:
            return

        await self.app.mount(
            LoadingIndicator(id="loading_indicator"),
            VerticalScroll(
                SpinnerWidget(id="spinner", text="Processing command"),
                Center(
                    Container(
                        Static(id="dashboard_replay", classes="dashboard_replay"),
                        Static(id="dashboard_replay_start_end", classes="dashboard_replay"),
                        Horizontal(
                            Button("⏪ Back", id="back_button", classes="replay_button"),
                            Button("⏸️  Pause", id="pause_button", classes="replay_button"),
                            Button("⏩ Forward", id="forward_button", classes="replay_button"),
                            Button("🔍 Seek", id="seek_button", classes="replay_button"),
                            classes="replay_buttons",
                        ),
                        ProgressBar(
                            id="dashboard_replay_progressbar", total=100, show_percentage=False, show_eta=False
                        ),
                        id="dashboard_replay_container",
                        classes="dashboard_replay",
                    )
                ),
                Container(
                    Center(
                        Static(id="dashboard_section_1", classes="panel_container"),
                        Static(id="dashboard_section_6", classes="panel_container"),
                        Static(id="dashboard_section_2", classes="panel_container"),
                        Static(id="dashboard_section_3", classes="panel_container"),
                        Static(id="dashboard_section_5", classes="panel_container"),
                        Static(id="dashboard_section_4", classes="panel_container"),
                    ),
                    Sparkline([], id="panel_dashboard_queries_qps"),
                    id="panel_dashboard",
                    classes="dashboard",
                ),
                Container(Label(id="metric_graphs_title"), TabbedContent(id="metric_graph_tabs"), id="panel_graphs"),
                Container(
                    Container(
                        Label(id="replication_title"),
                        Label(id="replication_variables"),
                        Center(
                            ScrollableContainer(Static(id="replication_status"), classes="replication_status"),
                            ScrollableContainer(
                                Static(id="replication_thread_applier"),
                                id="replication_thread_applier_container",
                                classes="replication_thread_applier",
                            ),
                        ),
                        id="replication_container",
                        classes="replication",
                    ),
                    Container(
                        Label(id="replicas_title"),
                        LoadingIndicator(id="replicas_loading_indicator"),
                        Container(id="replicas_grid"),
                        id="replicas_container",
                        classes="replicas",
                    ),
                    id="panel_replication",
                    classes="replication_panel",
                ),
                Container(
                    Label(id="processlist_title"),
                    DataTable(id="processlist_data", show_cursor=False),
                    id="panel_processlist",
                    classes="panel_container",
                ),
                Container(
                    Label(id="locks_title"),
                    DataTable(id="locks_datatable", show_cursor=False, zebra_stripes=True),
                    id="panel_locks",
                    classes="panel_container",
                ),
                Container(
                    Label(id="statements_title"),
                    DataTable(id="statements_datatable", show_cursor=False),
                    id="panel_statements",
                    classes="panel_container",
                ),
                # PgBouncer panel (used in combined mode - PostgreSQL + PgBouncer)
                Container(
                    Label(id="pgbouncer_title"),
                    Static(id="pgbouncer_panel_content"),
                    id="panel_pgbouncer",
                    classes="panel_container",
                ),
                # PgBouncer panels (used in standalone mode)
                Container(
                    Static(id="pgbouncer_dashboard_content"),
                    id="panel_pgbouncer_dashboard",
                    classes="dashboard",
                ),
                Container(
                    Static(id="pgbouncer_pools_content"),
                    id="panel_pgbouncer_pools",
                    classes="panel_container",
                ),
                Container(
                    Static(id="pgbouncer_clients_content"),
                    id="panel_pgbouncer_clients",
                    classes="panel_container",
                ),
                Container(
                    Static(id="pgbouncer_servers_content"),
                    id="panel_pgbouncer_servers",
                    classes="panel_container",
                ),
                classes="tab",
                id="main_container",
            ),
        )

        self.app.query_one("#main_container").display = False
        self.app.query_one("#loading_indicator").display = False

        panels = Panels()
        self.app.query_one("#metric_graphs_title", Label).update(panels.get_panel_title(panels.graphs.name))
        self.app.query_one("#replication_title", Label).update(panels.get_panel_title(panels.replication.name))
        self.app.query_one("#locks_title", Label).update(panels.get_panel_title(panels.locks.name))
        self.app.query_one("#statements_title", Label).update(panels.get_panel_title(panels.statements.name))
        self.app.query_one("#pgbouncer_title", Label).update(panels.get_panel_title(panels.pgbouncer.name))

        # Loop the metric instances and create the graph tabs
        metric_manager = MetricManager.MetricManager(None)
        for metric_instance_name, metric_instance in metric_manager.metrics.__dict__.items():
            metric_tab_name = metric_instance.tab_name
            graph_names = metric_instance.graphs
            graph_tab_name = metric_instance.graph_tab_name

            if not self.app.query(f"#graph_tab_{metric_tab_name}"):
                await self.app.query_one("#metric_graph_tabs", TabbedContent).add_pane(
                    TabPane(
                        graph_tab_name,
                        Label(id=f"metric_graph_stats_{metric_tab_name}", classes="metric_graph_stats"),
                        Horizontal(id=f"metric_graph_container_{metric_tab_name}", classes="metric_graph_container"),
                        Horizontal(
                            id=f"switch_container_{metric_tab_name}",
                            classes="switch_container switch_container",
                        ),
                        id=f"graph_tab_{metric_tab_name}",
                        name=metric_tab_name,
                    )
                )

            for graph_name in graph_names:
                graph_container = (
                    "metric_graph_container2"
                    if graph_name in ["graph_system_network", "graph_system_disk_io"]
                    else "metric_graph_container"
                )

                # Add graph_container2 only if it's needed
                if not self.app.query(f"#{graph_container}_{metric_tab_name}"):
                    if graph_container == "metric_graph_container2":
                        await self.app.query_one(f"#graph_tab_{metric_tab_name}", TabPane).mount(
                            Horizontal(id=f"{graph_container}_{metric_tab_name}", classes="metric_graph_container2"),
                            after=1,
                        )

                await self.app.query_one(f"#{graph_container}_{metric_tab_name}", Horizontal).mount(
                    MetricManager.Graph(id=f"{graph_name}", classes="panel_data")
                )

            for metric, metric_data in metric_instance.__dict__.items():
                if not self.app.query(f"#switch_container_{metric_tab_name} #{metric_instance_name}-{metric}"):
                    if (
                        isinstance(metric_data, MetricManager.MetricData)
                        and metric_data.graphable
                        and metric_data.create_switch
                    ):
                        await self.app.query_one(f"#switch_container_{metric_tab_name}", Horizontal).mount(
                            Label(metric_data.label)
                        )
                        await self.app.query_one(f"#switch_container_{metric_tab_name}", Horizontal).mount(
                            Switch(animate=False, id=f"{metric_instance_name}-{metric}", name=metric_tab_name)
                        )

                        # Toggle the switch if the metric is visible (means to enable it by default)
                        if metric_data.visible:
                            self.app.query_one(
                                f"#switch_container_{metric_tab_name} #{metric_instance_name}-{metric}", Switch
                            ).toggle()

    async def create_tab(
        self, tab_name: str = None, hostgroup_member: HostGroupMember = None, switch_tab: bool = True
    ) -> Tab:
        if len(self.app.screen_stack) > 1:
            return

        tab_id = self.generate_tab_id()

        # Create a new tab instance
        tab = Tab(id=tab_id, name=tab_name)

        # If we're using hostgroups
        config = copy.deepcopy(self.config)
        if hostgroup_member and self.config.hostgroup_hosts:
            config.replay_file = None
            config.host = hostgroup_member.host
            config.port = hostgroup_member.port
            tab.manual_tab_name = hostgroup_member.tab_title

            # If the hostgroup member has a credential profile, update config with its credentials
            credential_profile_data = self.config.credential_profiles.get(hostgroup_member.credential_profile)
            if credential_profile_data:
                config.credential_profile = hostgroup_member.credential_profile

                if credential_profile_data.user:
                    config.user = credential_profile_data.user
                if credential_profile_data.password:
                    config.password = credential_profile_data.password
                if credential_profile_data.socket:
                    config.socket = credential_profile_data.socket
                if credential_profile_data.ssl:
                    config.ssl = credential_profile_data.ssl

        hastin = Hastin(config=config, app=self.app)
        hastin.tab_id = tab_id
        tab.hastin = hastin

        # If we're in daemon mode, stop here since we don't need to
        # do anything else with the UI
        if hastin.daemon_mode:
            self.active_tab = tab
            self.tabs[tab_id] = tab

            for panel in hastin.daemon_mode_panels:
                getattr(hastin.panels, panel).visible = True

            return tab

        tab.save_references_to_components()

        # Create the tab in the UI
        intial_tab_name = "" if hostgroup_member else tab_name
        self.host_tabs.add_tab(TabWidget(intial_tab_name, id=tab_id))

        # Loop the metric instances and save references to the graphs and its labels
        for metric_instance in hastin.metric_manager.metrics.__dict__.values():
            metric_tab_name = metric_instance.tab_name
            graph_names = metric_instance.graphs

            # Save references graph's labels
            setattr(tab, metric_tab_name, self.app.query_one(f"#metric_graph_stats_{metric_tab_name}"))

            # Save references to the graphs
            for graph_name in graph_names:
                setattr(tab, graph_name, self.app.query_one(f"#{graph_name}"))

        if tab.manual_tab_name:
            self.rename_tab(tab, tab.manual_tab_name)

        tab.replication_container.display = False
        tab.replicas_container.display = False

        # By default, hide all the panels
        for panel in tab.hastin.panels.all():
            self.app.query_one(f"#panel_{panel}").display = False

        # Set panels to be visible for the ones the user specifies
        for panel in hastin.startup_panels:
            self.app.query_one(f"#panel_{panel}").display = True
            getattr(hastin.panels, panel).visible = True

        # Set what marker we use for graphs
        graphs = self.app.query(MetricManager.Graph)
        for graph in graphs:
            graph.marker = hastin.graph_marker

        tab.sparkline.data = [0]

        self.tabs[tab_id] = tab

        if switch_tab:
            self.switch_tab(tab_id)

        return tab

    async def remove_tab(self, tab: Tab):
        self.host_tabs.remove_tab(tab.id)

    def rename_tab(self, tab: Tab, manual_name: str = None):
        if tab.hastin.daemon_mode:
            return

        new_name = None
        if not manual_name and not tab.manual_tab_name:
            # host_with_port is the full host:port string, we want to split & truncate it to 24 characters
            host = tab.hastin.host_with_port.split(":")[0][:24]
            if not host:
                return

            # If the last character isn't a letter or number, remove it
            if not host[-1].isalnum():
                host = host[:-1]

            new_name = f"{host}:[dark_gray]{tab.hastin.port}"
        elif manual_name:
            new_name = manual_name
        elif tab.manual_tab_name:
            new_name = tab.manual_tab_name

        if new_name:
            tab.name = new_name

            if tab.hastin.replay_file:
                new_name = f"[b recording][Replay][/b recording] {new_name}"

            self.host_tabs.query(TabWidget).filter("#" + tab.id)[0].label = Content.from_rich_text(
                new_name, console=self.app.console
            )

    def switch_tab(self, tab_id: str, set_active: bool = True):
        tab = self.get_tab(tab_id)
        if not tab:
            return

        # Update the active/current tab
        self.active_tab = tab

        # Prevent recursive calls
        if set_active:
            self.host_tabs.active = tab_id

        # Update the topbar
        self.update_topbar(tab=tab)

        if not tab.hastin.main_db_connection.is_connected():
            tab.main_container.display = False
        else:
            tab.main_container.display = True

    def get_tab(self, id: str) -> Tab:
        return self.tabs.get(id)

    def get_all_tabs(self) -> list[Tab]:
        all_tabs = []

        for tab in self.tabs.values():
            all_tabs.append(tab)

        return all_tabs

    async def disconnect_tab(self, tab: Tab, update_topbar: bool = True):
        if tab.worker:
            tab.worker.cancel()
        if tab.replicas_worker:
            tab.replicas_worker.cancel()

        if tab.worker_timer:
            tab.worker_timer.stop()
        if tab.replicas_worker_timer:
            tab.replicas_worker_timer.stop()

        tab.hastin.main_db_connection.close()
        tab.hastin.secondary_db_connection.close()

        tab.hastin.replica_manager.remove_all_replicas()

        if self.active_tab.id == tab.id:
            tab.main_container.display = False
            tab.loading_indicator.display = False

        tab.sparkline.data = [0]
        tab.remove_replication_panel_components()

        if update_topbar:
            self.update_connection_status(tab=tab, connection_status=ConnectionStatus.disconnected)

    def setup_host_tab(self, tab: Tab):
        hastin = tab.hastin

        async def command_get_input(data):
            # Set tab_setup to False since it's only used when Hastin first loads
            if self.config.tab_setup:
                self.config.tab_setup = False

            # Universally set record_for_replay
            self.config.record_for_replay = data.get("record_for_replay")

            hostgroup = data.get("hostgroup")
            if hostgroup:
                hastin.app.connect_as_hostgroup(hostgroup)
            else:
                # Create a new tab since it's easier to manage the worker threads this way
                new_tab = await self.create_tab(tab_name=tab.name)
                new_tab.manual_tab_name = tab.manual_tab_name
                new_hastin = new_tab.hastin

                host_port = data["host"].split(":")
                new_hastin.host = host_port[0]
                new_hastin.port = int(host_port[1]) if len(host_port) > 1 else 3306
                new_hastin.credential_profile = data.get("credential_profile")
                new_hastin.user = data.get("username")
                new_hastin.password = data.get("password")
                new_hastin.socket = data.get("socket_file")
                new_hastin.ssl = data.get("ssl")
                new_hastin.record_for_replay = data.get("record_for_replay")
                new_hastin.replay_file = data.get("replay_file")

                # Init the new variables above
                new_hastin.reset_runtime_variables()

                # Remove the old tab and disconnect it
                await self.remove_tab(tab)
                await self.disconnect_tab(tab, update_topbar=False)
                self.tabs.pop(tab.id, None)

                new_tab.loading_indicator.display = True

                new_tab.dashboard_replay_container.display = False
                if new_hastin.replay_file:
                    new_tab.replay_manager = ReplayManager(new_hastin)
                    if not new_tab.replay_manager.verify_replay_file():
                        new_tab.loading_indicator.display = False
                        self.setup_host_tab(new_tab)
                        return

                    self.update_connection_status(tab=new_tab, connection_status=ConnectionStatus.connected)
                    new_hastin.app.run_worker_replay(new_tab.id)
                else:
                    new_hastin.app.run_worker_main(new_tab.id)
                    new_hastin.app.run_worker_replicas(new_tab.id)

        # If we're here because of a worker cancel error or manually disconnected,
        # we want to pre-populate the host/port
        is_disconnected = hastin.connection_status == ConnectionStatus.disconnected
        if tab.worker_cancel_error or is_disconnected or self.config.tab_setup:
            host = hastin.host
            port = hastin.port
        else:
            host = ""
            port = ""

        hastin.app.push_screen(
            TabSetupModal(
                credential_profile=hastin.credential_profile,
                credential_profiles=hastin.config.credential_profiles,
                host=host,
                port=port,
                username=hastin.user,
                password=hastin.password,
                ssl=hastin.ssl,
                record_for_replay=hastin.record_for_replay,
                socket_file=hastin.socket,
                hostgroups=hastin.hostgroup_hosts.keys(),
                available_hosts=hastin.tab_setup_available_hosts,
                replay_directory=hastin.config.replay_dir,
                replay_files=hastin.get_replay_files(),
                error_message=tab.worker_cancel_error,
            ),
            command_get_input,
        )
