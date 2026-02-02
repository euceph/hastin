"""Worker data processor for Hastin PostgreSQL monitoring dashboard."""

from datetime import timedelta
from typing import TYPE_CHECKING

import psycopg
from loguru import logger

from hastin.DataTypes import ConnectionStatus
from hastin.Modules.Queries import PgBouncerQueries, PostgresQueries
from hastin.Panels import Processlist as ProcesslistPanel

if TYPE_CHECKING:
    from hastin.App import HastinApp
    from hastin.Modules.TabManager import Tab


class WorkerDataProcessor:
    """Manages polling data processing and screen refresh operations for worker threads."""

    def __init__(self, app: "HastinApp"):
        self.app = app

    def process_postgresql_data(self, tab: "Tab"):
        """Process PostgreSQL data for a given tab."""
        hastin = tab.hastin

        # Fetch global variables (pg_settings)
        global_variables = hastin.main_db_connection.fetch_status_and_variables("variables")
        self.monitor_global_variable_change(tab=tab, old_data=hastin.global_variables, new_data=global_variables)
        hastin.global_variables = global_variables

        # At this point, we're connected so we need to do a few things
        if hastin.connection_status == ConnectionStatus.connecting:
            self.app.call_from_thread(
                self.app.tab_manager.update_connection_status, tab=tab, connection_status=ConnectionStatus.connected
            )
            # Get server info
            hastin.main_db_connection.execute(PostgresQueries.server_info)
            server_info = hastin.main_db_connection.fetchone()
            hastin.server_info = server_info
            hastin.host_version = hastin.parse_server_version(server_info.get("server_version"))
            hastin.is_replica = server_info.get("is_replica", False)

            # Configure PostgreSQL-specific settings
            hastin.configure_postgresql()
            hastin.detect_replication_role()

        # Fetch server info (for uptime monitoring)
        hastin.main_db_connection.execute(PostgresQueries.server_info)
        server_info = hastin.main_db_connection.fetchone()
        old_uptime = hastin.server_info.get("uptime_seconds", 0)
        new_uptime = server_info.get("uptime_seconds", 0)
        self.monitor_uptime_change(tab=tab, old_uptime=old_uptime, new_uptime=new_uptime)
        hastin.server_info = server_info

        # Fetch connection stats
        hastin.main_db_connection.execute(PostgresQueries.connection_stats)
        hastin.connection_stats = hastin.main_db_connection.fetchone()

        # Fetch database stats
        hastin.main_db_connection.execute(PostgresQueries.database_stats)
        hastin.database_stats = hastin.main_db_connection.fetchone()

        # Fetch bgwriter stats
        hastin.main_db_connection.execute(PostgresQueries.bgwriter_stats)
        hastin.bgwriter_stats = hastin.main_db_connection.fetchone()

        # Fetch replication status based on role
        if hastin.replication_role == "primary":
            hastin.main_db_connection.execute(PostgresQueries.replication_status_primary)
            hastin.replication_status = {"replicas": hastin.main_db_connection.fetchall()}

            hastin.main_db_connection.execute(PostgresQueries.replication_slots)
            hastin.replication_slots = hastin.main_db_connection.fetchall()

        elif hastin.replication_role == "replica":
            hastin.main_db_connection.execute(PostgresQueries.replication_status_replica)
            hastin.replication_status = hastin.main_db_connection.fetchone()

        # Fetch logical subscriptions
        hastin.main_db_connection.execute(PostgresQueries.logical_subscriptions)
        hastin.logical_subscriptions = hastin.main_db_connection.fetchall()

        # Fetch processlist if panel is visible
        if hastin.panels.processlist.visible:
            hastin.processlist_threads = ProcesslistPanel.fetch_data(tab)

        # Fetch locks if panel is visible
        if hastin.panels.locks.visible:
            hastin.main_db_connection.execute(PostgresQueries.locks)
            hastin.locks_data = hastin.main_db_connection.fetchall()

        # Fetch pg_stat_statements data if available
        if hastin.has_pg_stat_statements:
            # Always fetch statement type counts for dashboard display
            hastin.main_db_connection.execute(PostgresQueries.statement_type_counts)
            hastin.statement_type_counts = hastin.main_db_connection.fetchone() or {}

            # Fetch detailed statement stats only if panel is visible
            if hastin.panels.statements.visible:
                hastin.main_db_connection.execute(PostgresQueries.statement_stats)
                hastin.statements_data = hastin.main_db_connection.fetchall()

        # Fetch PgBouncer stats if available (combined mode)
        if hastin.has_pgbouncer and hastin.pgbouncer_connection and hastin.pgbouncer_connection.is_connected():
            try:
                hastin.pgbouncer_connection.execute(PgBouncerQueries.show_stats)
                hastin.pgbouncer_stats = hastin.pgbouncer_connection.fetchall()

                hastin.pgbouncer_connection.execute(PgBouncerQueries.show_pools)
                hastin.pgbouncer_pools = hastin.pgbouncer_connection.fetchall()

                # Get version on first connection
                if not hastin.pgbouncer_version:
                    hastin.pgbouncer_connection.execute(PgBouncerQueries.show_version)
                    version_result = hastin.pgbouncer_connection.fetchone()
                    if version_result:
                        hastin.pgbouncer_version = version_result.get("version", "Unknown")
            except psycopg.Error as e:
                logger.warning(f"Failed to fetch PgBouncer stats: {e}")
                hastin.has_pgbouncer = False

        # Update read-only status
        self.monitor_read_only_change(tab)

    def refresh_screen_postgresql(self, tab: "Tab"):
        """Refresh the PostgreSQL screen for a given tab."""
        hastin = tab.hastin

        if tab.loading_indicator.display:
            tab.loading_indicator.display = False

        # Hide standalone PgBouncer panels in PostgreSQL mode
        tab.panel_pgbouncer_dashboard.display = False
        tab.panel_pgbouncer_pools.display = False
        tab.panel_pgbouncer_clients.display = False
        tab.panel_pgbouncer_servers.display = False

        # Loop each panel and refresh it
        for panel in hastin.panels.get_all_panels():
            if panel.visible:
                if panel.name == hastin.panels.graphs.name:
                    continue

                # Skip pgbouncer panel if PgBouncer is not connected
                if panel.name == hastin.panels.pgbouncer.name and not hastin.has_pgbouncer:
                    tab.panel_pgbouncer.display = False
                    continue

                self.app.refresh_panel(tab, panel.name)

                if panel.name == hastin.panels.dashboard.name:
                    # Use PostgreSQL transaction commits for the sparkline
                    txn_values = hastin.metric_manager.metrics.pg_transactions.xact_commit.values
                    if txn_values:
                        tab.sparkline.data = list(txn_values)
                        tab.sparkline.refresh()

        self.app.update_graphs(tab.metric_graph_tabs.get_pane(tab.metric_graph_tabs.active).name)
        tab.refresh_replay_dashboard_section()

        if not hastin.daemon_mode:
            hastin.processlist_threads_snapshot = hastin.processlist_threads.copy()

    def monitor_global_variable_change(self, tab: "Tab", old_data: dict, new_data: dict):
        """Monitor and notify about global variable changes."""
        if not old_data:
            return

        hastin = tab.hastin
        exclude_variables = {"stats_reset", "xact_commit", "xact_rollback"}

        if hastin.exclude_notify_global_vars:
            exclude_variables.update(hastin.exclude_notify_global_vars)

        for variable, new_value in new_data.items():
            if any(item in variable.lower() for item in exclude_variables):
                continue

            old_value = old_data.get(variable)
            if old_value != new_value:
                logger.info(f"Setting {variable} changed: {old_value} -> {new_value}")

                include_host = ""
                if self.app.tab_manager.active_tab.id != tab.id:
                    include_host = f"Host:      [$light_blue]{hastin.host_with_port}[/$light_blue]\n"
                self.app.app.notify(
                    f"[b][$dark_yellow]{variable}[/b][/$dark_yellow]\n"
                    f"{include_host}"
                    f"Old Value: [$highlight]{old_value}[/$highlight]\n"
                    f"New Value: [$highlight]{new_value}[/$highlight]",
                    title="PostgreSQL Setting Change",
                    severity="warning",
                    timeout=15,
                )

    def monitor_uptime_change(self, tab: "Tab", old_uptime: int, new_uptime: int):
        """Monitor and handle uptime changes (e.g., server restarts)."""
        if old_uptime > new_uptime:
            formatted_old_uptime = str(timedelta(seconds=old_uptime))
            formatted_new_uptime = str(timedelta(seconds=new_uptime))

            logger.info(f"Uptime changed: {formatted_old_uptime} -> {formatted_new_uptime}")

            self.app.app.notify(
                f"PostgreSQL server appears to have restarted.\n"
                f"Old uptime: {formatted_old_uptime}\n"
                f"New uptime: {formatted_new_uptime}",
                title="Server Restart Detected",
                severity="warning",
                timeout=15,
            )

    def monitor_read_only_change(self, tab: "Tab"):
        """Monitor and notify about read-only status changes."""
        hastin = tab.hastin
        is_replica = hastin.is_replica

        formatted_status = ConnectionStatus.replica if is_replica else ConnectionStatus.primary

        if (
            hastin.connection_status in [ConnectionStatus.primary, ConnectionStatus.replica]
            and hastin.connection_status != formatted_status
        ):
            status = "replica (read-only)" if is_replica else "primary (read/write)"
            message = (
                f"Host [$light_blue]{hastin.host_with_port}[/$light_blue] is now [$b_highlight]{status}[/$b_highlight]"
            )

            logger.warning(f"Role changed: {hastin.connection_status} -> {formatted_status}")
            self.app.app.notify(
                title="Role Change Detected",
                message=message,
                severity="warning",
                timeout=15,
            )

            self.app.tab_manager.update_connection_status(tab=tab, connection_status=formatted_status)
        elif hastin.connection_status == ConnectionStatus.connected:
            self.app.tab_manager.update_connection_status(tab=tab, connection_status=formatted_status)

        hastin.connection_status = formatted_status

    def process_pgbouncer_data(self, tab: "Tab"):
        """Process PgBouncer data for a given tab (standalone mode)."""
        hastin = tab.hastin

        # At this point, we're connected so we need to do a few things
        if hastin.connection_status == ConnectionStatus.connecting:
            self.app.call_from_thread(
                self.app.tab_manager.update_connection_status, tab=tab, connection_status=ConnectionStatus.connected
            )
            # Get PgBouncer version
            hastin.main_db_connection.execute(PgBouncerQueries.show_version)
            version_result = hastin.main_db_connection.fetchone()
            if version_result:
                hastin.pgbouncer_version = version_result.get("version", "Unknown")
            hastin.host_version = hastin.pgbouncer_version

        # Fetch SHOW STATS
        hastin.main_db_connection.execute(PgBouncerQueries.show_stats)
        hastin.pgbouncer_stats = hastin.main_db_connection.fetchall()

        # Fetch SHOW POOLS
        hastin.main_db_connection.execute(PgBouncerQueries.show_pools)
        hastin.pgbouncer_pools = hastin.main_db_connection.fetchall()

        # Fetch SHOW CLIENTS if panel is visible
        if hastin.panels.pgbouncer_clients.visible:
            hastin.main_db_connection.execute(PgBouncerQueries.show_clients)
            hastin.pgbouncer_clients = hastin.main_db_connection.fetchall()

        # Fetch SHOW SERVERS if panel is visible
        if hastin.panels.pgbouncer_servers.visible:
            hastin.main_db_connection.execute(PgBouncerQueries.show_servers)
            hastin.pgbouncer_servers = hastin.main_db_connection.fetchall()

    def refresh_screen_pgbouncer(self, tab: "Tab"):
        """Refresh the PgBouncer screen for a given tab (standalone mode)."""
        from hastin.Panels import PgBouncerClients as PgBouncerClientsPanel
        from hastin.Panels import PgBouncerDashboard as PgBouncerDashboardPanel
        from hastin.Panels import PgBouncerPools as PgBouncerPoolsPanel
        from hastin.Panels import PgBouncerServers as PgBouncerServersPanel

        hastin = tab.hastin

        if tab.loading_indicator.display:
            tab.loading_indicator.display = False

        # In PgBouncer mode, we reuse the PostgreSQL dashboard container
        # but hide PostgreSQL-specific panels
        tab.panel_processlist.display = False
        tab.panel_replication.display = False
        tab.panel_locks.display = False
        tab.panel_statements.display = False
        tab.panel_pgbouncer.display = False  # Hide combined mode panel in standalone

        # Show the dashboard container (used for PgBouncer dashboard)
        tab.panel_dashboard.display = hastin.panels.pgbouncer_dashboard.visible

        # Loop each visible PgBouncer panel and refresh it
        if hastin.panels.pgbouncer_dashboard.visible:
            PgBouncerDashboardPanel.create_panel(tab)

        # Show pools panel
        if hastin.panels.pgbouncer_pools.visible:
            tab.panel_pgbouncer_pools.display = True
            PgBouncerPoolsPanel.create_panel(tab)
        else:
            tab.panel_pgbouncer_pools.display = False

        # Show clients panel
        if hastin.panels.pgbouncer_clients.visible:
            tab.panel_pgbouncer_clients.display = True
            PgBouncerClientsPanel.create_panel(tab)
        else:
            tab.panel_pgbouncer_clients.display = False

        # Show servers panel
        if hastin.panels.pgbouncer_servers.visible:
            tab.panel_pgbouncer_servers.display = True
            PgBouncerServersPanel.create_panel(tab)
        else:
            tab.panel_pgbouncer_servers.display = False

        # Show graphs panel
        tab.panel_graphs.display = hastin.panels.graphs.visible

        # Toggle metric graph tabs for PgBouncer (only System graphs apply)
        tab.toggle_entities_displays()

        # Update graph visualizations
        if hastin.panels.graphs.visible:
            self.app.update_graphs(tab.metric_graph_tabs.get_pane(tab.metric_graph_tabs.active).name)
