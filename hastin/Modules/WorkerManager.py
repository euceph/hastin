from datetime import datetime
from functools import partial
from typing import TYPE_CHECKING

from textual.worker import Worker, WorkerState, get_current_worker

import hastin.Modules.MetricManager as MetricManager
from hastin.DataTypes import ConnectionSource, ConnectionStatus
from hastin.Modules.ManualException import ManualException
from hastin.Modules.ReplayManager import ReplayManager
from hastin.Panels import Replication as ReplicationPanel

if TYPE_CHECKING:
    from hastin.App import HastinApp


class WorkerManager:
    """Handles all worker management operations for Hastin."""

    def __init__(self, app: "HastinApp"):
        self.app = app

    async def run_worker_replay(self, tab_id: str, manual_control: bool = False):
        tab = self.app.tab_manager.get_tab(tab_id)
        if not tab:
            return

        try:
            tab.worker = get_current_worker()
            tab.worker.name = tab_id

            hastin = tab.hastin

            tab.replay_manual_control = manual_control
            if (
                len(self.app.screen_stack) > 1
                or (hastin.pause_refresh and not manual_control)
                or tab.id != self.app.tab_manager.active_tab.id
            ):
                return

            replay_event_data = tab.replay_manager.get_next_refresh_interval()
            if not replay_event_data:
                tab.worker.cancel()
                return

            tab.replay_manager.fetch_global_variable_changes_for_current_replay_id()

            hastin.system_utilization = replay_event_data.system_utilization
            hastin.global_variables = replay_event_data.global_variables
            common_metrics = {
                "system_utilization": hastin.system_utilization,
                "global_variables": hastin.global_variables,
            }

            hastin.worker_processing_time = replay_event_data.get("replay_polling_latency", 0)

            # PostgreSQL replay data
            if hastin.connection_source in (ConnectionSource.postgresql, ConnectionSource.rds, ConnectionSource.aurora):
                hastin.host_version = hastin.parse_server_version(hastin.global_variables.get("server_version"))
                hastin.processlist_threads = replay_event_data.processlist
                hastin.replication_status = replay_event_data.replication_status
                hastin.connection_stats = getattr(replay_event_data, "connection_stats", {})
                hastin.database_stats = getattr(replay_event_data, "database_stats", {})

                connection_source_metrics = {
                    "replication_status": hastin.replication_status,
                    "database_stats": hastin.database_stats,
                }

            hastin.metric_manager.refresh_data(
                worker_start_time=datetime.now().astimezone(),
                **common_metrics,
                **connection_source_metrics,
            )

            hastin.metric_manager.datetimes = replay_event_data.metric_manager.get("datetimes")
            for metric_name, metric_data in replay_event_data.metric_manager.items():
                metric_instance = hastin.metric_manager.metrics.__dict__.get(metric_name)
                if metric_instance:
                    for metric_name, metric_values in metric_data.items():
                        metric: MetricManager.MetricData = metric_instance.__dict__.get(metric_name)
                        if metric:
                            metric.values = metric_values
                            metric.last_value = metric_values[-1]

        except Exception as e:
            self.app.notify(f"Error during replay: {str(e)}", title="Replay Error", severity="error")
            if tab.worker:
                tab.worker.cancel()

    async def run_worker_main(self, tab_id: str):
        tab = self.app.tab_manager.get_tab(tab_id)
        if not tab:
            return

        tab.worker = get_current_worker()
        tab.worker.name = tab_id

        hastin = tab.hastin
        try:
            if not hastin.main_db_connection.is_connected():
                self.app.call_from_thread(
                    self.app.tab_manager.update_connection_status,
                    tab=tab,
                    connection_status=ConnectionStatus.connecting,
                )

                tab.replay_manager = None
                if not hastin.daemon_mode and tab == self.app.tab_manager.active_tab:

                    def show_loading():
                        tab.loading_indicator.display = True

                    self.app.call_from_thread(show_loading)

                hastin.db_connect()

            worker_start_time = datetime.now().astimezone()
            hastin.polling_latency = (worker_start_time - hastin.worker_previous_start_time).total_seconds()
            hastin.worker_previous_start_time = worker_start_time

            hastin.collect_system_utilization()

            # Process data based on connection source
            if hastin.connection_source in (ConnectionSource.postgresql, ConnectionSource.rds, ConnectionSource.aurora):
                self.app.worker_data_processor.process_postgresql_data(tab)
            elif hastin.connection_source == ConnectionSource.pgbouncer:
                self.app.worker_data_processor.process_pgbouncer_data(tab)

            hastin.worker_processing_time = (datetime.now().astimezone() - worker_start_time).total_seconds()

            # Aggregate PgBouncer data for metrics
            pgbouncer_pools_aggregated = {}
            pgbouncer_stats_aggregated = {}
            if hastin.pgbouncer_pools:
                pgbouncer_pools_aggregated = {
                    "cl_active": sum(int(p.get("cl_active", 0)) for p in hastin.pgbouncer_pools),
                    "cl_waiting": sum(int(p.get("cl_waiting", 0)) for p in hastin.pgbouncer_pools),
                    "sv_active": sum(int(p.get("sv_active", 0)) for p in hastin.pgbouncer_pools),
                    "sv_idle": sum(int(p.get("sv_idle", 0)) for p in hastin.pgbouncer_pools),
                }
            if hastin.pgbouncer_stats:
                pgbouncer_stats_aggregated = {
                    "xact_count": sum(int(s.get("total_xact_count", 0)) for s in hastin.pgbouncer_stats),
                    "query_count": sum(int(s.get("total_query_count", 0)) for s in hastin.pgbouncer_stats),
                    "bytes_received": sum(int(s.get("total_received", 0)) for s in hastin.pgbouncer_stats),
                    "bytes_sent": sum(int(s.get("total_sent", 0)) for s in hastin.pgbouncer_stats),
                }

            hastin.metric_manager.refresh_data(
                worker_start_time=worker_start_time,
                polling_latency=hastin.polling_latency,
                system_utilization=hastin.system_utilization,
                global_variables=hastin.global_variables,
                database_stats=hastin.database_stats,
                bgwriter_stats=hastin.bgwriter_stats,
                connection_stats=hastin.connection_stats,
                replication_status=hastin.replication_status,
                pgbouncer_pools=pgbouncer_pools_aggregated,
                pgbouncer_stats=pgbouncer_stats_aggregated,
            )

            if not tab.replay_manager:
                tab.replay_manager = ReplayManager(hastin)

            tab.replay_manager.capture_state()
        except ManualException as exception:
            tab.worker_cancel_error = exception
            self.app.call_from_thread(self.app.tab_manager.disconnect_tab, tab)

    def run_worker_replicas(self, tab_id: str):
        tab = self.app.tab_manager.get_tab(tab_id)
        if not tab:
            return

        tab.replicas_worker = get_current_worker()
        tab.replicas_worker.name = tab_id

        hastin = tab.hastin

        if hastin.panels.replication.visible:
            if tab.id != self.app.tab_manager.active_tab.id:
                return

            # For PostgreSQL, replication is populated from pg_stat_replication
            if hastin.replication_role == "primary" and hastin.replication_status.get("replicas"):

                def update_replicas_ui():
                    tab.replicas_container.display = True

                self.app.call_from_thread(update_replicas_ui)
            else:

                def hide_replicas():
                    tab.replicas_container.display = False

                self.app.call_from_thread(hide_replicas)
        else:
            hastin.replica_manager.remove_all_replicas()

    def on_worker_state_changed(self, event: Worker.StateChanged):
        if event.state not in [WorkerState.SUCCESS, WorkerState.CANCELLED]:
            return

        tab = self.app.tab_manager.get_tab(event.worker.name)
        if not tab:
            return

        hastin = tab.hastin

        if event.worker.group == "main":
            if event.state == WorkerState.SUCCESS:
                self.app.worker_data_processor.monitor_read_only_change(tab)

                refresh_interval = hastin.refresh_interval

                if (
                    len(self.app.screen_stack) > 1
                    or hastin.pause_refresh
                    or not hastin.main_db_connection.is_connected()
                    or hastin.daemon_mode
                    or tab.id != self.app.tab_manager.active_tab.id
                ):
                    tab.worker_timer = self.app.set_timer(refresh_interval, partial(self.app.run_worker_main, tab.id))
                    return

                if not tab.main_container.display:
                    tab.toggle_metric_graph_tabs_display()
                    tab.layout_graphs()

                # Refresh screen based on connection source
                pg_sources = (ConnectionSource.postgresql, ConnectionSource.rds, ConnectionSource.aurora)
                if hastin.connection_source in pg_sources:
                    self.app.worker_data_processor.refresh_screen_postgresql(tab)
                elif hastin.connection_source == ConnectionSource.pgbouncer:
                    self.app.worker_data_processor.refresh_screen_pgbouncer(tab)

                if hastin.record_for_replay:
                    self.app.tab_manager.update_topbar(tab=tab)

                tab.toggle_entities_displays()

                tab.worker_timer = self.app.set_timer(refresh_interval, partial(self.app.run_worker_main, tab.id))
            elif event.state == WorkerState.CANCELLED:
                if tab.worker_cancel_error:
                    from loguru import logger

                    logger.critical(tab.worker_cancel_error)

                    if self.app.tab_manager.active_tab.id != tab.id or self.app.tab_manager.loading_hostgroups:
                        msg = (
                            f"[$b_light_blue]{hastin.host}:{hastin.port}[/$b_light_blue]: "
                            f"{tab.worker_cancel_error.reason}"
                        )
                        self.app.notify(msg, title="Connection Error", severity="error", timeout=10)

                    if not self.app.tab_manager.loading_hostgroups:
                        self.app.tab_manager.switch_tab(tab.id)
                        self.app.tab_manager.setup_host_tab(tab)
                        self.app.bell()
        elif event.worker.group == "replicas":
            if event.state == WorkerState.SUCCESS:
                if (
                    len(self.app.screen_stack) > 1
                    or hastin.pause_refresh
                    or tab.id != self.app.tab_manager.active_tab.id
                ):
                    tab.replicas_worker_timer = self.app.set_timer(
                        hastin.refresh_interval, partial(self.app.run_worker_replicas, tab.id)
                    )
                    return

                if hastin.panels.replication.visible and hastin.replication_role == "primary":
                    ReplicationPanel.create_panel(tab)
                    # Hide loading indicator after panel is created
                    tab.replicas_loading_indicator.display = False

                tab.replicas_worker_timer = self.app.set_timer(
                    hastin.refresh_interval, partial(self.app.run_worker_replicas, tab.id)
                )
        elif event.worker.group == "replay" and event.state == WorkerState.SUCCESS:
            if tab.id == self.app.tab_manager.active_tab.id:
                if len(self.app.screen_stack) > 1 or (hastin.pause_refresh and not tab.replay_manual_control):
                    tab.worker_timer = self.app.set_timer(
                        hastin.refresh_interval, partial(self.app.run_worker_replay, tab.id)
                    )
                    return
            else:
                return

            self.app.worker_data_processor.monitor_read_only_change(tab)

            if not tab.main_container.display:
                tab.toggle_metric_graph_tabs_display()
                tab.layout_graphs()

            if hastin.connection_source in (ConnectionSource.postgresql, ConnectionSource.rds, ConnectionSource.aurora):
                self.app.worker_data_processor.refresh_screen_postgresql(tab)

            tab.toggle_entities_displays()

            tab.worker_timer = self.app.set_timer(hastin.refresh_interval, partial(self.app.run_worker_replay, tab.id))
