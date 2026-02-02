# Use __future__ to allow 'deque[int]' type hint in MetricData
from __future__ import annotations

import contextlib
import dataclasses
from collections import defaultdict, deque
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import TypeAlias

import plotext as plt
from rich.text import Text
from textual.widgets import Static

from hastin.DataTypes import ConnectionSource
from hastin.Modules.Functions import format_bytes, format_number, format_time


def _all_pg_sources() -> list:
    """Return all PostgreSQL-compatible connection sources for metric graphs."""
    return [
        ConnectionSource.postgresql,
        ConnectionSource.rds,
        ConnectionSource.aurora,
        ConnectionSource.cloud_sql,
        ConnectionSource.alloydb,
        ConnectionSource.azure,
        ConnectionSource.cosmos_citus,
        ConnectionSource.supabase,
        ConnectionSource.neon,
        ConnectionSource.aiven,
        ConnectionSource.crunchy_bridge,
        ConnectionSource.digitalocean,
        ConnectionSource.heroku,
        ConnectionSource.timescale,
        ConnectionSource.railway,
        ConnectionSource.render,
        ConnectionSource.fly,
    ]


def _all_sources_with_system() -> list:
    """Return all sources that support system metrics (local connections only)."""
    return [ConnectionSource.postgresql, ConnectionSource.pgbouncer]


class MetricSource(Enum):
    """Enumeration of sources for metric data."""

    SYSTEM_UTILIZATION = "system_utilization"
    DISK_IO_METRICS = "disk_io_metrics"
    # PostgreSQL-specific sources
    DATABASE_STATS = "database_stats"
    BGWRITER_STATS = "bgwriter_stats"
    CONNECTION_STATS = "connection_stats"
    # PgBouncer-specific sources
    PGBOUNCER_POOLS = "pgbouncer_pools"
    PGBOUNCER_STATS = "pgbouncer_stats"
    NONE = "none"


@dataclass
class MetricColor:
    """Namespace for standard metric graph colors."""

    # Dark green theme colors - distinct shades
    gray: tuple = (100, 140, 120)  # Muted sage
    blue: tuple = (64, 224, 208)  # Turquoise/cyan (distinct from green)
    green: tuple = (50, 205, 50)  # Lime green (bright)
    red: tuple = (255, 99, 71)  # Tomato red (for contrast)
    yellow: tuple = (218, 165, 32)  # Goldenrod (warm contrast)
    purple: tuple = (147, 112, 219)  # Medium purple (cool contrast)
    orange: tuple = (255, 165, 0)  # Orange (warm accent)


class Graph(Static):
    """A Textual widget for rendering time-series graphs using plotext."""

    def __init__(self, *args, **kwargs) -> None:
        """Initialize the Graph widget."""
        super().__init__(*args, **kwargs)
        self.marker: str | None = None
        self.metric_instance: MetricInstance | None = None
        # This will be a deque, but we treat it as an iterable
        self.datetimes: deque[str] | None = None

    def on_show(self) -> None:
        """Render the graph when the widget is shown."""
        self.render_graph(self.metric_instance, self.datetimes)

    def on_resize(self) -> None:
        """Re-render the graph when the widget is resized."""
        self.render_graph(self.metric_instance, self.datetimes)

    def _setup_plot(self) -> None:
        """Clears and configures the plotext canvas."""
        plt.clf()
        plt.date_form("d/m/y H:M:S")
        plt.canvas_color((4, 13, 8))  # Dark green background
        plt.axes_color((4, 13, 8))  # Dark green axes
        plt.ticks_color((120, 176, 144))  # Green ticks
        plt.plotsize(self.size.width, self.size.height)

    def _finalize_plot(self, max_y_value: float) -> None:
        """Calculates Y-ticks, formats labels, and updates the widget."""
        max_y_ticks = 5
        y_tick_interval = (max_y_value / max_y_ticks) if max_y_ticks > 0 else 0

        if y_tick_interval >= 1:
            y_ticks = [i * y_tick_interval for i in range(int(max_y_ticks) + 1)]
        else:
            y_ticks = [float(i) for i in range(int(max_y_value) + 2)]

        format_function = get_number_format_function(self.metric_instance)
        y_labels = [format_function(val) for val in y_ticks]

        plt.yticks(y_ticks, y_labels)

        with contextlib.suppress(OSError):
            self.update(Text.from_ansi(plt.build()))

    def _render_system_memory_metrics(self, x: list[str], y: list[float]) -> float:
        """Renders the graph for SystemMemoryMetrics."""
        total_mem = self.metric_instance.Memory_Total.last_value or 0
        plt.hline(0, (10, 14, 27))
        plt.hline(total_mem, (252, 121, 121))
        plt.text(
            "Total",
            y=total_mem,
            x=max(x),
            alignment="right",
            color=(233, 233, 233),
            style="bold",
        )

        metric = self.metric_instance.Memory_Used
        plt.plot(x, y, marker=self.marker, label=metric.label, color=metric.color)
        return total_mem

    def _render_default_metrics(self, x: list[str]) -> float:
        """Renders a graph for any standard metric instance."""
        max_y = 0.0
        for metric_data in self.metric_instance.__dict__.values():
            if isinstance(metric_data, MetricData) and metric_data.visible:
                # **THREAD-SAFETY**: Snapshot deque to list
                y = list(metric_data.values)
                if y and x:
                    plt.plot(
                        x,
                        y,
                        marker=self.marker,
                        label=metric_data.label,
                        color=metric_data.color,
                    )
                    try:
                        max_y = max(max_y, max(y))
                    except ValueError:
                        pass
        return max_y

    def render_graph(self, metric_instance: MetricInstance | None, datetimes: deque[str] | None) -> None:
        """Renders a graph for the given metric instance and datetimes.

        Args:
            metric_instance: The metric dataclass instance to plot.
            datetimes: A deque of datetime strings for the X-axis.
        """
        self.metric_instance = metric_instance
        self.datetimes = datetimes

        if self.metric_instance is None or self.datetimes is None:
            self.update("")  # Clear the graph if no data
            return

        self._setup_plot()

        max_y_value = 0.0

        # Create a snapshot of the datetimes and all
        # relevant metric values at the beginning of the render for thread-safety
        try:
            x = list(self.datetimes)
            if not x:
                self.update("")
                return
        except RuntimeError:  # deque changed size during iteration
            self.update("")
            return

        try:
            if isinstance(self.metric_instance, SystemMemoryMetrics):
                y = list(self.metric_instance.Memory_Used.values)
                if x and y:
                    max_y_value = self._render_system_memory_metrics(x, y)
            else:
                # Default renderer snapshots its own 'y' values inside
                max_y_value = self._render_default_metrics(x)

        except (ValueError, TypeError, IndexError):
            pass  # Catch errors during plotting

        self._finalize_plot(max_y_value)


def get_number_format_function(data: MetricInstance, color: bool = False) -> Callable[[int | float], str]:
    """Returns the correct formatting function based on the metric type."""
    data_formatters: dict[type, Callable[[int | float], str]] = {
        ReplicationLagMetrics: lambda val: format_time(val),
        SystemMemoryMetrics: lambda val: format_bytes(val, color=color),
        SystemNetworkMetrics: lambda val: format_bytes(val, color=color),
    }
    return data_formatters.get(type(data), lambda val: format_number(val, color=color))


@dataclass
class MetricData:
    label: str
    color: tuple
    visible: bool = True
    save_history: bool = True
    per_second_calculation: bool = True
    last_value: int | None = None
    graphable: bool = True
    create_switch: bool = True
    # Use a deque for O(1) appends and pops
    values: deque[int] = field(default_factory=deque)


@dataclass
class SystemCPUMetrics:
    CPU_Percent: MetricData
    graphs: list[str]
    tab_name: str = "system"
    graph_tab_name = "System"
    metric_source: MetricSource = MetricSource.SYSTEM_UTILIZATION
    connection_source: list[ConnectionSource] = field(
        default_factory=lambda: [ConnectionSource.postgresql, ConnectionSource.rds, ConnectionSource.pgbouncer]
    )
    use_with_replay: bool = True


@dataclass
class SystemMemoryMetrics:
    Memory_Total: MetricData
    Memory_Used: MetricData
    graphs: list[str]
    tab_name: str = "system"
    graph_tab_name = "System"
    metric_source: MetricSource = MetricSource.SYSTEM_UTILIZATION
    connection_source: list[ConnectionSource] = field(
        default_factory=lambda: [ConnectionSource.postgresql, ConnectionSource.rds, ConnectionSource.pgbouncer]
    )
    use_with_replay: bool = True


@dataclass
class SystemNetworkMetrics:
    Network_Down: MetricData
    Network_Up: MetricData
    graphs: list[str]
    tab_name: str = "system"
    graph_tab_name = "System"
    metric_source: MetricSource = MetricSource.SYSTEM_UTILIZATION
    connection_source: list[ConnectionSource] = field(
        default_factory=lambda: [ConnectionSource.postgresql, ConnectionSource.rds, ConnectionSource.pgbouncer]
    )
    use_with_replay: bool = True


@dataclass
class SystemDiskIOMetrics:
    Disk_Read: MetricData
    Disk_Write: MetricData
    graphs: list[str]
    tab_name: str = "system"
    graph_tab_name = "System"
    metric_source: MetricSource = MetricSource.SYSTEM_UTILIZATION
    connection_source: list[ConnectionSource] = field(
        default_factory=lambda: [ConnectionSource.postgresql, ConnectionSource.rds, ConnectionSource.pgbouncer]
    )
    use_with_replay: bool = True


@dataclass
class ReplicationLagMetrics:
    lag: MetricData
    graphs: list[str]
    tab_name: str = "replication_lag"
    graph_tab_name = "Replication"
    metric_source: MetricSource = MetricSource.NONE
    connection_source: list[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.postgresql])
    use_with_replay: bool = True


# =============================================================================
# PostgreSQL-Specific Metrics
# =============================================================================


@dataclass
class PgTransactionMetrics:
    """PostgreSQL transaction metrics from pg_stat_database."""

    xact_commit: MetricData
    xact_rollback: MetricData
    graphs: list[str]
    tab_name: str = "pg_transactions"
    graph_tab_name = "Transactions"
    metric_source: MetricSource = MetricSource.DATABASE_STATS
    connection_source: list[ConnectionSource] = field(
        default_factory=_all_pg_sources
    )
    use_with_replay: bool = True


@dataclass
class PgTupleMetrics:
    """PostgreSQL tuple operation metrics from pg_stat_database."""

    tup_fetched: MetricData
    tup_inserted: MetricData
    tup_updated: MetricData
    tup_deleted: MetricData
    graphs: list[str]
    tab_name: str = "pg_tuples"
    graph_tab_name = "Tuples"
    metric_source: MetricSource = MetricSource.DATABASE_STATS
    connection_source: list[ConnectionSource] = field(
        default_factory=_all_pg_sources
    )
    use_with_replay: bool = True


@dataclass
class PgBlockIOMetrics:
    """PostgreSQL block I/O metrics from pg_stat_database."""

    blks_hit: MetricData
    blks_read: MetricData
    graphs: list[str]
    tab_name: str = "pg_block_io"
    graph_tab_name = "Block I/O"
    metric_source: MetricSource = MetricSource.DATABASE_STATS
    connection_source: list[ConnectionSource] = field(
        default_factory=_all_pg_sources
    )
    use_with_replay: bool = True


@dataclass
class PgCacheHitRatioMetrics:
    """PostgreSQL buffer cache hit ratio."""

    cache_hit_ratio: MetricData
    graphs: list[str]
    tab_name: str = "pg_cache"
    graph_tab_name = "Cache Hit %"
    metric_source: MetricSource = MetricSource.NONE  # Calculated metric
    connection_source: list[ConnectionSource] = field(
        default_factory=_all_pg_sources
    )
    use_with_replay: bool = True


@dataclass
class PgConnectionMetrics:
    """PostgreSQL connection metrics from pg_stat_activity."""

    active: MetricData
    idle: MetricData
    idle_in_transaction: MetricData
    graphs: list[str]
    tab_name: str = "pg_connections"
    graph_tab_name = "Connections"
    metric_source: MetricSource = MetricSource.CONNECTION_STATS
    connection_source: list[ConnectionSource] = field(
        default_factory=_all_pg_sources
    )
    use_with_replay: bool = True


@dataclass
class PgCheckpointMetrics:
    """PostgreSQL checkpoint metrics from pg_stat_bgwriter."""

    checkpoints_timed: MetricData
    checkpoints_req: MetricData
    buffers_checkpoint: MetricData
    graphs: list[str]
    tab_name: str = "pg_checkpoints"
    graph_tab_name = "Checkpoints"
    metric_source: MetricSource = MetricSource.BGWRITER_STATS
    connection_source: list[ConnectionSource] = field(
        default_factory=_all_pg_sources
    )
    use_with_replay: bool = True


@dataclass
class PgTempFileMetrics:
    """PostgreSQL temp file metrics from pg_stat_database."""

    temp_files: MetricData
    temp_bytes: MetricData
    graphs: list[str]
    tab_name: str = "pg_temp_files"
    graph_tab_name = "Temp Files"
    metric_source: MetricSource = MetricSource.DATABASE_STATS
    connection_source: list[ConnectionSource] = field(
        default_factory=_all_pg_sources
    )
    use_with_replay: bool = True


@dataclass
class PgBouncerConnectionMetrics:
    """PgBouncer connection metrics from SHOW POOLS."""

    cl_active: MetricData
    cl_waiting: MetricData
    sv_active: MetricData
    sv_idle: MetricData
    graphs: list[str]
    tab_name: str = "pgbouncer_connections"
    graph_tab_name = "Connections"
    metric_source: MetricSource = MetricSource.PGBOUNCER_POOLS
    connection_source: list[ConnectionSource] = field(
        default_factory=lambda: [ConnectionSource.pgbouncer]
    )
    use_with_replay: bool = True


@dataclass
class PgBouncerTrafficMetrics:
    """PgBouncer traffic metrics from SHOW STATS."""

    xact_count: MetricData
    query_count: MetricData
    bytes_received: MetricData
    bytes_sent: MetricData
    graphs: list[str]
    tab_name: str = "pgbouncer_traffic"
    graph_tab_name = "Traffic"
    metric_source: MetricSource = MetricSource.PGBOUNCER_STATS
    connection_source: list[ConnectionSource] = field(
        default_factory=lambda: [ConnectionSource.pgbouncer]
    )
    use_with_replay: bool = True


# Type alias for all metric types
MetricInstance: TypeAlias = (
    SystemCPUMetrics
    | SystemMemoryMetrics
    | SystemNetworkMetrics
    | SystemDiskIOMetrics
    | ReplicationLagMetrics
    | PgTransactionMetrics
    | PgTupleMetrics
    | PgBlockIOMetrics
    | PgCacheHitRatioMetrics
    | PgConnectionMetrics
    | PgCheckpointMetrics
    | PgTempFileMetrics
    | PgBouncerConnectionMetrics
    | PgBouncerTrafficMetrics
)


@dataclass
class MetricInstances:
    """Container for all specific metric instances."""

    # System metrics (work with all connection sources)
    system_cpu: SystemCPUMetrics
    system_memory: SystemMemoryMetrics
    system_disk_io: SystemDiskIOMetrics
    system_network: SystemNetworkMetrics

    # PostgreSQL-specific metrics
    pg_transactions: PgTransactionMetrics
    pg_tuples: PgTupleMetrics
    pg_block_io: PgBlockIOMetrics
    pg_cache_hit_ratio: PgCacheHitRatioMetrics
    pg_connections: PgConnectionMetrics
    pg_checkpoints: PgCheckpointMetrics
    pg_temp_files: PgTempFileMetrics

    # Replication lag
    replication_lag: ReplicationLagMetrics

    # PgBouncer-specific metrics
    pgbouncer_connections: PgBouncerConnectionMetrics
    pgbouncer_traffic: PgBouncerTrafficMetrics


class MetricManager:
    """Manages the state, collection, and processing of all metrics."""

    def __init__(self, replay_file: str, daemon_mode: bool = False):
        """Initialize the MetricManager.

        Args:
            replay_file: Path to a replay file, if one is being used.
            daemon_mode: True if running in daemon mode (trims old data).
        """
        self.connection_source = ConnectionSource.postgresql
        self.replay_file = replay_file
        self.daemon_mode = daemon_mode

        # Attributes populated by refresh_data
        self.worker_start_time: datetime | None = None
        self.system_utilization: dict[str, int] = {}
        self.disk_io_metrics: dict[str, int] = {}
        self.replication_status: dict[str, int | str] = {}
        # PostgreSQL-specific data stores
        self.database_stats: dict[str, int] = {}
        self.bgwriter_stats: dict[str, int] = {}
        self.connection_stats: dict[str, int] = {}
        # PgBouncer-specific data stores
        self.pgbouncer_pools: dict[str, int] = {}
        self.pgbouncer_stats: dict[str, int] = {}

        # State attributes
        self.initialized: bool = False
        self.polling_latency: float = 0
        # Use a deque for O(1) appends and pops
        self.datetimes: deque[str] = deque()

        # The authoritative structure of all metrics
        self.metrics: MetricInstances = None  # type: ignore

        # Optimized lookup tables for processing
        # For fast, source-based processing in update_..._values
        self._source_to_metrics_processing: dict[MetricSource, list[tuple[str, MetricData, list[ConnectionSource]]]] = (
            defaultdict(list)
        )
        # For fast history cleanup in daemon_cleanup_data
        self._all_metrics_data_history: list[MetricData] = []

        # Setup the dispatch map for metric sources
        self._metric_source_map: dict[MetricSource, dict[str, int] | None] = {
            MetricSource.SYSTEM_UTILIZATION: self.system_utilization,
            MetricSource.DISK_IO_METRICS: self.disk_io_metrics,
            # PostgreSQL-specific sources
            MetricSource.DATABASE_STATS: self.database_stats,
            MetricSource.BGWRITER_STATS: self.bgwriter_stats,
            MetricSource.CONNECTION_STATS: self.connection_stats,
            # PgBouncer-specific sources
            MetricSource.PGBOUNCER_POOLS: self.pgbouncer_pools,
            MetricSource.PGBOUNCER_STATS: self.pgbouncer_stats,
            MetricSource.NONE: None,
        }

        self.reset()

    def reset(self):
        """Resets all metrics and state to their default values."""
        self.initialized = False
        self.polling_latency = 0
        self.datetimes.clear()

        # Clear raw data stores
        self.system_utilization.clear()
        self.disk_io_metrics.clear()
        self.replication_status.clear()
        # Clear PostgreSQL data stores
        self.database_stats.clear()
        self.bgwriter_stats.clear()
        self.connection_stats.clear()
        # Clear PgBouncer data stores
        self.pgbouncer_pools.clear()
        self.pgbouncer_stats.clear()

        # Clear performance lookup tables
        self._source_to_metrics_processing.clear()
        self._all_metrics_data_history.clear()

        self.metrics = MetricInstances(
            system_cpu=SystemCPUMetrics(
                graphs=["graph_system_cpu"],
                CPU_Percent=MetricData(
                    label="CPU %",
                    color=MetricColor.blue,
                    per_second_calculation=False,
                    create_switch=False,
                ),
            ),
            system_memory=SystemMemoryMetrics(
                graphs=["graph_system_memory"],
                Memory_Total=MetricData(
                    label="Total",
                    color=MetricColor.blue,
                    per_second_calculation=False,
                    visible=False,
                    save_history=False,
                    create_switch=False,
                ),
                Memory_Used=MetricData(
                    label="Memory Used",
                    color=MetricColor.green,
                    per_second_calculation=False,
                    create_switch=False,
                ),
            ),
            system_disk_io=SystemDiskIOMetrics(
                graphs=["graph_system_disk_io"],
                Disk_Read=MetricData(label="IOPS Read", color=MetricColor.blue),
                Disk_Write=MetricData(label="IOPS Write", color=MetricColor.yellow),
            ),
            system_network=SystemNetworkMetrics(
                graphs=["graph_system_network"],
                Network_Down=MetricData(label="Net Dn", color=MetricColor.blue),
                Network_Up=MetricData(label="Net Up", color=MetricColor.gray),
            ),
            # PostgreSQL-specific metrics
            pg_transactions=PgTransactionMetrics(
                graphs=["graph_pg_transactions"],
                xact_commit=MetricData(label="Commits", color=MetricColor.green),
                xact_rollback=MetricData(label="Rollbacks", color=MetricColor.red),
            ),
            pg_tuples=PgTupleMetrics(
                graphs=["graph_pg_tuples"],
                tup_fetched=MetricData(label="Fetched", color=MetricColor.blue),
                tup_inserted=MetricData(label="Inserted", color=MetricColor.green),
                tup_updated=MetricData(label="Updated", color=MetricColor.yellow),
                tup_deleted=MetricData(label="Deleted", color=MetricColor.red),
            ),
            pg_block_io=PgBlockIOMetrics(
                graphs=["graph_pg_block_io"],
                blks_hit=MetricData(label="Buffer Hits", color=MetricColor.green),
                blks_read=MetricData(label="Disk Reads", color=MetricColor.red),
            ),
            pg_cache_hit_ratio=PgCacheHitRatioMetrics(
                graphs=["graph_pg_cache"],
                cache_hit_ratio=MetricData(
                    label="Hit Ratio %",
                    color=MetricColor.green,
                    per_second_calculation=False,
                    create_switch=False,
                ),
            ),
            pg_connections=PgConnectionMetrics(
                graphs=["graph_pg_connections"],
                active=MetricData(
                    label="Active",
                    color=MetricColor.green,
                    per_second_calculation=False,
                ),
                idle=MetricData(
                    label="Idle",
                    color=MetricColor.blue,
                    per_second_calculation=False,
                ),
                idle_in_transaction=MetricData(
                    label="Idle in Txn",
                    color=MetricColor.yellow,
                    per_second_calculation=False,
                ),
            ),
            pg_checkpoints=PgCheckpointMetrics(
                graphs=["graph_pg_checkpoints"],
                checkpoints_timed=MetricData(label="Timed", color=MetricColor.green),
                checkpoints_req=MetricData(label="Requested", color=MetricColor.yellow),
                buffers_checkpoint=MetricData(label="Buffers", color=MetricColor.blue),
            ),
            pg_temp_files=PgTempFileMetrics(
                graphs=["graph_pg_temp_files"],
                temp_files=MetricData(label="Temp Files", color=MetricColor.yellow),
                temp_bytes=MetricData(label="Temp Bytes", color=MetricColor.red),
            ),
            replication_lag=ReplicationLagMetrics(
                graphs=["graph_replication_lag"],
                lag=MetricData(
                    label="Lag",
                    color=MetricColor.blue,
                    per_second_calculation=False,
                    create_switch=False,
                ),
            ),
            # PgBouncer-specific metrics
            pgbouncer_connections=PgBouncerConnectionMetrics(
                graphs=["graph_pgbouncer_connections"],
                cl_active=MetricData(
                    label="Clients Active",
                    color=MetricColor.green,
                    per_second_calculation=False,
                ),
                cl_waiting=MetricData(
                    label="Clients Waiting",
                    color=MetricColor.yellow,
                    per_second_calculation=False,
                ),
                sv_active=MetricData(
                    label="Servers Active",
                    color=MetricColor.blue,
                    per_second_calculation=False,
                ),
                sv_idle=MetricData(
                    label="Servers Idle",
                    color=MetricColor.gray,
                    per_second_calculation=False,
                ),
            ),
            pgbouncer_traffic=PgBouncerTrafficMetrics(
                graphs=["graph_pgbouncer_traffic"],
                xact_count=MetricData(label="Transactions", color=MetricColor.green),
                query_count=MetricData(label="Queries", color=MetricColor.blue),
                bytes_received=MetricData(label="Received", color=MetricColor.yellow),
                bytes_sent=MetricData(label="Sent", color=MetricColor.purple),
            ),
        )

        # Build the optimized lookup tables
        for metric_instance in self.metrics.__dict__.values():
            if not dataclasses.is_dataclass(metric_instance):
                continue

            source = getattr(metric_instance, "metric_source", MetricSource.NONE)
            conn_source = getattr(metric_instance, "connection_source", [])

            for attr_name, metric_data in metric_instance.__dict__.items():
                if isinstance(metric_data, MetricData):
                    if metric_data.save_history:
                        self._all_metrics_data_history.append(metric_data)

                    # Add to processing list if it has a valid source
                    if source != MetricSource.NONE:
                        self._source_to_metrics_processing[source].append((attr_name, metric_data, conn_source))

    def refresh_data(
        self,
        worker_start_time: datetime,
        polling_latency: float = 0,
        system_utilization: dict[str, int] = None,
        disk_io_metrics: dict[str, int] = None,
        replication_status: dict[str, int | str] = None,
        database_stats: dict[str, int | str] = None,
        bgwriter_stats: dict[str, int | str] = None,
        connection_stats: dict[str, int | str] = None,
        global_variables: dict[str, str] = None,
        pgbouncer_pools: dict[str, int] = None,
        pgbouncer_stats: dict[str, int] = None,
    ):
        """Ingests new data from a polling worker and updates all metric values.

        Note: global_variables is accepted for compatibility but not used for metrics.
        """
        if replication_status is None:
            replication_status = {}
        if disk_io_metrics is None:
            disk_io_metrics = {}
        if system_utilization is None:
            system_utilization = {}
        if database_stats is None:
            database_stats = {}
        if bgwriter_stats is None:
            bgwriter_stats = {}
        if connection_stats is None:
            connection_stats = {}
        if pgbouncer_pools is None:
            pgbouncer_pools = {}
        if pgbouncer_stats is None:
            pgbouncer_stats = {}

        self.worker_start_time = worker_start_time
        self.polling_latency = polling_latency
        self.system_utilization.update(system_utilization)
        self.disk_io_metrics.update(disk_io_metrics)
        self.replication_status = replication_status
        # PostgreSQL-specific data
        self.database_stats.update(database_stats)
        self.bgwriter_stats.update(bgwriter_stats)
        self.connection_stats.update(connection_stats)
        # PgBouncer-specific data
        self.pgbouncer_pools.update(pgbouncer_pools)
        self.pgbouncer_stats.update(pgbouncer_stats)

        if not self.replay_file:
            self.update_metrics_per_second_values()
            self.update_metrics_replication_lag()
            self.update_pg_cache_hit_ratio()  # PostgreSQL cache hit ratio
            self.update_metrics_last_value()  # Must be last

        self.add_metric_datetime()
        self.daemon_cleanup_data()

        self.initialized = True

    def add_metric(self, metric_data: MetricData, value: int):
        """Adds a new data point to a metric's value list."""
        if self.initialized:
            if metric_data.save_history:
                metric_data.values.append(value)
            else:
                # If not saving history, just keep the latest value
                if metric_data.values:
                    metric_data.values[0] = value
                else:
                    metric_data.values.append(value)

    def add_metric_datetime(self):
        """Adds the current worker timestamp to the global datetime list."""
        if self.initialized and not self.replay_file and self.worker_start_time:
            self.datetimes.append(self.worker_start_time.strftime("%d/%m/%y %H:%M:%S"))

    def get_metric_source_data(self, metric_source: MetricSource) -> dict[str, int] | None:
        """Retrieves the raw data dictionary for a given MetricSource."""
        return self._metric_source_map.get(metric_source)

    def update_metrics_per_second_values(self):
        """Iterates over all metrics and calculates their new per-second values
        using the optimized lookup table.
        """
        for source, metric_tuples in self._source_to_metrics_processing.items():
            metric_source_data = self.get_metric_source_data(source)
            if metric_source_data is None:
                continue

            for metric_name, metric_data, conn_source in metric_tuples:
                if self.connection_source not in conn_source:
                    continue

                current_metric_source_value = metric_source_data.get(metric_name, 0)

                if metric_data.last_value is None:
                    metric_data.last_value = current_metric_source_value
                    continue

                if metric_data.per_second_calculation:
                    metric_diff = current_metric_source_value - metric_data.last_value
                    metric_status_per_sec = round(metric_diff / self.polling_latency) if self.polling_latency > 0 else 0
                else:
                    metric_status_per_sec = current_metric_source_value

                # Special case for CPU_Percent smoothing
                if metric_name == "CPU_Percent":
                    if len(metric_data.values) == 1 and metric_data.values[0] == 0:
                        metric_data.values[0] = metric_status_per_sec
                    elif (
                        metric_status_per_sec in {0, 100}
                        and abs(metric_status_per_sec - (metric_data.values[-1] if metric_data.values else 0)) > 10
                    ):
                        recent_values = list(metric_data.values)[-3:]
                        if recent_values:
                            metric_status_per_sec = sum(recent_values) / len(recent_values)

                self.add_metric(metric_data, int(metric_status_per_sec))

    def update_metrics_last_value(self):
        """Updates the 'last_value' for all metrics using the optimized lookup table."""
        for source, metric_tuples in self._source_to_metrics_processing.items():
            metric_source_data = self.get_metric_source_data(source)
            if metric_source_data is None:
                continue

            for metric_name, metric_data, _ in metric_tuples:
                metric_data.last_value = metric_source_data.get(metric_name, 0)

    def update_metrics_replication_lag(self):
        """Updates the replication lag metric."""
        self.add_metric(
            self.metrics.replication_lag.lag,
            int(self.replication_status.get("Seconds_Behind", 0)),
        )

    def update_pg_cache_hit_ratio(self):
        """Updates the PostgreSQL buffer cache hit ratio metric."""
        blks_hit = self.database_stats.get("blks_hit", 0)
        blks_read = self.database_stats.get("blks_read", 0)
        total_blocks = blks_hit + blks_read

        # No blocks read yet means 100% hit ratio (nothing was missed)
        hit_ratio = round((blks_hit / total_blocks) * 100, 1) if total_blocks > 0 else 100.0

        self.add_metric(self.metrics.pg_cache_hit_ratio.cache_hit_ratio, int(hit_ratio))

    def daemon_cleanup_data(self):
        """Cleanup data for daemon mode to keep the metrics data small."""
        if not self.daemon_mode or not self.datetimes:
            return

        time_threshold = datetime.now().astimezone() - timedelta(minutes=10)

        # Efficiently pop from the left (O(1) per item)
        while self.datetimes:
            try:
                # Peek at the leftmost datetime
                first_dt = datetime.strptime(self.datetimes[0], "%d/%m/%y %H:%M:%S").astimezone()
                if first_dt < time_threshold:
                    # If it's too old, pop it
                    self.datetimes.popleft()
                    # And pop the corresponding value from all metrics
                    for metric_data in self._all_metrics_data_history:
                        if metric_data.values:
                            metric_data.values.popleft()
                else:
                    # The first item is new enough, so all others are too
                    break
            except (ValueError, IndexError):
                try:
                    self.datetimes.popleft()  # Discard bad data
                    for metric_data in self._all_metrics_data_history:
                        if metric_data.values:
                            metric_data.values.popleft()
                except IndexError:
                    break  # Deque is empty
