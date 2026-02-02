"""Dashboard panel for Hastin PostgreSQL monitoring dashboard."""

from datetime import datetime, timedelta

from rich.style import Style
from rich.table import Table
from rich.text import Text

from hastin.Modules.Functions import format_bytes, format_number
from hastin.Modules.TabManager import Tab
from hastin.Panels import Replication as ReplicationPanel


def create_panel(tab: Tab) -> Table:
    """Create the main dashboard panel."""
    hastin = tab.hastin

    server_info = hastin.server_info
    connection_stats = hastin.connection_stats
    database_stats = hastin.database_stats

    table_title_style = Style(color="#bbc8e8", bold=True)

    ####################
    # Host Information #
    ####################
    host_info_title = Text.from_markup(
        f"{hastin.panels.get_key(hastin.panels.dashboard.name)}Host Information"
    )
    host_info_title.justify = "center"
    table_information = Table(
        show_header=False,
        box=None,
        title=host_info_title,
        title_style=table_title_style,
        expand=True,
    )

    # Determine host type
    if hastin.replication_role == "primary":
        host_type = "Primary"
    elif hastin.replication_role == "replica":
        host_type = "Replica"
    else:
        host_type = "Standalone"

    # Prefix with cloud provider name if detected
    if hastin.detected_cloud_provider:
        # Use short names for common providers
        provider_prefixes = {
            "aurora": "Aurora",
            "rds": "RDS",
            "cloud_sql": "Cloud SQL",
            "alloydb": "AlloyDB",
            "azure": "Azure",
            "cosmos_citus": "Cosmos",
            "supabase": "Supabase",
            "neon": "Neon",
            "aiven": "Aiven",
            "crunchy_bridge": "Crunchy",
            "digitalocean": "DO",
            "heroku": "Heroku",
            "timescale": "Timescale",
            "railway": "Railway",
            "render": "Render",
            "fly": "Fly",
        }
        prefix = provider_prefixes.get(hastin.detected_cloud_provider, "")
        if prefix:
            host_type = f"{prefix} {host_type}"

    replicas_count = len(hastin.replication_status.get("replicas", [])) if hastin.replication_role == "primary" else 0

    table_information.add_column()
    table_information.add_column(min_width=25, max_width=35)
    table_information.add_row("[label]Version", f"{hastin.host_distro} {hastin.host_version}")
    table_information.add_row("[label]Role", host_type)
    table_information.add_row("[label]Uptime", str(timedelta(seconds=server_info.get("uptime_seconds", 0))))
    table_information.add_row("[label]Database", server_info.get("current_db", "N/A"))

    if hastin.replication_role == "primary":
        table_information.add_row("[label]Replicas", str(replicas_count))

    # Connection stats
    table_information.add_row(
        "[label]Connections",
        f"[label]total[/label] {format_number(connection_stats.get('total_connections', 0))}"
        f"[highlight]/[/highlight][label]max[/label] {format_number(connection_stats.get('max_connections', 0))}",
    )
    table_information.add_row(
        "[label]Active",
        f"[label]active[/label] {format_number(connection_stats.get('active', 0))}"
        f"[highlight]/[/highlight][label]idle[/label] {format_number(connection_stats.get('idle', 0))}"
        f"[highlight]/[/highlight][label]wait[/label] {format_number(connection_stats.get('waiting', 0))}",
    )

    if connection_stats.get("idle_in_transaction", 0) > 0:
        idle_in_txn = connection_stats.get("idle_in_transaction", 0)
        table_information.add_row("[label]Idle in Txn", f"[yellow]{idle_in_txn}[/yellow]")

    if not hastin.replay_file:
        runtime = str(datetime.now().astimezone() - hastin.hastin_start_time).split(".")[0]
        table_information.add_row(
            "[label]Runtime",
            f"{runtime} [label]Latency[/label] {round(hastin.worker_processing_time, 2)}s",
        )

    tab.dashboard_section_1.update(table_information)

    ######################
    # System Utilization #
    ######################
    table = create_system_utilization_table(tab)

    if table:
        tab.dashboard_section_6.update(table)

    ###################
    # Database Stats  #
    ###################
    statement_counts = hastin.statement_type_counts

    # Use combined table if we have statement counts, otherwise single table
    if statement_counts:
        table_db = Table(show_header=False, box=None, title="Database Stats / Queries", title_style=table_title_style)
        table_db.add_column()  # Label 1
        table_db.add_column(width=10)  # Value 1
        table_db.add_column(width=2)  # Spacer
        table_db.add_column()  # Label 2
        table_db.add_column(width=10)  # Value 2
    else:
        table_db = Table(show_header=False, box=None, title="Database Stats", title_style=table_title_style)
        table_db.add_column()
        table_db.add_column(width=12)

    # Cache hit ratio
    cache_hit_ratio = database_stats.get("cache_hit_ratio", 0)
    if cache_hit_ratio >= 99:
        cache_color = "green"
    elif cache_hit_ratio >= 95:
        cache_color = "yellow"
    else:
        cache_color = "red"

    if statement_counts:
        # Combined layout: Database Stats on left, Queries on right
        table_db.add_row(
            "[label]Cache Hit", f"[{cache_color}]{cache_hit_ratio}%[/{cache_color}]",
            "",
            "[label]SELECT", format_number(int(statement_counts.get("select_calls", 0)))
        )
        table_db.add_row(
            "[label]Commits", format_number(database_stats.get("xact_commit", 0)),
            "",
            "[label]INSERT", format_number(int(statement_counts.get("insert_calls", 0)))
        )
        table_db.add_row(
            "[label]Rollbacks", format_number(database_stats.get("xact_rollback", 0)),
            "",
            "[label]UPDATE", format_number(int(statement_counts.get("update_calls", 0)))
        )
        table_db.add_row(
            "[label]Rows Read", format_number(database_stats.get("tup_fetched", 0)),
            "",
            "[label]DELETE", format_number(int(statement_counts.get("delete_calls", 0)))
        )
        table_db.add_row("[label]Rows Insert", format_number(database_stats.get("tup_inserted", 0)), "", "", "")
        table_db.add_row("[label]Rows Update", format_number(database_stats.get("tup_updated", 0)), "", "", "")
        table_db.add_row("[label]Rows Delete", format_number(database_stats.get("tup_deleted", 0)), "", "", "")

        # Conflicts and deadlocks
        deadlocks = database_stats.get("deadlocks", 0)
        if deadlocks > 0:
            table_db.add_row("[label]Deadlocks", f"[red]{deadlocks}[/red]", "", "", "")
        else:
            table_db.add_row("[label]Deadlocks", "0", "", "", "")

        conflicts = database_stats.get("conflicts", 0)
        if conflicts > 0:
            table_db.add_row("[label]Conflicts", f"[yellow]{conflicts}[/yellow]", "", "", "")
    else:
        # Single table layout (no pg_stat_statements)
        table_db.add_row("[label]Cache Hit", f"[{cache_color}]{cache_hit_ratio}%[/{cache_color}]")
        table_db.add_row("[label]Commits", format_number(database_stats.get("xact_commit", 0)))
        table_db.add_row("[label]Rollbacks", format_number(database_stats.get("xact_rollback", 0)))
        table_db.add_row("[label]Rows Read", format_number(database_stats.get("tup_fetched", 0)))
        table_db.add_row("[label]Rows Insert", format_number(database_stats.get("tup_inserted", 0)))
        table_db.add_row("[label]Rows Update", format_number(database_stats.get("tup_updated", 0)))
        table_db.add_row("[label]Rows Delete", format_number(database_stats.get("tup_deleted", 0)))

        deadlocks = database_stats.get("deadlocks", 0)
        if deadlocks > 0:
            table_db.add_row("[label]Deadlocks", f"[red]{deadlocks}[/red]")
        else:
            table_db.add_row("[label]Deadlocks", "0")

        conflicts = database_stats.get("conflicts", 0)
        if conflicts > 0:
            table_db.add_row("[label]Conflicts", f"[yellow]{conflicts}[/yellow]")

    tab.dashboard_section_2.update(table_db)

    ####################
    # Checkpoint Stats #
    ####################
    table_checkpoint = Table(show_header=False, box=None, title="Checkpoints", title_style=table_title_style)

    bgwriter_stats = hastin.bgwriter_stats
    if bgwriter_stats:
        table_checkpoint.add_column()
        table_checkpoint.add_column(width=12)

        table_checkpoint.add_row("[label]Timed", format_number(bgwriter_stats.get("checkpoints_timed", 0)))
        table_checkpoint.add_row("[label]Requested", format_number(bgwriter_stats.get("checkpoints_req", 0)))

        # Buffers
        table_checkpoint.add_row("[label]Buf Chkpt", format_number(bgwriter_stats.get("buffers_checkpoint", 0)))
        table_checkpoint.add_row("[label]Buf Clean", format_number(bgwriter_stats.get("buffers_clean", 0)))
        table_checkpoint.add_row("[label]Buf Backend", format_number(bgwriter_stats.get("buffers_backend", 0)))
        table_checkpoint.add_row("[label]Buf Alloc", format_number(bgwriter_stats.get("buffers_alloc", 0)))

        tab.dashboard_section_3.display = True
        tab.dashboard_section_3.update(table_checkpoint)
    else:
        tab.dashboard_section_3.display = False

    ###############
    # Replication #
    ###############
    if hastin.replication_status and not hastin.panels.replication.visible:
        tab.dashboard_section_5.display = True
        tab.dashboard_section_5.update(ReplicationPanel.create_replication_table(tab, dashboard_table=True))
    else:
        tab.dashboard_section_5.display = False

    ###############
    # Statistics #
    ###############
    if hastin.show_statistics_per_second:
        tab.dashboard_section_4.display = True

        table_stats = Table(show_header=False, box=None, title="Statistics/s", title_style=table_title_style)

        table_stats.add_column()
        table_stats.add_column(min_width=8)

        # PostgreSQL transaction and tuple metrics
        pg_txn = hastin.metric_manager.metrics.pg_transactions
        pg_tuples = hastin.metric_manager.metrics.pg_tuples

        # Transaction rate (commits + rollbacks per second)
        commits = pg_txn.xact_commit.values[-1] if pg_txn.xact_commit.values else 0
        rollbacks = pg_txn.xact_rollback.values[-1] if pg_txn.xact_rollback.values else 0
        table_stats.add_row("[label]TXN/s", format_number(commits + rollbacks))

        # Tuple operation rates
        tuple_metrics = [
            ("Fetched/s", pg_tuples.tup_fetched),
            ("Insert/s", pg_tuples.tup_inserted),
            ("Update/s", pg_tuples.tup_updated),
            ("Delete/s", pg_tuples.tup_deleted),
        ]

        for label, metric_data in tuple_metrics:
            if metric_data and metric_data.values:
                table_stats.add_row(f"[label]{label}", format_number(metric_data.values[-1]))
            else:
                table_stats.add_row(f"[label]{label}", "0")

        tab.dashboard_section_4.update(table_stats)
    else:
        tab.dashboard_section_4.display = False


def create_system_utilization_table(tab: Tab) -> Table:
    """Create the system utilization table."""
    hastin = tab.hastin

    system_utilization = hastin.system_utilization
    if not system_utilization:
        return None

    # Include provider name in title if available
    provider_suffix = ""
    if hastin.system_metrics_provider:
        provider_name = hastin.system_metrics_provider.name
        if provider_name and provider_name != "none":
            provider_suffix = f" ({provider_name})"

    table = Table(
        show_header=False,
        box=None,
        title=f"System Utilization{provider_suffix}",
        title_style=Style(color="#bbc8e8", bold=True),
    )
    table.add_column()
    table.add_column(min_width=18, max_width=25)

    def format_percent(value, thresholds=(80, 90), colors=("green", "yellow", "red")):
        if value > thresholds[1]:
            return f"[{colors[2]}]{value}%[/{colors[2]}]"
        elif value > thresholds[0]:
            return f"[{colors[1]}]{value}%[/{colors[1]}]"
        return f"[{colors[0]}]{value}%[/{colors[0]}]"

    # Uptime
    uptime = system_utilization.get("Uptime", "N/A")
    table.add_row("[label]Uptime", str(timedelta(seconds=uptime)) if uptime != "N/A" else "N/A")

    # CPU
    cpu_percent_values = hastin.metric_manager.metrics.system_cpu.CPU_Percent.values
    if cpu_percent_values:
        cpu_percent = round(cpu_percent_values[-1], 2)
        formatted_cpu_percent = format_percent(cpu_percent)
        cpu_cores = system_utilization.get("CPU_Count", "N/A")
        table.add_row("[label]CPU", f"{formatted_cpu_percent} [label]cores[/label] {cpu_cores}")
    else:
        table.add_row("[label]CPU", "N/A")

    # CPU Load
    load_averages = system_utilization.get("CPU_Load_Avg")
    if load_averages:
        formatted_load = " ".join(f"{avg:.2f}" for avg in load_averages)
        table.add_row("[label]Load", formatted_load)

    # Memory
    memory_used = hastin.metric_manager.metrics.system_memory.Memory_Used.last_value
    memory_total = hastin.metric_manager.metrics.system_memory.Memory_Total.last_value
    if memory_used and memory_total:
        memory_percent_used = round((memory_used / memory_total) * 100, 2)
        formatted_memory_percent_used = format_percent(memory_percent_used)
        table.add_row(
            "[label]Memory",
            (
                f"{formatted_memory_percent_used}\n{format_bytes(memory_used)}"
                f"[dark_gray]/[/dark_gray]{format_bytes(memory_total)}"
            ),
        )
    else:
        table.add_row("[label]Memory", "N/A\n")

    # Swap
    swap_used = format_bytes(system_utilization.get("Swap_Used", "N/A"))
    swap_total = format_bytes(system_utilization.get("Swap_Total", "N/A"))
    table.add_row("[label]Swap", f"{swap_used}[dark_gray]/[/dark_gray]{swap_total}")

    # Disk I/O
    disk_read_values = hastin.metric_manager.metrics.system_disk_io.Disk_Read.values
    disk_write_values = hastin.metric_manager.metrics.system_disk_io.Disk_Write.values
    if disk_read_values and disk_write_values:
        last_disk_read = format_number(disk_read_values[-1])
        last_disk_write = format_number(disk_write_values[-1])
        table.add_row(
            "[label]Disk",
            f"[label]IOPS R[/label] {last_disk_read}\n[label]IOPS W[/label] {last_disk_write}",
        )
    else:
        table.add_row("[label]Disk", "[label]IOPS R[/label] N/A\n[label]IOPS W[/label] N/A")

    return table
