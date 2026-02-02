"""PgBouncer Dashboard panel for standalone mode."""

from datetime import datetime

from rich.style import Style
from rich.table import Table
from rich.text import Text

from hastin.Modules.Functions import format_bytes, format_number
from hastin.Modules.TabManager import Tab


def create_panel(tab: Tab) -> Table:
    """Create the PgBouncer dashboard panel."""
    hastin = tab.hastin

    table_title_style = Style(color="#bbc8e8", bold=True)

    ####################
    # PgBouncer Info   #
    ####################
    info_title = Text.from_markup(
        f"{hastin.panels.get_key(hastin.panels.pgbouncer_dashboard.name)}PgBouncer"
    )
    info_title.justify = "center"
    table_info = Table(
        show_header=False,
        box=None,
        title=info_title,
        title_style=table_title_style,
        expand=True,
    )

    table_info.add_column()
    table_info.add_column(min_width=25, max_width=35)

    # Version
    table_info.add_row("[label]Version", hastin.pgbouncer_version or "N/A")

    # Aggregate pool stats
    pools = hastin.pgbouncer_pools
    total_cl_active = sum(int(p.get("cl_active", 0)) for p in pools)
    total_cl_waiting = sum(int(p.get("cl_waiting", 0)) for p in pools)
    total_sv_active = sum(int(p.get("sv_active", 0)) for p in pools)
    total_sv_idle = sum(int(p.get("sv_idle", 0)) for p in pools)
    total_sv_used = sum(int(p.get("sv_used", 0)) for p in pools)
    max_wait = max((float(p.get("maxwait", 0)) for p in pools), default=0)

    table_info.add_row("[label]Pools", str(len(pools)))
    table_info.add_row(
        "[label]Clients",
        f"[label]active[/label] {total_cl_active}"
        f"[highlight]/[/highlight][label]wait[/label] {total_cl_waiting}",
    )
    table_info.add_row(
        "[label]Servers",
        f"[label]active[/label] {total_sv_active}"
        f"[highlight]/[/highlight][label]idle[/label] {total_sv_idle}"
        f"[highlight]/[/highlight][label]used[/label] {total_sv_used}",
    )

    # Max wait time coloring
    if max_wait > 1.0:
        wait_color = "red"
    elif max_wait > 0.5:
        wait_color = "yellow"
    else:
        wait_color = "green"
    table_info.add_row("[label]Max Wait", f"[{wait_color}]{max_wait:.2f}s[/{wait_color}]")

    if not hastin.replay_file:
        runtime = str(datetime.now().astimezone() - hastin.hastin_start_time).split(".")[0]
        table_info.add_row(
            "[label]Runtime",
            f"{runtime} [label]Latency[/label] {round(hastin.worker_processing_time, 2)}s",
        )

    tab.dashboard_section_1.update(table_info)
    tab.dashboard_section_1.display = True

    ####################
    # Aggregate Stats  #
    ####################
    stats = hastin.pgbouncer_stats
    if stats:
        table_stats = Table(
            show_header=False,
            box=None,
            title="Traffic Stats",
            title_style=table_title_style,
        )
        table_stats.add_column()
        table_stats.add_column(min_width=12)

        # Sum up stats across all databases
        total_xact = sum(int(s.get("total_xact_count", 0)) for s in stats)
        total_query = sum(int(s.get("total_query_count", 0)) for s in stats)
        total_received = sum(int(s.get("total_received", 0)) for s in stats)
        total_sent = sum(int(s.get("total_sent", 0)) for s in stats)
        avg_query_time = 0
        if stats:
            # Average of averages (weighted would be better, but this is simpler)
            times = [float(s.get("avg_query_time", 0)) for s in stats if s.get("avg_query_time")]
            if times:
                avg_query_time = sum(times) / len(times)

        table_stats.add_row("[label]Transactions", format_number(total_xact))
        table_stats.add_row("[label]Queries", format_number(total_query))
        table_stats.add_row("[label]Received", format_bytes(total_received))
        table_stats.add_row("[label]Sent", format_bytes(total_sent))
        table_stats.add_row("[label]Avg Query", f"{avg_query_time:.2f}ms")

        tab.dashboard_section_2.update(table_stats)
        tab.dashboard_section_2.display = True
    else:
        tab.dashboard_section_2.display = False

    # Hide unused sections for PgBouncer mode
    tab.dashboard_section_3.display = False
    tab.dashboard_section_4.display = False
    tab.dashboard_section_5.display = False
    # Section 6 is for system utilization - keep it if system_utilization is available
    if not hastin.system_utilization:
        tab.dashboard_section_6.display = False
