"""PgBouncer panel for combined mode (PostgreSQL + PgBouncer monitoring)."""

from rich import box
from rich.style import Style
from rich.table import Table

from hastin.Modules.Functions import format_bytes, format_number
from hastin.Modules.TabManager import Tab


def create_panel(tab: Tab) -> Table:
    """Create the PgBouncer panel for combined mode."""
    hastin = tab.hastin

    table_title_style = Style(color="#bbc8e8", bold=True)

    # Main container table to hold both sections side by side
    main_table = Table(
        show_header=False,
        box=None,
        expand=True,
        padding=(0, 1),
    )
    main_table.add_column(ratio=1)
    main_table.add_column(ratio=2)

    ####################
    # PgBouncer Info   #
    ####################
    table_info = Table(
        show_header=False,
        box=None,
        title="Summary",
        title_style=table_title_style,
        expand=True,
    )

    table_info.add_column()
    table_info.add_column(min_width=20, max_width=30)

    # Version
    table_info.add_row("[label]Version", hastin.pgbouncer_version or "N/A")

    # Connection status
    if hastin.pgbouncer_connection and hastin.pgbouncer_connection.is_connected():
        status = "[green]Connected[/green]"
    else:
        status = "[red]Disconnected[/red]"
    table_info.add_row("[label]Status", status)

    # Aggregate pool stats
    pools = hastin.pgbouncer_pools
    if pools:
        total_cl_active = sum(int(p.get("cl_active", 0)) for p in pools)
        total_cl_waiting = sum(int(p.get("cl_waiting", 0)) for p in pools)
        total_sv_active = sum(int(p.get("sv_active", 0)) for p in pools)
        total_sv_idle = sum(int(p.get("sv_idle", 0)) for p in pools)
        total_sv_used = sum(int(p.get("sv_used", 0)) for p in pools)
        max_wait = max((float(p.get("maxwait", 0)) for p in pools), default=0)

        table_info.add_row("[label]Pools", str(len(pools)))

        # Client connections
        cl_wait_str = str(total_cl_waiting)
        if total_cl_waiting > 0:
            cl_wait_str = f"[yellow]{total_cl_waiting}[/yellow]"
        table_info.add_row(
            "[label]Clients",
            f"[label]active[/label] {total_cl_active}"
            f"[highlight]/[/highlight][label]wait[/label] {cl_wait_str}",
        )

        # Server connections
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
    else:
        table_info.add_row("[label]Pools", "[dark_gray]No data[/dark_gray]")

    # Traffic stats
    stats = hastin.pgbouncer_stats
    if stats:
        total_xact = sum(int(s.get("total_xact_count", 0)) for s in stats)
        total_query = sum(int(s.get("total_query_count", 0)) for s in stats)
        total_received = sum(int(s.get("total_received", 0)) for s in stats)
        total_sent = sum(int(s.get("total_sent", 0)) for s in stats)

        table_info.add_row("[label]Transactions", format_number(total_xact))
        table_info.add_row("[label]Queries", format_number(total_query))
        table_info.add_row(
            "[label]Traffic",
            f"[label]in[/label] {format_bytes(total_received)}"
            f"[highlight]/[/highlight][label]out[/label] {format_bytes(total_sent)}",
        )

    ####################
    # Pool Table       #
    ####################
    pool_table = Table(
        title="Pools",
        title_style=table_title_style,
        header_style="b",
        box=box.SIMPLE,
        show_edge=False,
        expand=True,
    )

    pool_table.add_column("Database", style="light_blue", no_wrap=True)
    pool_table.add_column("User", style="label", no_wrap=True)
    pool_table.add_column("Mode", no_wrap=True)
    pool_table.add_column("Cl Act", justify="right")
    pool_table.add_column("Cl Wait", justify="right")
    pool_table.add_column("Sv Act", justify="right")
    pool_table.add_column("Sv Idle", justify="right")
    pool_table.add_column("Max Wait", justify="right")

    if pools:
        for pool in pools:
            database = pool.get("database", "")
            user = pool.get("user", "")
            pool_mode = pool.get("pool_mode", "")

            cl_active = int(pool.get("cl_active", 0))
            cl_waiting = int(pool.get("cl_waiting", 0))
            sv_active = int(pool.get("sv_active", 0))
            sv_idle = int(pool.get("sv_idle", 0))
            maxwait = float(pool.get("maxwait", 0))

            # Color coding for waiting clients
            cl_waiting_str = f"[yellow]{cl_waiting}[/yellow]" if cl_waiting > 0 else str(cl_waiting)

            # Color coding for max wait
            if maxwait > 1.0:
                maxwait_str = f"[red]{maxwait:.2f}s[/red]"
            elif maxwait > 0.5:
                maxwait_str = f"[yellow]{maxwait:.2f}s[/yellow]"
            else:
                maxwait_str = f"{maxwait:.2f}s"

            # Pool mode coloring
            mode_colors = {
                "session": "green",
                "transaction": "yellow",
                "statement": "purple",
            }
            mode_color = mode_colors.get(pool_mode, "white")
            pool_mode_str = f"[{mode_color}]{pool_mode}[/{mode_color}]"

            pool_table.add_row(
                database,
                user,
                pool_mode_str,
                str(cl_active),
                cl_waiting_str,
                str(sv_active),
                str(sv_idle),
                maxwait_str,
            )
    else:
        pool_table.add_row("[dark_gray]No pools[/dark_gray]", "", "", "", "", "", "", "")

    main_table.add_row(table_info, pool_table)

    tab.hastin.app.query_one("#pgbouncer_panel_content").update(main_table)
