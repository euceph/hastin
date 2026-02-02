"""PgBouncer Pools panel for standalone mode."""

from rich import box
from rich.style import Style
from rich.table import Table
from rich.text import Text

from hastin.Modules.TabManager import Tab


def create_panel(tab: Tab) -> Table:
    """Create the PgBouncer pools panel."""
    hastin = tab.hastin
    pools = hastin.pgbouncer_pools

    # Create the table
    title = Text.from_markup(f"{hastin.panels.get_key(hastin.panels.pgbouncer_pools.name)}Pools")
    title.justify = "center"

    table = Table(
        title=title,
        title_style=Style(color="#bbc8e8", bold=True),
        header_style="b",
        box=box.SIMPLE,
        show_edge=False,
        expand=True,
    )

    table.add_column("Database", style="light_blue", no_wrap=True)
    table.add_column("User", style="label", no_wrap=True)
    table.add_column("Mode", no_wrap=True)
    table.add_column("Cl Active", justify="right")
    table.add_column("Cl Wait", justify="right")
    table.add_column("Sv Active", justify="right")
    table.add_column("Sv Idle", justify="right")
    table.add_column("Sv Used", justify="right")
    table.add_column("Max Wait", justify="right")

    for pool in pools:
        database = pool.get("database", "")
        user = pool.get("user", "")
        pool_mode = pool.get("pool_mode", "")

        cl_active = int(pool.get("cl_active", 0))
        cl_waiting = int(pool.get("cl_waiting", 0))
        sv_active = int(pool.get("sv_active", 0))
        sv_idle = int(pool.get("sv_idle", 0))
        sv_used = int(pool.get("sv_used", 0))
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

        table.add_row(
            database,
            user,
            pool_mode_str,
            str(cl_active),
            cl_waiting_str,
            str(sv_active),
            str(sv_idle),
            str(sv_used),
            maxwait_str,
        )

    if not pools:
        table.add_row("[dark_gray]No pools[/dark_gray]", "", "", "", "", "", "", "", "")

    tab.hastin.app.query_one("#pgbouncer_pools_content").update(table)
