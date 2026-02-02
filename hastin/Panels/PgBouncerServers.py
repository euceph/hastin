"""PgBouncer Servers panel for standalone mode."""

from rich import box
from rich.style import Style
from rich.table import Table
from rich.text import Text

from hastin.Modules.TabManager import Tab


def create_panel(tab: Tab) -> Table:
    """Create the PgBouncer servers panel."""
    hastin = tab.hastin
    servers = hastin.pgbouncer_servers

    # Create the table
    title = Text.from_markup(f"{hastin.panels.get_key(hastin.panels.pgbouncer_servers.name)}Servers")
    title.justify = "center"

    table = Table(
        title=title,
        title_style=Style(color="#bbc8e8", bold=True),
        header_style="b",
        box=box.SIMPLE,
        show_edge=False,
        expand=True,
    )

    table.add_column("Type", no_wrap=True)
    table.add_column("User", style="label", no_wrap=True)
    table.add_column("Database", style="light_blue", no_wrap=True)
    table.add_column("State", no_wrap=True)
    table.add_column("Address", no_wrap=True)
    table.add_column("Port", justify="right")
    table.add_column("Local Addr", no_wrap=True)
    table.add_column("Connect Time", no_wrap=True)

    for server in servers:
        server_type = server.get("type", "")
        user = server.get("user", "")
        database = server.get("database", "")
        state = server.get("state", "")
        addr = server.get("addr", "")
        port = server.get("port", "")
        local_addr = server.get("local_addr", "")
        connect_time = server.get("connect_time", "")

        # State coloring
        state_colors = {
            "active": "green",
            "idle": "dark_gray",
            "used": "label",
            "tested": "yellow",
            "new": "purple",
        }
        state_color = state_colors.get(state, "white")
        state_str = f"[{state_color}]{state}[/{state_color}]"

        table.add_row(
            server_type,
            user,
            database,
            state_str,
            addr,
            str(port),
            local_addr,
            connect_time,
        )

    if not servers:
        table.add_row("[dark_gray]No servers[/dark_gray]", "", "", "", "", "", "", "")

    tab.hastin.app.query_one("#pgbouncer_servers_content").update(table)
