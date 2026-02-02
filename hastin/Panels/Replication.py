"""Replication panel for Hastin PostgreSQL monitoring dashboard.

Supports:
- Streaming replication (primary shows replicas, replica shows upstream)
- Logical replication (subscriptions)
- Replication slots
"""

from rich.style import Style
from rich.table import Table
from textual._node_list import DuplicateIds
from textual.containers import ScrollableContainer
from textual.widgets import Static

from hastin.Modules.Functions import format_bytes
from hastin.Modules.TabManager import Tab


def create_panel(tab: Tab):
    """Create the replication panel."""
    hastin = tab.hastin

    # Create the appropriate replication panel based on role
    if hastin.replication_role == "primary":
        create_primary_panel(tab)
    elif hastin.replication_role == "replica":
        create_replica_panel(tab)
    else:
        # Standalone - check for logical subscriptions
        tab.replication_container.display = False

    # Create logical subscriptions panel if applicable (skip if UI not ready)
    if hasattr(tab, "logical_subscriptions_container"):
        create_logical_subscriptions_panel(tab)

    # Create replication slots panel (skip if UI not ready)
    if hasattr(tab, "replication_slots_container"):
        create_replication_slots_panel(tab)


def create_primary_panel(tab: Tab):
    """Create the replication panel for a primary server."""
    hastin = tab.hastin
    panels = hastin.panels

    replicas = hastin.replication_status.get("replicas", [])

    # Hide the replica status container (that's for when WE are a replica)
    tab.replication_container.display = False

    if not replicas:
        tab.replicas_container.display = False
        return

    tab.replicas_container.display = True

    # Update title
    title_prefix = panels.get_key(panels.replication.name)
    tab.replicas_title.update(f"[b]{title_prefix}Streaming Replicas ([$highlight]{len(replicas)}[/$highlight])")

    # Create tables for each replica
    existing_replica_ids = set()
    existing_replica_components = {
        replica.id.split("_")[1]: replica for replica in tab.hastin.app.query(f".replica_{tab.id}")
    }

    for replica_data in replicas:
        pid = replica_data.get("pid")
        replica_id = str(pid)
        existing_replica_ids.add(replica_id)

        table = create_streaming_replica_table(replica_data)

        if replica_id in existing_replica_components:
            existing_replica_components[replica_id].update(table)
        else:
            try:
                tab.replicas_grid.mount(
                    ScrollableContainer(
                        Static(
                            table,
                            id=f"replica_{replica_id}_{tab.id}",
                            classes=f"replica_{tab.id}",
                        ),
                        id=f"replica_container_{replica_id}_{tab.id}",
                        classes=f"replica_container_{tab.id} replica_container",
                    )
                )
            except DuplicateIds:
                tab.hastin.app.notify(
                    f"Failed to mount replica [$highlight]{replica_data.get('client_addr')}",
                    severity="error",
                )

    # Remove replicas that no longer exist
    for replica_id, container in existing_replica_components.items():
        if replica_id not in existing_replica_ids:
            container.parent.remove()


def create_replica_panel(tab: Tab):
    """Create the replication panel for a replica server."""
    hastin = tab.hastin
    panels = hastin.panels

    replication_status = hastin.replication_status

    if not replication_status:
        tab.replication_container.display = False
        return

    tab.replication_container.display = True

    # Update title
    title_prefix = panels.get_key(panels.replication.name)
    tab.replication_title.update(f"[b]{title_prefix}Replication Status")

    # Create the replication status table
    table = create_wal_receiver_table(replication_status)
    tab.replication_status.update(table)


def create_streaming_replica_table(replica_data: dict) -> Table:
    """Create a table for a streaming replica."""
    table = Table(box=None, show_header=False)
    table.add_column()
    table.add_column(overflow="fold")

    # Replica info
    client_addr = replica_data.get("client_addr", "N/A")
    application_name = replica_data.get("application_name", "N/A")
    state = replica_data.get("state", "N/A")
    sync_state = replica_data.get("sync_state", "async")

    # Format state
    if state == "streaming":
        formatted_state = f"[green]{state}[/green]"
    elif state == "catchup":
        formatted_state = f"[yellow]{state}[/yellow]"
    else:
        formatted_state = f"[red]{state}[/red]"

    # Format sync state
    if sync_state == "sync":
        formatted_sync = "[green]sync[/green]"
    elif sync_state == "potential":
        formatted_sync = "[yellow]potential[/yellow]"
    else:
        formatted_sync = "[dark_gray]async[/dark_gray]"

    # Replication lag
    lag_bytes = replica_data.get("replication_lag_bytes", 0)
    if lag_bytes is None:
        lag_bytes = 0

    if lag_bytes > 100 * 1024 * 1024:  # > 100MB
        lag_color = "red"
    elif lag_bytes > 10 * 1024 * 1024:  # > 10MB
        lag_color = "yellow"
    else:
        lag_color = "green"

    table.add_row("[b][light_blue]Client", f"[light_blue]{client_addr}")
    table.add_row("[label]Application", application_name)
    table.add_row("[label]State", formatted_state)
    table.add_row("[label]Sync", formatted_sync)
    table.add_row("[label]Lag", f"[{lag_color}]{format_bytes(lag_bytes)}[/{lag_color}]")

    # LSN positions
    sent_lsn = replica_data.get("sent_lsn", "N/A")
    replay_lsn = replica_data.get("replay_lsn", "N/A")
    table.add_row("[label]Sent LSN", str(sent_lsn))
    table.add_row("[label]Replay LSN", str(replay_lsn))

    return table


def create_wal_receiver_table(replication_status: dict) -> Table:
    """Create a table for WAL receiver status (replica side)."""
    table = Table(box=None, show_header=False)
    table.add_column()
    table.add_column(overflow="fold")

    status = replication_status.get("status", "N/A")
    sender_host = replication_status.get("sender_host", "N/A")
    sender_port = replication_status.get("sender_port", "N/A")
    slot_name = replication_status.get("slot_name", "N/A")

    # Format status
    formatted_status = f"[green]{status}[/green]" if status == "streaming" else f"[yellow]{status}[/yellow]"

    # Lag
    lag_bytes = replication_status.get("lag_bytes", 0)
    if lag_bytes is None:
        lag_bytes = 0

    if lag_bytes > 100 * 1024 * 1024:
        lag_color = "red"
    elif lag_bytes > 10 * 1024 * 1024:
        lag_color = "yellow"
    else:
        lag_color = "green"

    table.add_row("[label]Primary", f"{sender_host}:{sender_port}")
    table.add_row("[label]Status", formatted_status)
    table.add_row("[label]Slot", str(slot_name))
    table.add_row("[label]Lag", f"[{lag_color}]{format_bytes(lag_bytes)}[/{lag_color}]")

    # LSN positions
    received_lsn = replication_status.get("received_lsn", "N/A")
    latest_end_lsn = replication_status.get("latest_end_lsn", "N/A")
    table.add_row("[label]Received LSN", str(received_lsn))
    table.add_row("[label]Latest LSN", str(latest_end_lsn))

    return table


def create_logical_subscriptions_panel(tab: Tab):
    """Create the logical subscriptions panel."""
    hastin = tab.hastin

    subscriptions = hastin.logical_subscriptions

    if not subscriptions:
        tab.logical_subscriptions_container.display = False
        return

    tab.logical_subscriptions_container.display = True

    # Update title
    tab.logical_subscriptions_title.update(f"[b]Logical Subscriptions ([$highlight]{len(subscriptions)}[/$highlight])")

    # Create table
    table = Table(box=None, header_style="#c5c7d2")
    table.add_column("Subscription")
    table.add_column("PID")
    table.add_column("Lag (bytes)")
    table.add_column("Received LSN")
    table.add_column("Latest LSN")

    for sub in subscriptions:
        lag_bytes = sub.get("lag_bytes", 0)
        if lag_bytes is None:
            lag_bytes = 0

        if lag_bytes > 100 * 1024 * 1024:
            lag_color = "red"
        elif lag_bytes > 10 * 1024 * 1024:
            lag_color = "yellow"
        else:
            lag_color = "green"

        table.add_row(
            sub.get("subname", "N/A"),
            str(sub.get("pid", "N/A")),
            f"[{lag_color}]{format_bytes(lag_bytes)}[/{lag_color}]",
            str(sub.get("received_lsn", "N/A")),
            str(sub.get("latest_end_lsn", "N/A")),
        )

    tab.logical_subscriptions_table.update(table)


def create_replication_slots_panel(tab: Tab):
    """Create the replication slots panel."""
    hastin = tab.hastin

    slots = hastin.replication_slots

    if not slots:
        tab.replication_slots_container.display = False
        return

    tab.replication_slots_container.display = True

    # Update title
    tab.replication_slots_title.update(f"[b]Replication Slots ([$highlight]{len(slots)}[/$highlight])")

    # Create table
    table = Table(box=None, header_style="#c5c7d2")
    table.add_column("Slot Name")
    table.add_column("Type")
    table.add_column("Active")
    table.add_column("Lag (bytes)")
    table.add_column("Restart LSN")

    for slot in slots:
        slot_type = slot.get("slot_type", "N/A")
        active = slot.get("active", False)
        active_str = "[green]Yes[/green]" if active else "[red]No[/red]"

        lag_bytes = slot.get("slot_lag_bytes", 0)
        if lag_bytes is None:
            lag_bytes = 0

        if lag_bytes > 1024 * 1024 * 1024:  # > 1GB
            lag_color = "red"
        elif lag_bytes > 100 * 1024 * 1024:  # > 100MB
            lag_color = "yellow"
        else:
            lag_color = "green"

        table.add_row(
            slot.get("slot_name", "N/A"),
            slot_type,
            active_str,
            f"[{lag_color}]{format_bytes(lag_bytes)}[/{lag_color}]",
            str(slot.get("restart_lsn", "N/A")),
        )

    tab.replication_slots_table.update(table)


def create_replication_table(tab: Tab, dashboard_table=False) -> Table:
    """Create a summary replication table for the dashboard."""
    hastin = tab.hastin

    table = Table(
        show_header=False,
        box=None,
        title="Replication" if dashboard_table else None,
        title_style=Style(color="#bbc8e8", bold=True) if dashboard_table else None,
    )
    table.add_column()
    table.add_column(max_width=30 if dashboard_table else None, overflow="fold")

    if hastin.replication_role == "primary":
        replicas = hastin.replication_status.get("replicas", [])
        table.add_row("[label]Role", "[green]Primary[/green]")
        table.add_row("[label]Replicas", str(len(replicas)))

        if replicas:
            # Show first replica's lag
            first_replica = replicas[0]
            lag_bytes = first_replica.get("replication_lag_bytes", 0) or 0
            table.add_row("[label]Max Lag", format_bytes(lag_bytes))

    elif hastin.replication_role == "replica":
        status = hastin.replication_status
        table.add_row("[label]Role", "[yellow]Replica[/yellow]")

        sender_host = status.get("sender_host", "N/A")
        table.add_row("[label]Primary", str(sender_host))

        recv_status = status.get("status", "N/A")
        if recv_status == "streaming":
            formatted_status = f"[green]{recv_status}[/green]"
        else:
            formatted_status = f"[yellow]{recv_status}[/yellow]"
        table.add_row("[label]Status", formatted_status)

        lag_bytes = status.get("lag_bytes", 0) or 0
        table.add_row("[label]Lag", format_bytes(lag_bytes))

    else:
        table.add_row("[label]Role", "[dark_gray]Standalone[/dark_gray]")

    # Show logical subscriptions count if any
    if hastin.logical_subscriptions:
        table.add_row("[label]Subscriptions", str(len(hastin.logical_subscriptions)))

    return table
