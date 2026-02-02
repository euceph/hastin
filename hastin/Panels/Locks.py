"""Locks panel for Hastin PostgreSQL monitoring dashboard.

Displays pg_locks data showing current lock information.
"""

import psycopg
from loguru import logger
from textual.widgets import DataTable

from hastin.Modules.Queries import PostgresQueries
from hastin.Modules.TabManager import Tab


def fetch_data(tab: Tab) -> list[dict]:
    """Fetch lock data from pg_locks."""
    hastin = tab.hastin

    # Use secondary connection for ad-hoc queries (main is used by worker)
    conn = hastin.secondary_db_connection
    if not conn or not conn.is_connected():
        return []

    try:
        conn.execute(PostgresQueries.locks)
        return conn.fetchall() or []
    except psycopg.Error as e:
        logger.error(f"Failed to fetch locks data: {e}")
        return []


def create_panel(tab: Tab) -> DataTable:
    """Create the locks panel."""
    hastin = tab.hastin

    columns = [
        {"name": "PID", "field": "pid", "width": 8},
        {"name": "Type", "field": "locktype", "width": 14},
        {"name": "Database", "field": "database", "width": 14},
        {"name": "Relation", "field": "relation", "width": 20},
        {"name": "Mode", "field": "mode", "width": 20},
        {"name": "Granted", "field": "granted", "width": 8},
        {"name": "Wait", "field": "wait_event", "width": 16},
        {"name": "Query", "field": "query", "width": None},
    ]

    locks_datatable = tab.locks_datatable

    if not locks_datatable.columns:
        for column_data in columns:
            locks_datatable.add_column(column_data["name"], key=column_data["name"], width=column_data["width"])

    # Clear existing rows
    locks_datatable.clear()

    # Get lock data from hastin state, or fetch if empty
    locks_data = getattr(hastin, "locks_data", []) or []
    if not locks_data:
        locks_data = fetch_data(tab)
        hastin.locks_data = locks_data

    for lock in locks_data:
        row_values = []
        for column_data in columns:
            field = column_data["field"]
            value = lock.get(field, "")

            # Format granted as Yes/No
            if field == "granted":
                value = "Yes" if value else "[red]No[/red]"

            # Truncate query
            if field == "query" and value:
                value = (value[:80] + "...") if len(value) > 80 else value

            row_values.append(str(value) if value is not None else "")

        locks_datatable.add_row(*row_values)

    return locks_datatable
