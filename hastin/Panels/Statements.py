"""Statements panel for Hastin PostgreSQL monitoring dashboard.

Displays pg_stat_statements data showing query statistics.
Requires the pg_stat_statements extension to be installed.
"""

import psycopg
from loguru import logger
from textual.widgets import DataTable

from hastin.Modules.Functions import format_number
from hastin.Modules.Queries import PostgresQueries
from hastin.Modules.TabManager import Tab


def fetch_data(tab: Tab) -> list[dict]:
    """Fetch statement stats from pg_stat_statements."""
    hastin = tab.hastin

    # Use secondary connection for ad-hoc queries (main is used by worker)
    conn = hastin.secondary_db_connection
    if not conn or not conn.is_connected():
        return []

    if not hastin.has_pg_stat_statements:
        return []

    try:
        conn.execute(PostgresQueries.statement_stats)
        return conn.fetchall() or []
    except psycopg.Error as e:
        logger.error(f"Failed to fetch statements data: {e}")
        return []


def create_panel(tab: Tab) -> DataTable:
    """Create the statements panel."""
    hastin = tab.hastin

    columns = [
        {"name": "Calls", "field": "calls", "width": 10, "format_number": True},
        {"name": "Total Time", "field": "total_exec_time", "width": 12, "format_number": False},
        {"name": "Mean Time", "field": "mean_exec_time", "width": 10, "format_number": False},
        {"name": "Rows", "field": "rows", "width": 10, "format_number": True},
        {"name": "Hit %", "field": "cache_hit_ratio", "width": 8, "format_number": False},
        {"name": "Query", "field": "query_preview", "width": None, "format_number": False},
    ]

    statements_datatable = tab.statements_datatable

    if not statements_datatable.columns:
        for column_data in columns:
            statements_datatable.add_column(column_data["name"], key=column_data["name"], width=column_data["width"])

    statements_datatable.clear()

    if not hastin.has_pg_stat_statements:
        return statements_datatable

    # Get statement data from hastin state, or fetch if empty
    statements_data = getattr(hastin, "statements_data", []) or []
    if not statements_data:
        statements_data = fetch_data(tab)
        hastin.statements_data = statements_data

    for stmt in statements_data:
        row_values = []
        for column_data in columns:
            field = column_data["field"]
            value = stmt.get(field, "")

            # Format numbers
            if column_data.get("format_number") and value:
                value = format_number(value)

            # Format time in milliseconds
            if field in ("total_exec_time", "mean_exec_time") and value:
                try:
                    ms = float(value)
                    value = f"{ms / 1000:.2f}s" if ms >= 1000 else f"{ms:.2f}ms"
                except (ValueError, TypeError):
                    pass

            # Format cache hit ratio as percentage
            if field == "cache_hit_ratio" and value:
                try:
                    value = f"{float(value):.1f}%"
                except (ValueError, TypeError):
                    pass

            row_values.append(str(value) if value is not None else "")

        statements_datatable.add_row(*row_values)

    return statements_datatable
