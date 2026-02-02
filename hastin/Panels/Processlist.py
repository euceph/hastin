"""Processlist panel for Hastin PostgreSQL monitoring dashboard."""

import psycopg
from loguru import logger
from rich.syntax import Syntax
from textual.widgets import DataTable

from hastin.DataTypes import ProcesslistThread
from hastin.Modules.Functions import format_number, format_query
from hastin.Modules.Queries import PostgresQueries
from hastin.Modules.TabManager import Tab


def create_panel(tab: Tab) -> DataTable:
    """Create the processlist panel."""
    hastin = tab.hastin

    columns = [
        {"name": "PID", "field": "id", "width": 8, "format_number": False},
        {"name": "User", "field": "user", "width": 16, "format_number": False},
        {"name": "Database", "field": "db", "width": 14, "format_number": False},
        {"name": "Host", "field": "host", "width": 16, "format_number": False},
        {"name": "State", "field": "formatted_state", "width": 18, "format_number": False},
        {"name": "Wait", "field": "formatted_wait", "width": 20, "format_number": False},
        {"name": "Age", "field": "formatted_time", "width": 9, "format_number": False},
        {"name": "Query", "field": "formatted_query", "width": None, "format_number": False},
        {"name": "time_seconds", "field": "time", "width": 0, "format_number": False},
    ]

    query_length_max = 300
    processlist_datatable = tab.processlist_datatable

    if len(processlist_datatable.columns) != len(columns):
        processlist_datatable.clear(columns=True)

    column_names = []
    column_fields = []
    column_format_numbers = []

    if not processlist_datatable.columns:
        for column_data in columns:
            processlist_datatable.add_column(column_data["name"], key=column_data["name"], width=column_data["width"])

    for column_data in columns:
        column_names.append(column_data["name"])
        column_fields.append(column_data["field"])
        column_format_numbers.append(column_data["format_number"])

    threads_to_render: dict[str, ProcesslistThread] = {}

    # Apply filters for replays
    if hastin.replay_file:
        for thread_id, thread in hastin.processlist_threads.items():
            thread: ProcesslistThread

            if hastin.user_filter and hastin.user_filter != thread.user:
                continue

            if hastin.db_filter and hastin.db_filter != thread.db:
                continue

            if hastin.host_filter and hastin.host_filter not in thread.host:
                continue

            if hastin.query_time_filter and thread.time < hastin.query_time_filter:
                continue

            if hastin.query_filter and hastin.query_filter not in thread.formatted_query.code:
                continue

            if hastin.state_filter and hastin.state_filter != thread.state:
                continue

            if not hastin.show_idle_threads and thread.state in ("idle", ""):
                continue

            threads_to_render[thread_id] = thread
    else:
        threads_to_render = hastin.processlist_threads

    for thread_id, thread in threads_to_render.items():
        thread: ProcesslistThread

        if thread_id in processlist_datatable.rows:
            datatable_row = processlist_datatable.get_row(thread_id)

            for column_id, (
                column_name,
                column_field,
                column_format_number,
            ) in enumerate(zip(column_names, column_fields, column_format_numbers, strict=False)):
                column_value = getattr(thread, column_field)
                thread_value = format_number(column_value) if column_format_number else column_value

                datatable_value = datatable_row[column_id]

                temp_thread_value = thread_value
                temp_datatable_value = datatable_value
                update_width = False

                if column_field == "formatted_query":
                    update_width = True
                    if isinstance(thread_value, Syntax):
                        temp_thread_value = thread_value.code[:query_length_max]
                        thread_value = format_query(temp_thread_value)
                    if isinstance(datatable_value, Syntax):
                        temp_datatable_value = datatable_value.code

                if (
                    temp_thread_value != temp_datatable_value
                    or column_field == "formatted_time"
                    or column_field == "time"
                ):
                    processlist_datatable.update_cell(thread_id, column_name, thread_value, update_width=update_width)

        else:
            row_values = []
            for column_field, column_format_number in zip(column_fields, column_format_numbers, strict=False):
                column_value = getattr(thread, column_field)
                thread_value = format_number(column_value) if column_format_number else column_value

                if column_field == "formatted_query" and isinstance(thread_value, Syntax):
                    thread_value = format_query(thread_value.code[:query_length_max])

                row_values.append(thread_value)

            if row_values:
                processlist_datatable.add_row(*row_values, key=thread_id)

    if hastin.replay_file:
        hastin.processlist_threads = threads_to_render

    # Remove rows from datatable that are no longer in our render list
    if threads_to_render:
        rows_to_remove = set(processlist_datatable.rows.keys()) - set(threads_to_render.keys())
        for id in rows_to_remove:
            processlist_datatable.remove_row(id)
    else:
        if processlist_datatable.row_count:
            processlist_datatable.clear()

    processlist_datatable.sort("time_seconds", reverse=hastin.sort_by_time_descending)

    # Build title with count and optional limited visibility indicator
    base_title = hastin.panels.get_panel_title(hastin.panels.processlist.name)
    count_str = f"([$highlight]{processlist_datatable.row_count}[/$highlight])"

    if not hastin.has_full_visibility:
        title = f"{base_title} {count_str} [$dark_gray](your sessions)[/$dark_gray]"
    else:
        title = f"{base_title} {count_str}"

    tab.processlist_title.update(title)


def fetch_data(tab: Tab) -> dict[str, ProcesslistThread]:
    """Fetch processlist data from pg_stat_activity."""
    hastin = tab.hastin

    # Ensure connection is available
    if not hastin.main_db_connection or not hastin.main_db_connection.is_connected():
        return {}

    try:
        # Build filters for the query using parameterized queries to prevent SQL injection
        filters = []
        params = []

        if not hastin.show_idle_threads:
            filters.append("state NOT IN ('idle', '')")

        if hastin.user_filter:
            filters.append("usename = %s")
            params.append(hastin.user_filter)

        if hastin.db_filter:
            filters.append("datname = %s")
            params.append(hastin.db_filter)

        if hastin.host_filter:
            filters.append("client_addr::text LIKE %s")
            params.append(f"{hastin.host_filter}%")

        if hastin.query_time_filter:
            filters.append("EXTRACT(EPOCH FROM (now() - query_start))::int >= %s")
            params.append(hastin.query_time_filter)

        if hastin.query_filter:
            filters.append("query LIKE %s")
            params.append(f"%{hastin.query_filter}%")

        if hastin.state_filter:
            filters.append("state = %s")
            params.append(hastin.state_filter)

        # Build the query
        if filters:
            filter_clause = " AND " + " AND ".join(filters)
            query = PostgresQueries.processlist_filtered.format(filters=filter_clause)
        else:
            query = PostgresQueries.processlist

        hastin.main_db_connection.execute(query, tuple(params) if params else None)
        threads = hastin.main_db_connection.fetchall() or []

        processlist_threads = {}
        for thread in threads:
            # Don't include Hastin's own connections
            if hastin.main_db_connection.connection_id == thread["pid"] or (
                hastin.secondary_db_connection and hastin.secondary_db_connection.connection_id == thread["pid"]
            ):
                continue

            # Clean up query
            thread["query"] = thread.get("query") or ""

            # Resolve hostname if possible
            if thread.get("host") and thread["host"] != "local":
                host = thread["host"].split(":")[0]
                thread["host"] = hastin.get_hostname(host)

            processlist_threads[str(thread["pid"])] = ProcesslistThread(thread)

        return processlist_threads

    except psycopg.Error as e:
        logger.error(f"Failed to fetch processlist data: {e}")
        return {}
