from __future__ import annotations

import re
import string
import time
from ssl import SSLError
from typing import TYPE_CHECKING

import psycopg
from loguru import logger
from psycopg.rows import dict_row

from hastin.DataTypes import ConnectionSource
from hastin.Modules.ManualException import ManualException
from hastin.Modules.Queries import PostgresQueries

if TYPE_CHECKING:
    from textual.app import App


class Database:
    """PostgreSQL database connection handler using psycopg3."""

    # Default query timeout in milliseconds (30 seconds)
    DEFAULT_STATEMENT_TIMEOUT = 30000

    def __init__(
        self,
        app: App,
        host: str,
        user: str,
        password: str,
        database: str,
        port: int,
        ssl_mode: str = "prefer",
        save_connection_id: bool = True,
        auto_connect: bool = True,
        daemon_mode: bool = False,
        pgbouncer_mode: bool = False,
        statement_timeout: int = None,
    ):
        self.app = app
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.ssl_mode = ssl_mode
        self.save_connection_id = save_connection_id
        self.daemon_mode = daemon_mode
        self.pgbouncer_mode = pgbouncer_mode
        self.statement_timeout = statement_timeout if statement_timeout is not None else self.DEFAULT_STATEMENT_TIMEOUT

        # PostgreSQL error codes for privilege issues
        self._PRIVILEGE_ERROR_CODES = {
            "42501",  # INSUFFICIENT_PRIVILEGE
            "42000",  # SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
            "42P01",  # UNDEFINED_TABLE
        }

        self.connection: psycopg.Connection = None
        self.connection_id: int = None
        self.source: ConnectionSource = ConnectionSource.postgresql
        self.is_running_query: bool = False
        self.has_connected: bool = False
        self.last_execute_successful: bool = False
        self.privilege_errors_notified: set = set()
        self.non_printable_regex = re.compile(f"[^{re.escape(string.printable)}]")

        if daemon_mode:
            self.max_reconnect_attempts: int = 999999999
        else:
            self.max_reconnect_attempts: int = 3

        if auto_connect:
            self.connect()

    def connect(self, reconnect_attempt: bool = False):
        """Establish connection to PostgreSQL server."""
        try:
            # Build connection string with statement timeout for query protection
            # Note: options parameter sets session-level statement_timeout
            conninfo = (
                f"host={self.host} "
                f"port={self.port} "
                f"user={self.user} "
                f"password={self.password} "
                f"dbname={self.database} "
                f"sslmode={self.ssl_mode} "
                f"application_name=Hastin "
                f"connect_timeout=5"
            )

            # Add statement_timeout for PostgreSQL connections (not PgBouncer)
            if not self.pgbouncer_mode:
                conninfo += f" options='-c statement_timeout={self.statement_timeout}'"

            # PgBouncer admin only supports simple query protocol
            if self.pgbouncer_mode:
                self.connection = psycopg.connect(
                    conninfo,
                    row_factory=dict_row,
                    autocommit=True,
                    prepare_threshold=None,  # Disable prepared statements for simple query protocol
                )
            else:
                self.connection = psycopg.connect(
                    conninfo,
                    row_factory=dict_row,
                    autocommit=True,
                )

            # Get backend PID for processlist filtering (skip for PgBouncer)
            if self.save_connection_id and not self.pgbouncer_mode:
                with self.connection.cursor() as cur:
                    cur.execute("SELECT pg_backend_pid()")
                    result = cur.fetchone()
                    self.connection_id = result["pg_backend_pid"] if result else None

            if self.pgbouncer_mode:
                logger.info("Connected to PgBouncer")
            else:
                logger.info(f"Connected to PostgreSQL with backend PID {self.connection_id}")
            self.has_connected = True

        except psycopg.Error as e:
            if reconnect_attempt:
                logger.error(f"Failed to reconnect to PostgreSQL: {e}")
                escaped_error_message = str(e).replace("[", "\\[")
                self.app.notify(
                    (
                        f"[$b_light_blue]{self.host}:{self.port}[/$b_light_blue]: "
                        f"Failed to reconnect to PostgreSQL: {escaped_error_message}"
                    ),
                    title="PostgreSQL Reconnection Failed",
                    severity="error",
                    timeout=10,
                )
            else:
                raise ManualException(str(e)) from e
        except SSLError as e:
            raise ManualException(f"SSL error: {e}") from e

    def close(self):
        """Close the database connection."""
        if self.is_connected():
            self.connection.close()

    def is_connected(self) -> bool:
        """Check if the connection is open."""
        return self.connection is not None and not self.connection.closed

    def _process_row(self, row: dict) -> dict:
        """Process a row, decoding bytes if necessary."""
        if row is None:
            return {}
        return {field: self._decode_value(value) for field, value in row.items()}

    def _decode_value(self, value):
        """Decode byte values to strings."""
        if isinstance(value, (bytes, bytearray)):
            try:
                decoded_value = value.decode("utf-8")
            except UnicodeDecodeError:
                try:
                    decoded_value = value.decode("latin-1")
                except UnicodeDecodeError:
                    return f"/* Failed to decode, returning hex: {value.hex()} */"
            return self.non_printable_regex.sub("?", decoded_value)
        return value

    def fetchall(self) -> list[dict]:
        """Fetch all rows from the last query."""
        if not self.is_connected() or not self.last_execute_successful:
            return []

        rows = self.cursor.fetchall()
        return [self._process_row(row) for row in rows] if rows else []

    def fetchone(self) -> dict:
        """Fetch a single row from the last query."""
        if not self.is_connected() or not self.last_execute_successful:
            return {}

        row = self.cursor.fetchone()
        return self._process_row(row) if row else {}

    def fetch_value_from_field(self, query: str, field: str = None, values: tuple = None):
        """Execute a query and return a single field value."""
        if not self.is_connected():
            return None

        self.execute(query, values)
        data = self.fetchone()

        if not data:
            return None

        field = field or next(iter(data))
        value = data.get(field)
        return self._decode_value(value)

    def fetch_status_and_variables(self, command: str) -> dict:
        """Fetch PostgreSQL settings or stats as a dictionary."""
        if not self.is_connected():
            return {}

        query = getattr(PostgresQueries, command, None)
        if not query:
            return {}

        self.execute(query)
        data = self.fetchall()

        # Convert list of name/value pairs to dictionary
        result = {}
        for row in data:
            name = row.get("name") or row.get("setting_name")
            value = row.get("setting") or row.get("value")
            if name and value is not None:
                # Try to convert numeric values
                if isinstance(value, str) and value.lstrip("-").isdigit():
                    result[name] = int(value)
                else:
                    result[name] = value
        return result

    def execute(self, query: str, values: tuple = None, ignore_error: bool = False):
        """Execute a SQL query."""
        if not self.is_connected():
            self.last_execute_successful = False
            return None

        if self.is_running_query:
            self.app.notify(
                "Another query is already running, please repeat action",
                title="Unable to run multiple queries at the same time",
                severity="error",
                timeout=10,
            )
            self.last_execute_successful = False
            return None

        # Prefix queries with Hastin comment for identification (skip for PgBouncer)
        prefixed_query = query if self.pgbouncer_mode else "/* Hastin */ " + query
        raw_query = query

        if raw_query in self.privilege_errors_notified:
            self.last_execute_successful = False
            return None

        for attempt_number in range(self.max_reconnect_attempts):
            self.is_running_query = True
            error_message = None

            try:
                self.cursor = self.connection.cursor()
                if values:
                    self.cursor.execute(prefixed_query, values)
                else:
                    self.cursor.execute(prefixed_query)

                self.is_running_query = False
                self.last_execute_successful = True

                return self.cursor.rowcount

            except AttributeError:
                self.is_running_query = False
                self.last_execute_successful = False
                self.close()
                self.connect()
                time.sleep(1)

            except psycopg.Error as e:
                self.is_running_query = False
                self.last_execute_successful = False

                error_code = e.sqlstate or ""
                error_message = str(e)

                if error_code in self._PRIVILEGE_ERROR_CODES:
                    if raw_query not in self.privilege_errors_notified:
                        self.privilege_errors_notified.add(raw_query)
                        logger.warning(
                            f"Privilege error ({error_code}): {error_message}. "
                            f"Query: {raw_query}. "
                            f"This query will be skipped."
                        )
                        escaped_error_message = error_message.replace("[", "\\[")
                        escaped_query = raw_query.replace("[", "\\[")[:100]
                        self.app.notify(
                            f"[$b_highlight]{self.host}:{self.port}[/$b_highlight]: [dim]{error_code}: "
                            f"{escaped_error_message}[/dim]\nQuery: [$b_light_blue]{escaped_query}...[/$b_light_blue]\n"
                            "Stats for this feature won't be available.",
                            title="Insufficient Privileges",
                            severity="warning",
                            timeout=9,
                        )
                    return None

                if ignore_error:
                    return None

                if isinstance(e, (psycopg.OperationalError,)):
                    logger.error(f"PostgreSQL connection lost: {error_message}, attempting to reconnect...")
                    escaped_error_message = error_message.replace("[", "\\[")
                    self.app.notify(
                        f"[$b_light_blue]{self.host}:{self.port}[/$b_light_blue]: {escaped_error_message}",
                        title="PostgreSQL Connection Lost",
                        severity="error",
                        timeout=10,
                    )

                    self.close()
                    self.connect(reconnect_attempt=True)

                    if not self.is_connected():
                        time.sleep(min(1 * (2**attempt_number), 20))
                        continue

                    self.app.notify(
                        f"[$b_light_blue]{self.host}:{self.port}[/$b_light_blue]: Successfully reconnected",
                        title="PostgreSQL Connection Created",
                        severity="success",
                        timeout=10,
                    )

                    return self.execute(query, values)
                else:
                    raise ManualException(error_message, query=query, code=error_code) from e

        if not self.is_connected():
            raise ManualException(
                f"Failed to reconnect to PostgreSQL after {self.max_reconnect_attempts} attempts",
                query=query,
            )

    def terminate_backend(self, pid: int) -> bool:
        """Terminate a PostgreSQL backend process (kill query)."""
        try:
            self.execute("SELECT pg_terminate_backend(%s)", (pid,))
            result = self.fetchone()
            return result.get("pg_terminate_backend", False)
        except psycopg.Error as e:
            logger.error(f"Failed to terminate backend {pid}: {e}")
            return False

    def cancel_backend(self, pid: int) -> bool:
        """Cancel a running query without terminating the connection."""
        try:
            self.execute("SELECT pg_cancel_backend(%s)", (pid,))
            result = self.fetchone()
            return result.get("pg_cancel_backend", False)
        except psycopg.Error as e:
            logger.error(f"Failed to cancel backend {pid}: {e}")
            return False

    def check_permissions(self) -> dict:
        """Check current user permissions for monitoring."""
        self.execute(PostgresQueries.permission_check)
        result = self.fetchone()
        return result

    def detect_environment(self) -> dict:
        """Detect if running on RDS/Aurora or self-hosted."""
        self.execute(PostgresQueries.environment_detection)
        result = self.fetchone()
        return result

    def check_extension(self, extension_name: str) -> bool:
        """Check if a PostgreSQL extension is installed."""
        self.execute("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = %s) AS installed", (extension_name,))
        result = self.fetchone()
        return result.get("installed", False)
