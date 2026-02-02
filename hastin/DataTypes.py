from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

from rich.table import Table

from hastin.Modules.Functions import format_query, format_time


@dataclass
class ConnectionSource:
    """PostgreSQL connection source types."""

    # Self-hosted / generic
    postgresql = "PostgreSQL"
    pgbouncer = "PgBouncer"

    # AWS
    rds = "Amazon RDS"
    aurora = "Amazon Aurora"

    # Google Cloud
    cloud_sql = "Google Cloud SQL"
    alloydb = "Google AlloyDB"

    # Microsoft Azure
    azure = "Azure PostgreSQL"
    cosmos_citus = "Azure Cosmos DB"

    # Other cloud providers
    supabase = "Supabase"
    neon = "Neon"
    aiven = "Aiven"
    crunchy_bridge = "Crunchy Bridge"
    digitalocean = "DigitalOcean"
    heroku = "Heroku"
    timescale = "Timescale Cloud"
    railway = "Railway"
    render = "Render"
    fly = "Fly.io"


@dataclass
class ConnectionStatus:
    connecting = "CONNECTING"
    connected = "CONNECTED"
    disconnected = "DISCONNECTED"
    read_write = "R/W"
    read_only = "RO"
    primary = "PRIMARY"
    replica = "REPLICA"


@dataclass
class Replica:
    """Represents a streaming replication replica."""

    row_key: str
    pid: int
    host: str
    port: int = None
    application_name: str = None
    state: str = None
    table: Table = None
    replication_status: dict[str, str | int] = field(default_factory=dict)
    sync_state: str = None
    replication_lag_bytes: int = 0


class ReplicaManager:
    """Manages replica connections for a PostgreSQL primary."""

    def __init__(self):
        self.available_replicas: list[dict[str, str]] = []
        self.replicas: dict[str, Replica] = {}

    def create_replica_row_key(self, host: str, port: int) -> str:
        input_string = f"{host}:{port}"
        return hashlib.sha256(input_string.encode()).hexdigest()

    def add_replica(self, row_key: str, pid: int, host: str, port: int = None) -> Replica:
        self.replicas[row_key] = Replica(row_key=row_key, pid=pid, host=host, port=port)
        return self.replicas[row_key]

    def remove_replica(self, row_key: str):
        del self.replicas[row_key]

    def get_replica(self, row_key: str) -> Replica:
        return self.replicas.get(row_key)

    def remove_all_replicas(self):
        self.replicas = {}

    def get_sorted_replicas(self) -> list[Replica]:
        return sorted(self.replicas.values(), key=lambda x: x.host)


@dataclass
class Panel:
    name: str
    display_name: str
    key: str = None
    visible: bool = False
    daemon_supported: bool = True


class Panels:
    """Panel definitions for Hastin PostgreSQL dashboard."""

    def __init__(self):
        self.dashboard = Panel("dashboard", "Dashboard", "¹")
        self.processlist = Panel("processlist", "Processlist", "²")
        self.graphs = Panel("graphs", "Metric Graphs", "³", visible=True)
        self.replication = Panel("replication", "Replication", "⁴")
        self.locks = Panel("locks", "Locks", "⁵")
        self.statements = Panel("statements", "Query Stats", "⁶")
        self.pgbouncer = Panel("pgbouncer", "PgBouncer", "⁷")
        # PgBouncer panels (used in standalone mode)
        self.pgbouncer_dashboard = Panel("pgbouncer_dashboard", "PgBouncer", "¹")
        self.pgbouncer_pools = Panel("pgbouncer_pools", "Pools", "²")
        self.pgbouncer_clients = Panel("pgbouncer_clients", "Clients", "³")
        self.pgbouncer_servers = Panel("pgbouncer_servers", "Servers", "⁴")

    def validate_panels(self, panel_list_str: str | list[str], valid_panel_names: list[str]) -> list[str]:
        panels = panel_list_str.split(",") if isinstance(panel_list_str, str) else panel_list_str

        invalid_panels = [panel for panel in panels if panel not in valid_panel_names]
        if invalid_panels:
            raise ValueError(
                f"Panel(s) [red2]{', '.join(invalid_panels)}[/red2] are not valid (see --help for more information)"
            )

        return panels

    def get_panel(self, panel_name: str) -> Panel:
        return self.__dict__.get(panel_name, None)

    def get_all_daemon_panel_names(self) -> list[str]:
        return [panel.name for panel in self.__dict__.values() if isinstance(panel, Panel) and panel.daemon_supported]

    def get_all_panels(self) -> list[Panel]:
        return [panel for panel in self.__dict__.values() if isinstance(panel, Panel)]

    def get_key(self, panel_name: str) -> str:
        return f"[b highlight]{self.get_panel(panel_name).key}[/b highlight]"

    def get_panel_title(self, panel_name: str) -> str:
        panel = self.get_panel(panel_name)
        return f"[$b_highlight]{panel.key}[/$b_highlight]{panel.display_name}"

    def all(self) -> list[str]:
        return [
            panel.name
            for name, panel in self.__dict__.items()
            if not name.startswith("__") and isinstance(panel, Panel)
        ]


class ProcesslistThread:
    """Represents a PostgreSQL backend process from pg_stat_activity."""

    def __init__(self, thread_data: dict[str, str]):
        self.thread_data = thread_data

        self.id = str(thread_data.get("pid", ""))
        self.pid = thread_data.get("pid")
        self.user = thread_data.get("user", "")
        self.host = thread_data.get("host", "")
        self.db = thread_data.get("database", "")
        self.application = thread_data.get("application", "")
        self.time = int(thread_data.get("time") or 0)
        self.state = thread_data.get("state", "")
        self.wait_event_type = thread_data.get("wait_event_type", "")
        self.wait_event = thread_data.get("wait_event", "")
        self.formatted_query = self._get_formatted_query(thread_data.get("query", ""))
        self.formatted_time = self._get_formatted_time()
        self.formatted_state = self._get_formatted_state()
        self.formatted_wait = self._get_formatted_wait()

    def _get_formatted_time(self) -> str:
        thread_color = self._get_time_color()
        return f"[{thread_color}]{format_time(self.time)}[/{thread_color}]" if thread_color else format_time(self.time)

    def _get_time_color(self) -> str:
        thread_color = ""
        if self.formatted_query.code and self.state == "active":
            if self.time >= 10:
                thread_color = "red"
            elif self.time >= 5:
                thread_color = "yellow"
            else:
                thread_color = "green"
        return thread_color

    def _get_formatted_state(self) -> str:
        state_formats = {
            "active": "[green]active[/green]",
            "idle": "[dark_gray]idle[/dark_gray]",
            "idle in transaction": "[yellow]idle in txn[/yellow]",
            "idle in transaction (aborted)": "[red]idle in txn (aborted)[/red]",
        }
        return state_formats.get(self.state, self.state or "[dark_gray]N/A[/dark_gray]")

    def _get_formatted_wait(self) -> str:
        if not self.wait_event_type:
            return "[dark_gray]N/A[/dark_gray]"

        wait_type = self.wait_event_type
        wait_event = self.wait_event or ""

        # Color coding based on wait type
        if wait_type == "Lock":
            return f"[red]{wait_type}:{wait_event}[/red]"
        elif wait_type in ("LWLock", "BufferPin"):
            return f"[yellow]{wait_type}:{wait_event}[/yellow]"
        elif wait_type == "IO":
            return f"[cyan]{wait_type}:{wait_event}[/cyan]"
        elif wait_type == "Client":
            return f"[dark_gray]{wait_type}:{wait_event}[/dark_gray]"

        return f"{wait_type}:{wait_event}"

    def _get_formatted_query(self, query: str):
        return format_query(query)

    def _get_formatted_string(self, string: str):
        if not string:
            return "[dark_gray]N/A"
        return string

    def _get_formatted_number(self, number):
        if not number or number == "0":
            return "[dark_gray]0"
        return number


class HotkeyCommands:
    show_thread = "show_thread"
    thread_filter = "thread_filter"
    thread_kill_by_parameter = "thread_kill_by_parameter"
    variable_search = "variable_search"
    rename_tab = "rename_tab"
    refresh_interval = "refresh_interval"
    replay_seek = "replay_seek"
    maximize_panel = "maximize_panel"
