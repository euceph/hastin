from __future__ import annotations

import ipaddress
import os
import socket
from datetime import datetime

import psycopg
from loguru import logger
from packaging.version import InvalidVersion
from packaging.version import parse as parse_version
from rich.text import Text
from textual.app import App
from textual.widgets import Switch

import hastin.DataTypes as DataTypes
import hastin.Modules.MetricManager as MetricManager
from hastin.DataTypes import ConnectionSource
from hastin.Modules.ArgumentParser import Config
from hastin.Modules.Functions import load_host_cache_file
from hastin.Modules.PostgreSQL import Database
from hastin.Modules.SSHTunnel import SSHTunnelManager
from hastin.Modules.SystemMetricsProviders import (
    SystemMetricsProvider,
    get_system_metrics_provider,
)


class Hastin:
    """Core state class for Hastin PostgreSQL monitoring dashboard."""

    def __init__(self, config: Config, app: App) -> None:
        self.config = config
        self.app = app
        self.app_version = config.app_version

        self.tab_id: str = None

        # Config options
        self.user = config.user
        self.password = config.password
        self.host = config.host
        self.port = config.port
        self.database = getattr(config, "database", "postgres")
        self.ssl_mode = getattr(config, "ssl_mode", "prefer")
        self.host_cache_file = config.host_cache_file
        self.tab_setup_file = config.tab_setup_file
        self.refresh_interval = config.refresh_interval
        self.tab_setup_available_hosts = config.tab_setup_available_hosts
        self.startup_panels = config.startup_panels
        self.graph_marker = config.graph_marker
        self.record_for_replay = config.record_for_replay
        self.daemon_mode = config.daemon_mode
        self.daemon_mode_panels = config.daemon_mode_panels
        self.replay_file = config.replay_file
        self.replay_dir = config.replay_dir
        self.replay_retention_hours = config.replay_retention_hours
        self.exclude_notify_global_vars = config.exclude_notify_global_vars
        self.credential_profile = getattr(config, "credential_profile", None)
        self.ssl = getattr(config, "ssl", {})
        self.socket = getattr(config, "socket", None)
        self.hostgroup_hosts = getattr(config, "hostgroup_hosts", {})

        # PgBouncer options
        self.pgbouncer_mode = getattr(config, "pgbouncer_mode", False)
        self.pgbouncer_host = getattr(config, "pgbouncer_host", None)
        self.pgbouncer_port = getattr(config, "pgbouncer_port", 6432)
        self.pgbouncer_user = getattr(config, "pgbouncer_user", None)
        self.pgbouncer_password = getattr(config, "pgbouncer_password", None)

        # SSH tunnel options
        self.ssh_host = getattr(config, "ssh", None)
        self.ssh_user = getattr(config, "ssh_user", None)
        self.ssh_port = getattr(config, "ssh_port", 22)
        self.ssh_key = getattr(config, "ssh_key", None)
        self.ssh_tunnel_manager: SSHTunnelManager | None = None
        self.ssh_tunnel_active: bool = False

        self.panels = DataTypes.Panels()

        # For PgBouncer standalone mode, use PgBouncer panels
        if self.pgbouncer_mode:
            pgbouncer_panels = ["pgbouncer_dashboard", "pgbouncer_pools", "graphs"]
            for panel in pgbouncer_panels:
                if panel in self.panels.all():
                    getattr(self.panels, panel).visible = True
        else:
            panels_to_show = self.daemon_mode_panels if self.daemon_mode else self.startup_panels
            for panel in panels_to_show:
                if panel in self.panels.all():
                    getattr(self.panels, panel).visible = True

        self.show_idle_threads: bool = False
        self.sort_by_time_descending: bool = True

        self.reset_runtime_variables()

    def reset_runtime_variables(self):
        """Reset all runtime variables to initial state."""
        self.metric_manager = MetricManager.MetricManager(self.replay_file, self.daemon_mode)
        self.replica_manager = DataTypes.ReplicaManager()

        self.hastin_start_time: datetime = datetime.now().astimezone()
        self.worker_previous_start_time: datetime = datetime.now().astimezone()
        self.worker_processing_time: float = 0
        self.polling_latency: float = 0
        self.connection_status: DataTypes.ConnectionStatus = None

        # PostgreSQL-specific state
        self.server_info: dict[str, str | int] = {}
        self.connection_stats: dict[str, int] = {}
        self.database_stats: dict[str, int | float] = {}
        self.global_variables: dict[str, int | str] = {}  # pg_settings
        self.bgwriter_stats: dict[str, int] = {}
        self.replication_status: dict[str, any] = {}
        self.replication_role: str = None  # 'primary', 'replica', 'standalone'
        self.logical_subscriptions: list[dict[str, any]] = []
        self.replication_slots: list[dict[str, any]] = []
        self.blocked_queries: list[dict[str, any]] = []
        self.statement_stats: list[dict[str, any]] = []
        self.locks_data: list[dict[str, any]] = []
        self.statements_data: list[dict[str, any]] = []
        self.system_utilization: dict[str, int | str] = {}
        self.host_cache: dict[str, str] = {}

        self.processlist_threads: dict[int, DataTypes.ProcesslistThread] = {}
        self.processlist_threads_snapshot: dict[int, DataTypes.ProcesslistThread] = {}

        # Filters that can be applied
        self.user_filter: str = None
        self.db_filter: str = None
        self.host_filter: str = None
        self.query_filter: str = None
        self.query_time_filter: int = None
        self.state_filter: str = None

        # Environment detection
        self.connection_source: ConnectionSource = (
            ConnectionSource.pgbouncer if self.pgbouncer_mode else ConnectionSource.postgresql
        )
        self.detected_cloud_provider: str | None = None  # Stores the detected provider name
        self.is_replica: bool = False

        # PgBouncer state (for standalone and combined modes)
        self.pgbouncer_connection: Database | None = None
        self.pgbouncer_stats: dict = {}
        self.pgbouncer_pools: list[dict] = []
        self.pgbouncer_clients: list[dict] = []
        self.pgbouncer_servers: list[dict] = []
        self.pgbouncer_version: str | None = None
        self.has_pgbouncer: bool = self.pgbouncer_mode or self.pgbouncer_host is not None

        # Permissions
        self.has_full_visibility: bool = False
        self.has_pg_stat_statements: bool = False
        self.statement_type_counts: dict = {}  # SELECT/INSERT/UPDATE/DELETE counts from pg_stat_statements

        # Main connection is used for Textual's worker thread so it can run asynchronous
        # PgBouncer doesn't support SSL for admin connections, so disable it
        ssl_mode = "disable" if self.pgbouncer_mode else self.ssl_mode
        db_connection_args = {
            "app": self.app,
            "host": self.host,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "port": self.port,
            "ssl_mode": ssl_mode,
            "auto_connect": False,
            "daemon_mode": self.daemon_mode,
            "pgbouncer_mode": self.pgbouncer_mode,
        }
        self.main_db_connection = Database(**db_connection_args)
        # Secondary connection is for ad-hoc commands that are not a part of the worker thread
        self.secondary_db_connection = Database(**db_connection_args, save_connection_id=False)

        # Misc variables
        self.host_distro: str = "PgBouncer" if self.pgbouncer_mode else "PostgreSQL"
        self.host_with_port: str = f"{self.host}:{self.port}"
        self.host_version: str = None
        self.pause_refresh: bool = False
        self.show_statistics_per_second: bool = False  # Toggle with Shift+S

        self.host_cache_from_file = load_host_cache_file(self.host_cache_file)

        self.update_switches_after_reset()

        # System metrics provider (set up after cloud detection in configure_postgresql)
        self.system_metrics_provider: SystemMetricsProvider | None = None
        self.enable_system_utilization = False

    def db_connect(self):
        """Connect to PostgreSQL or PgBouncer database."""
        # Set up SSH tunnels if configured
        if self.ssh_host:
            self._setup_ssh_tunnels()

        self.main_db_connection.connect()
        if not self.daemon_mode:
            self.secondary_db_connection.connect()

        if not self.pgbouncer_mode:
            self.connection_source = self.main_db_connection.source
        self.metric_manager.connection_source = self.connection_source

        # Set up PgBouncer connection for combined mode
        if self.pgbouncer_host and not self.pgbouncer_mode:
            self._setup_pgbouncer_connection()

        # Add host to tab setup file if it doesn't exist
        self.add_host_to_tab_setup_file()

    def _setup_ssh_tunnels(self):
        """Set up SSH tunnels for remote database connections."""
        try:
            self.ssh_tunnel_manager = SSHTunnelManager(
                ssh_host=self.ssh_host,
                ssh_user=self.ssh_user,
                ssh_port=self.ssh_port,
                ssh_key=self.ssh_key,
            )

            # Store original remote host/port for display purposes
            self._original_host = self.host
            self._original_port = self.port

            # Create tunnel for main PostgreSQL connection
            pg_tunnel = self.ssh_tunnel_manager.create_tunnel(
                name="postgres",
                remote_host=self.host,
                remote_port=self.port,
            )

            # Update connection parameters to use tunneled port
            tunneled_port = pg_tunnel.local_port
            self.main_db_connection.host = "127.0.0.1"
            self.main_db_connection.port = tunneled_port
            self.secondary_db_connection.host = "127.0.0.1"
            self.secondary_db_connection.port = tunneled_port

            logger.info(f"SSH tunnel established: localhost:{tunneled_port} -> {self.ssh_host}:{self._original_port}")

            # Create tunnel for PgBouncer if configured
            if self.pgbouncer_host and not self.pgbouncer_mode:
                pgb_tunnel = self.ssh_tunnel_manager.create_tunnel(
                    name="pgbouncer",
                    remote_host=self.pgbouncer_host,
                    remote_port=self.pgbouncer_port,
                )
                # Store tunneled PgBouncer port for _setup_pgbouncer_connection
                self._tunneled_pgbouncer_port = pgb_tunnel.local_port
                logger.info(
                    f"SSH tunnel established: localhost:{pgb_tunnel.local_port} -> "
                    f"{self.ssh_host}:{self.pgbouncer_port}"
                )

            self.ssh_tunnel_active = True

        except Exception as e:
            logger.error(f"Failed to establish SSH tunnel: {e}")
            if self.ssh_tunnel_manager:
                self.ssh_tunnel_manager.close_all()
                self.ssh_tunnel_manager = None
            raise

    def _setup_pgbouncer_connection(self):
        """Set up PgBouncer connection for combined mode."""
        try:
            # Use tunneled port/host if SSH tunnel is active
            pgb_host = self.pgbouncer_host
            pgb_port = self.pgbouncer_port
            if self.ssh_tunnel_active and hasattr(self, "_tunneled_pgbouncer_port"):
                pgb_host = "127.0.0.1"
                pgb_port = self._tunneled_pgbouncer_port

            pgbouncer_args = {
                "app": self.app,
                "host": pgb_host,
                "user": self.pgbouncer_user or self.user,
                "password": self.pgbouncer_password or self.password,
                "database": "pgbouncer",
                "port": pgb_port,
                "ssl_mode": "disable",  # PgBouncer admin typically doesn't use SSL
                "auto_connect": True,
                "daemon_mode": self.daemon_mode,
                "save_connection_id": False,
                "pgbouncer_mode": True,  # Enable PgBouncer-specific handling
            }
            self.pgbouncer_connection = Database(**pgbouncer_args)
            self.has_pgbouncer = True
        except psycopg.Error as e:
            logger.warning(f"Failed to connect to PgBouncer: {e}")
            self.pgbouncer_connection = None
            self.has_pgbouncer = False

    def configure_postgresql(self):
        """Configure PostgreSQL-specific settings after connection."""
        if not self.is_pg_version_at_least("14"):
            from hastin.Modules.ManualException import ManualException

            raise ManualException(
                f"PostgreSQL 14+ required. Server version: {self.host_version or 'unknown'}"
            )

        # Detect cloud environment
        self._detect_cloud_provider()

        # Set up system metrics provider (after cloud detection)
        self._setup_system_metrics_provider()

        self.host_with_port = self._get_display_hostname()

        permissions = self.main_db_connection.check_permissions()
        self.has_full_visibility = (
            permissions.get("has_read_all_stats", False)
            or permissions.get("has_pg_monitor", False)
            or permissions.get("is_superuser", False)
        )

        if not self.has_full_visibility:
            logger.warning(
                "Limited visibility: only showing your own sessions. Grant pg_read_all_stats role for full processlist."
            )
            self.app.notify(
                "Limited visibility: only your own sessions are shown.\n"
                "Grant pg_read_all_stats role for full processlist.",
                title="Limited Permissions",
                severity="warning",
                timeout=10,
            )

        self.has_pg_stat_statements = self.main_db_connection.check_extension("pg_stat_statements")
        if not self.has_pg_stat_statements:
            logger.info("pg_stat_statements extension not installed - Query Stats panel will be unavailable")

    def _detect_cloud_provider(self):
        """Detect cloud PostgreSQL provider from settings, extensions, or hostname."""
        env_info = self.main_db_connection.detect_environment()

        # Priority order: most specific detection first
        # AWS
        if env_info.get("is_aurora"):
            self.connection_source = ConnectionSource.aurora
            self.host_distro = "Amazon Aurora PostgreSQL"
            self.detected_cloud_provider = "aurora"
        elif env_info.get("is_rds"):
            self.connection_source = ConnectionSource.rds
            self.host_distro = "Amazon RDS PostgreSQL"
            self.detected_cloud_provider = "rds"
        # Google Cloud
        elif env_info.get("is_alloydb"):
            self.connection_source = ConnectionSource.alloydb
            self.host_distro = "Google AlloyDB"
            self.detected_cloud_provider = "alloydb"
        elif env_info.get("is_cloud_sql"):
            self.connection_source = ConnectionSource.cloud_sql
            self.host_distro = "Google Cloud SQL"
            self.detected_cloud_provider = "cloud_sql"
        # Azure
        elif env_info.get("is_citus"):
            self.connection_source = ConnectionSource.cosmos_citus
            self.host_distro = "Azure Cosmos DB (Citus)"
            self.detected_cloud_provider = "cosmos_citus"
        elif env_info.get("is_azure"):
            self.connection_source = ConnectionSource.azure
            self.host_distro = "Azure PostgreSQL"
            self.detected_cloud_provider = "azure"
        # Other providers (detected via extensions/settings)
        elif env_info.get("is_supabase"):
            self.connection_source = ConnectionSource.supabase
            self.host_distro = "Supabase"
            self.detected_cloud_provider = "supabase"
        elif env_info.get("is_neon"):
            self.connection_source = ConnectionSource.neon
            self.host_distro = "Neon"
            self.detected_cloud_provider = "neon"
        elif env_info.get("is_crunchy"):
            self.connection_source = ConnectionSource.crunchy_bridge
            self.host_distro = "Crunchy Bridge"
            self.detected_cloud_provider = "crunchy_bridge"
        elif env_info.get("has_timescaledb"):
            # Timescale could be self-hosted, but if detected with cloud hostname, it's Timescale Cloud
            if ".timescaledb.io" in self.host or ".tsdb.cloud.timescale.com" in self.host:
                self.connection_source = ConnectionSource.timescale
                self.host_distro = "Timescale Cloud"
                self.detected_cloud_provider = "timescale"
        else:
            # Fallback to hostname-based detection
            self._detect_provider_from_hostname()

    def _detect_provider_from_hostname(self):
        """Detect cloud provider from hostname patterns."""
        host_lower = self.host.lower()

        # Hostname patterns for various providers
        hostname_patterns = [
            # AWS (fallback if settings detection missed)
            (".rds.amazonaws.com", ConnectionSource.rds, "Amazon RDS PostgreSQL", "rds"),
            (".cluster-", ConnectionSource.aurora, "Amazon Aurora PostgreSQL", "aurora"),  # Aurora clusters
            # Google Cloud
            (".sql.goog", ConnectionSource.cloud_sql, "Google Cloud SQL", "cloud_sql"),
            # Azure
            (".postgres.database.azure.com", ConnectionSource.azure, "Azure PostgreSQL", "azure"),
            (".postgres.cosmos.azure.com", ConnectionSource.cosmos_citus, "Azure Cosmos DB", "cosmos_citus"),
            # Aiven
            (".aivencloud.com", ConnectionSource.aiven, "Aiven PostgreSQL", "aiven"),
            (".aiven.io", ConnectionSource.aiven, "Aiven PostgreSQL", "aiven"),
            # DigitalOcean
            (".db.ondigitalocean.com", ConnectionSource.digitalocean, "DigitalOcean PostgreSQL", "digitalocean"),
            # Supabase
            (".supabase.co", ConnectionSource.supabase, "Supabase", "supabase"),
            (".supabase.com", ConnectionSource.supabase, "Supabase", "supabase"),
            # Neon
            (".neon.tech", ConnectionSource.neon, "Neon", "neon"),
            # Railway
            (".railway.app", ConnectionSource.railway, "Railway PostgreSQL", "railway"),
            # Render
            (".render.com", ConnectionSource.render, "Render PostgreSQL", "render"),
            # Fly.io
            (".fly.dev", ConnectionSource.fly, "Fly.io PostgreSQL", "fly"),
            (".internal", ConnectionSource.fly, "Fly.io PostgreSQL", "fly"),  # Fly internal network
            # Heroku (uses AWS under the hood)
            (".herokuapp.com", ConnectionSource.heroku, "Heroku PostgreSQL", "heroku"),
            # Crunchy Bridge
            (".db.postgresbridge.com", ConnectionSource.crunchy_bridge, "Crunchy Bridge", "crunchy_bridge"),
            # Timescale Cloud
            (".timescaledb.io", ConnectionSource.timescale, "Timescale Cloud", "timescale"),
            (".tsdb.cloud.timescale.com", ConnectionSource.timescale, "Timescale Cloud", "timescale"),
        ]

        for pattern, source, distro, provider in hostname_patterns:
            if pattern in host_lower:
                self.connection_source = source
                self.host_distro = distro
                self.detected_cloud_provider = provider
                return

        # No cloud provider detected - keep as generic PostgreSQL
        self.host_distro = "PostgreSQL"
        self.detected_cloud_provider = None

    def _get_display_hostname(self) -> str:
        """Get a shortened hostname for display, removing long cloud suffixes."""
        # Patterns to shorten for display
        shorten_patterns = [
            ".rds.amazonaws.com",
            ".cluster-ro-",  # Aurora read replica
            ".cluster-",  # Aurora cluster
            ".postgres.database.azure.com",
            ".postgres.cosmos.azure.com",
            ".aivencloud.com",
            ".aiven.io",
            ".db.ondigitalocean.com",
            ".supabase.co",
            ".supabase.com",
            ".neon.tech",
            ".railway.app",
            ".render.com",
            ".fly.dev",
            ".db.postgresbridge.com",
            ".timescaledb.io",
            ".tsdb.cloud.timescale.com",
            ".sql.goog",
        ]

        display_host = self.host
        for pattern in shorten_patterns:
            if pattern in display_host:
                display_host = display_host.split(pattern)[0]
                break

        # If using SSH tunnel, show SSH host in display
        if self.ssh_tunnel_active:
            return f"{self.ssh_host}:{self._original_port}"

        return f"{display_host}:{self.port}"

    def _setup_system_metrics_provider(self):
        """Initialize appropriate system metrics provider based on detection/configuration."""
        self.system_metrics_provider = get_system_metrics_provider(
            detected_cloud_provider=self.detected_cloud_provider,
            db_connection=self.main_db_connection,
            host=self.host,
            manual_override=getattr(self.config, "system_metrics", None),
            ssh_tunnel_active=self.ssh_tunnel_active,
            aws_region=getattr(self.config, "aws_region", None),
            aws_db_identifier=getattr(self.config, "aws_db_identifier", None),
            gcp_project=getattr(self.config, "gcp_project", None),
            gcp_instance=getattr(self.config, "gcp_instance", None),
            azure_subscription=getattr(self.config, "azure_subscription", None),
            azure_resource_group=getattr(self.config, "azure_resource_group", None),
            azure_server_name=getattr(self.config, "azure_server_name", None),
        )
        self.enable_system_utilization = self.system_metrics_provider.name != "none"
        if self.enable_system_utilization:
            logger.info(f"System metrics provider: {self.system_metrics_provider.name}")

    def detect_replication_role(self):
        """Detect if this instance is a primary, replica, or standalone."""
        from hastin.Modules.Queries import PostgresQueries

        self.main_db_connection.execute(PostgresQueries.server_info)
        info = self.main_db_connection.fetchone()
        self.is_replica = info.get("is_replica", False)

        if self.is_replica:
            self.replication_role = "replica"
        else:
            self.main_db_connection.execute(PostgresQueries.replication_status_primary)
            replicas = self.main_db_connection.fetchall()

            if replicas:
                self.replication_role = "primary"
            else:
                self.replication_role = "standalone"

        return self.replication_role

    def terminate_backend(self, pid: int) -> bool:
        """Terminate a PostgreSQL backend (kill query/connection)."""
        return self.secondary_db_connection.terminate_backend(pid)

    def cancel_backend(self, pid: int) -> bool:
        """Cancel a running query without terminating the connection."""
        return self.secondary_db_connection.cancel_backend(pid)

    def collect_system_utilization(self):
        """Collect system utilization metrics using the configured provider."""
        if not self.enable_system_utilization or not self.system_metrics_provider:
            return

        metrics = self.system_metrics_provider.collect()
        if not metrics:
            return

        self.system_utilization = {
            "Uptime": metrics.uptime_seconds,
            "CPU_Count": metrics.cpu_count,
            "CPU_Percent": metrics.cpu_percent,
            "Memory_Total": metrics.memory_total,
            "Memory_Used": metrics.memory_used,
            "Swap_Total": metrics.swap_total,
            "Swap_Used": metrics.swap_used,
            "Network_Up": metrics.network_bytes_sent,
            "Network_Down": metrics.network_bytes_recv,
            "Disk_Read": metrics.disk_read_iops,
            "Disk_Write": metrics.disk_write_iops,
        }
        if metrics.cpu_load_avg:
            self.system_utilization["CPU_Load_Avg"] = metrics.cpu_load_avg

    def add_host_to_tab_setup_file(self):
        """Add the current host to the tab setup file."""
        if self.daemon_mode:
            return

        with open(self.tab_setup_file, "a+") as file:
            file.seek(0)
            lines = file.readlines()

            host = f"{self.host}:{self.port}\n" if self.port != 5432 else f"{self.host}\n"

            if host not in lines:
                file.write(host)
                self.tab_setup_available_hosts.append(host[:-1])

    def is_pg_version_at_least(self, target: str, use_version: str = None) -> bool:
        """Check if PostgreSQL version is at least the target version."""
        version = self.host_version
        if use_version:
            version = use_version

        if not version or version == "N/A":
            return False

        try:
            parsed_source = parse_version(version)
            parsed_target = parse_version(target)
            return parsed_source >= parsed_target
        except InvalidVersion:
            # If version parsing fails, assume it's not compatible
            return False

    def parse_server_version(self, version: str) -> str:
        """Parse and format the server version string.

        PostgreSQL version strings can be:
        - "16.11" (simple)
        - "16.11 (Debian 16.11-1.pgdg13+1)" (with build info)
        - "14.5.1" (three-part)
        """
        if not version:
            return "N/A"

        # Extract just the version number (before any space or parenthesis)
        version_part = version.split()[0] if " " in version else version
        version_part = version_part.split("(")[0].strip()

        # Validate it looks like a version number
        parts = version_part.split(".")
        if len(parts) >= 2 and parts[0].isdigit():
            return ".".join(parts[:3])

        return version_part if version_part else "N/A"

    def get_hostname(self, host):
        """Resolve hostname from IP address using cache."""
        if host in self.host_cache:
            return self.host_cache[host]

        if self.host_cache_from_file and host in self.host_cache_from_file:
            self.host_cache[host] = self.host_cache_from_file[host]
            return self.host_cache_from_file[host]

        try:
            ipaddress.IPv4Network(host)
            hostname = socket.gethostbyaddr(host)[0]
            self.host_cache[host] = hostname
        except (OSError, ValueError):
            self.host_cache[host] = host
            hostname = host

        return hostname

    def update_switches_after_reset(self):
        """Set the graph switches to their current state after a reset."""
        switches = self.app.query(f".switch_container_{self.tab_id} Switch")
        for switch in switches:
            switch: Switch
            metric_instance_name = switch.name
            metric = switch.id

            metric_instance = getattr(self.metric_manager.metrics, metric_instance_name)
            metric_data: MetricManager.MetricData = getattr(metric_instance, metric)
            metric_data.visible = switch.value

    def get_replay_files(self):
        """Get a list of replay files in the replay directory.

        Returns:
            list: A list of tuples in the format (full_path, formatted host name + replay name).
        """
        if not self.replay_dir or not os.path.exists(self.replay_dir):
            return []

        replay_files = []
        try:
            with os.scandir(self.replay_dir) as entries:
                for entry in entries:
                    if entry.is_dir():
                        entry_path = entry.path
                        for file in os.scandir(entry_path):
                            if file.is_file():
                                host_name = entry.name[:30]

                                port = ""
                                if len(entry.name) >= 30 and "_" in entry.name:
                                    port = "_" + entry.name.rsplit("_", 1)[-1]

                                formatted_replay_name = f"[label]{host_name}{port}[/label]"
                                formatted_replay_name += f": [b light_blue]{file.name}[/b light_blue]"

                                replay_files.append((file.path, Text.from_markup(formatted_replay_name)))
        except OSError as e:
            self.app.notify(str(e), title="Error getting replay files", severity="error")

        replay_files.sort(key=lambda x: x[0])

        return replay_files

    def format_bytes(self, bytes_value: int) -> str:
        """Format bytes to human-readable string."""
        if bytes_value is None:
            return "N/A"

        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if abs(bytes_value) < 1024:
                return f"{bytes_value:.1f} {unit}"
            bytes_value /= 1024

        return f"{bytes_value:.1f} PB"
