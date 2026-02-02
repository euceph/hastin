import argparse
import json
import os
import sys
from configparser import RawConfigParser
from dataclasses import dataclass, field, fields
from urllib.parse import urlparse

from rich import box
from rich.console import Console
from rich.table import Table
from rich.theme import Theme

from hastin.DataTypes import Panels


@dataclass
class CredentialProfile:
    name: str
    host: str = None
    port: int = None
    user: str = None
    password: str = None
    database: str = None
    ssl_mode: str = None


@dataclass
class HostGroupMember:
    tab_title: str
    host: str
    port: int = None
    credential_profile: CredentialProfile = None


@dataclass
class Config:
    app_version: str
    tab_setup: bool = False
    credential_profile: str = None
    user: str = None
    password: str = None
    host: str = "localhost"
    port: int = 5432
    database: str = "postgres"
    ssl_mode: str = "prefer"
    # PgBouncer options (for combined mode)
    pgbouncer_host: str = None
    pgbouncer_port: int = 6432
    pgbouncer_user: str = None
    pgbouncer_password: str = None
    # PgBouncer standalone mode flag (set when pgbouncer:// URI used)
    pgbouncer_mode: bool = False
    # System metrics provider options
    system_metrics: str = None  # Provider override: aws, gcp, azure, extension, local, none
    aws_region: str = None
    aws_db_identifier: str = None
    gcp_project: str = None
    gcp_instance: str = None
    azure_subscription: str = None
    azure_resource_group: str = None
    azure_server_name: str = None
    # SSH tunnel options
    ssh: str = None  # SSH host (can be alias from ~/.ssh/config)
    ssh_user: str = None
    ssh_port: int = 22
    ssh_key: str = None
    config_file: list[str] = field(
        default_factory=lambda: [
            "/etc/hastin.conf",
            "/etc/hastin/hastin.conf",
            f"{os.path.expanduser('~')}/.hastin.conf",
        ]
    )
    host_cache_file: str = field(default_factory=lambda: f"{os.path.expanduser('~')}/hastin_host_cache")
    tab_setup_file: str = field(default_factory=lambda: f"{os.path.expanduser('~')}/hastin_hosts")
    refresh_interval: int = 1
    credential_profiles: dict[str, CredentialProfile] = field(default_factory=dict)
    tab_setup_available_hosts: list[str] = field(default_factory=list)
    startup_panels: list[str] = field(default_factory=lambda: ["dashboard", "processlist", "graphs"])
    graph_marker: str = "braille"
    pypi_repository: str = "https://pypi.org/pypi/hastin/json"
    hostgroup: str = None
    hostgroup_hosts: dict[str, list[HostGroupMember]] = field(default_factory=dict)
    record_for_replay: bool = False
    daemon_mode: bool = False
    daemon_mode_panels: list[str] = field(default_factory=lambda: ["processlist", "locks"])
    daemon_mode_log_file: str = field(default_factory=lambda: f"{os.path.expanduser('~')}/hastin_daemon.log")
    replay_file: str = None
    replay_dir: str = None
    replay_retention_hours: int = 48
    exclude_notify_global_vars: str = None


class ArgumentParser:
    def __init__(self, app_version: str):
        self.config_object_options = {}

        excluded_options = ["app_version", "tab_setup_available_hosts", "hostgroup_hosts", "credential_profiles"]
        for variable in fields(Config):
            if variable.name not in excluded_options:
                self.config_object_options[variable.name] = variable.type

        self.formatted_options = "\n\t".join(
            [
                (
                    f"(comma-separated str) {option}"
                    if option in ("daemon_mode_panels", "startup_panels", "exclude_notify_global_vars")
                    else f"({data_type.__name__}) {option}"
                    if hasattr(data_type, "__name__")
                    else f"(str) {option} []"
                )
                for option, data_type in self.config_object_options.items()
                if option != "config_file"
            ]
        )

        epilog = f"""
Order of precedence for methods that pass options to Hastin:
\t1. Command-line
\t2. Credential profile (set by --cred-profile)
\t3. Environment variables
\t4. Hastin's config (set by --config-file)

Credential profiles can be defined in Hastin's config file:
\thost, port (default 5432), user, password, database, ssl_mode

Environment variables:
\tHASTIN_USER, HASTIN_PASSWORD, HASTIN_HOST, HASTIN_PORT, HASTIN_DATABASE, HASTIN_SSL_MODE

Hastin's config supports these options under [hastin] section:
\t{self.formatted_options}
"""
        self.parser = argparse.ArgumentParser(
            conflict_handler="resolve",
            description="Hastin - PostgreSQL TUI monitoring dashboard",
            epilog=epilog,
            formatter_class=argparse.RawTextHelpFormatter,
        )
        self.config = Config(app_version)
        self.panels = Panels()

        self.console = Console(style="#e9e9e9", highlight=False)
        self.console.push_theme(Theme({"red2": "b #fb9a9a"}))

        self._add_options()
        self._parse()

    def _add_options(self):
        self.parser.add_argument(
            "uri",
            metavar="uri",
            type=str,
            nargs="?",
            help=f"URI string: postgresql://user:password@host:port/database (port default: {self.config.port})",
        )
        self.parser.add_argument(
            "--tab-setup", dest="tab_setup", action="store_true", help="Start with Tab Setup modal"
        )
        self.parser.add_argument(
            "-C", "--cred-profile", dest="credential_profile", type=str, metavar="", help="Credential profile to use"
        )
        self.parser.add_argument("-u", "--user", dest="user", type=str, metavar="", help="Username")
        self.parser.add_argument("-p", "--password", dest="password", type=str, metavar="", help="Password")
        self.parser.add_argument("-h", "--host", dest="host", type=str, metavar="", help="Hostname/IP")
        self.parser.add_argument(
            "-P", "--port", dest="port", type=int, metavar="", help=f"Port [default: {self.config.port}]"
        )
        self.parser.add_argument(
            "-d",
            "--database",
            dest="database",
            type=str,
            metavar="",
            help=f"Database [default: {self.config.database}]",
        )
        self.parser.add_argument(
            "-c",
            "--config-file",
            dest="config_file",
            type=str,
            metavar="",
            help=f"Config file [default: {self.config.config_file}]",
        )
        self.parser.add_argument(
            "-r",
            "--refresh-interval",
            dest="refresh_interval",
            type=int,
            metavar="",
            help=f"Refresh interval in seconds [default: {self.config.refresh_interval}]",
        )
        self.parser.add_argument(
            "--ssl-mode",
            dest="ssl_mode",
            type=str,
            metavar="",
            help=f"PostgreSQL sslmode [default: {self.config.ssl_mode}]",
        )
        self.parser.add_argument(
            "--panels",
            dest="startup_panels",
            type=str,
            metavar="",
            help=f"Startup panels [default: {self.config.startup_panels}]",
        )
        self.parser.add_argument(
            "-H", "--hostgroup", dest="hostgroup", type=str, metavar="", help="Hostgroup from config file"
        )
        self.parser.add_argument(
            "-R", "--record", dest="record_for_replay", action="store_true", help="Enable recording for replay"
        )
        self.parser.add_argument("-D", "--daemon", dest="daemon_mode", action="store_true", help="Start in daemon mode")
        self.parser.add_argument("--replay-file", dest="replay_file", type=str, metavar="", help="Replay file to load")
        self.parser.add_argument(
            "--replay-dir", dest="replay_dir", type=str, metavar="", help="Directory for replay files"
        )
        self.parser.add_argument(
            "--debug-options", dest="debug_options", action="store_true", help="Display options and exit"
        )
        self.parser.add_argument(
            "-V", "--version", action="version", version=self.config.app_version, help="Display version"
        )
        # PgBouncer options (for combined mode: PostgreSQL + PgBouncer sidecar)
        self.parser.add_argument(
            "--pgbouncer-host",
            dest="pgbouncer_host",
            type=str,
            metavar="",
            help="PgBouncer admin host (enables combined mode)",
        )
        self.parser.add_argument(
            "--pgbouncer-port",
            dest="pgbouncer_port",
            type=int,
            metavar="",
            help=f"PgBouncer admin port [default: {self.config.pgbouncer_port}]",
        )
        self.parser.add_argument(
            "--pgbouncer-user",
            dest="pgbouncer_user",
            type=str,
            metavar="",
            help="PgBouncer admin user (defaults to --user)",
        )
        self.parser.add_argument(
            "--pgbouncer-password",
            dest="pgbouncer_password",
            type=str,
            metavar="",
            help="PgBouncer admin password (defaults to --password)",
        )
        # System metrics provider options
        self.parser.add_argument(
            "--system-metrics",
            dest="system_metrics",
            type=str,
            choices=["aws", "gcp", "azure", "extension", "local", "none"],
            metavar="",
            help="System metrics provider [default: auto-detect]",
        )
        # AWS-specific options
        self.parser.add_argument(
            "--aws-region",
            dest="aws_region",
            type=str,
            metavar="",
            help="AWS region for CloudWatch metrics",
        )
        self.parser.add_argument(
            "--aws-db-identifier",
            dest="aws_db_identifier",
            type=str,
            metavar="",
            help="RDS/Aurora DB instance identifier",
        )
        # GCP-specific options
        self.parser.add_argument(
            "--gcp-project",
            dest="gcp_project",
            type=str,
            metavar="",
            help="GCP project ID for Cloud Monitoring",
        )
        self.parser.add_argument(
            "--gcp-instance",
            dest="gcp_instance",
            type=str,
            metavar="",
            help="Cloud SQL/AlloyDB instance ID",
        )
        # Azure-specific options
        self.parser.add_argument(
            "--azure-subscription",
            dest="azure_subscription",
            type=str,
            metavar="",
            help="Azure subscription ID",
        )
        self.parser.add_argument(
            "--azure-resource-group",
            dest="azure_resource_group",
            type=str,
            metavar="",
            help="Azure resource group name",
        )
        self.parser.add_argument(
            "--azure-server-name",
            dest="azure_server_name",
            type=str,
            metavar="",
            help="Azure PostgreSQL server name",
        )
        # SSH tunnel options
        self.parser.add_argument(
            "--ssh",
            dest="ssh",
            type=str,
            metavar="",
            help="SSH host for tunneling (can be alias from ~/.ssh/config)",
        )
        self.parser.add_argument(
            "--ssh-user",
            dest="ssh_user",
            type=str,
            metavar="",
            help="SSH user (uses SSH config default if not specified)",
        )
        self.parser.add_argument(
            "--ssh-port",
            dest="ssh_port",
            type=int,
            metavar="",
            help="SSH port [default: 22]",
        )
        self.parser.add_argument(
            "--ssh-key",
            dest="ssh_key",
            type=str,
            metavar="",
            help="Path to SSH private key",
        )

    def set_config_value(self, source, option, value):
        setattr(self.config, option, value)
        self.add_to_debug_options(source, option, value)

    def add_to_debug_options(self, source, option, value):
        if self.debug_options:
            self.debug_options_table.add_row(source, option, str(value))

    def _parse(self):
        login_options = ["user", "password", "host", "port", "database", "ssl_mode"]
        options = vars(self.parser.parse_args())
        hastin_config_login_options_used = {}
        hostgroups = {}

        self.debug_options = options.get("debug_options", False)
        if self.debug_options:
            self.debug_options_table = Table(box=box.SIMPLE_HEAVY, header_style="b", style="#333f62")
            self.debug_options_table.add_column("Source")
            self.debug_options_table.add_column("Option", style="#91abec")
            self.debug_options_table.add_column("Value", style="#bbc8e8")

        if options["config_file"]:
            self.config.config_file = [options["config_file"]]

        # Load from config files
        for config_file in self.config.config_file:
            if os.path.isfile(config_file):
                cfg = RawConfigParser()
                cfg.read(config_file)

                for option, data_type in self.config_object_options.items():
                    if cfg.has_option("hastin", option):
                        value = self.verify_config_value(option, cfg.get("hastin", option), data_type)
                        if option not in login_options and value:
                            self.set_config_value("hastin config", option, value)
                        else:
                            hastin_config_login_options_used[option] = value

                for section in cfg.sections():
                    if section.startswith("credential_profile"):
                        self.parse_credential_profile(cfg, section)
                    elif section != "hastin":
                        hosts = self.parse_hostgroup(cfg, section, config_file)
                        if hosts:
                            hostgroups[section] = hosts

        self.config.hostgroup_hosts = hostgroups

        for option in self.config_object_options:
            if option not in login_options and options.get(option):
                self.set_config_value("command-line", option, options[option])

        # Set login options in order of precedence
        for option in login_options:
            if hastin_config_login_options_used.get(option):
                self.set_config_value("hastin config", option, hastin_config_login_options_used[option])

            env_var = f"HASTIN_{option.upper()}"
            if os.environ.get(env_var):
                self.set_config_value("env variable", option, os.environ.get(env_var))

            if self.config.credential_profile:
                profile = self.config.credential_profiles.get(self.config.credential_profile)
                if profile and hasattr(profile, option) and getattr(profile, option):
                    self.set_config_value(f"cred profile {profile.name}", option, getattr(profile, option))

            if options.get(option):
                self.set_config_value("command-line", option, options[option])

        # Parse URI
        if options["uri"]:
            try:
                parsed = urlparse(options["uri"])
                if parsed.scheme in ("postgresql", "postgres"):
                    self.set_config_value("uri", "user", parsed.username)
                    self.set_config_value("uri", "password", parsed.password)
                    self.set_config_value("uri", "host", parsed.hostname)
                    self.set_config_value("uri", "port", parsed.port or 5432)
                    if parsed.path and parsed.path != "/":
                        self.set_config_value("uri", "database", parsed.path.lstrip("/"))
                elif parsed.scheme == "pgbouncer":
                    # PgBouncer standalone mode
                    self.set_config_value("uri", "pgbouncer_mode", True)
                    self.set_config_value("uri", "user", parsed.username)
                    self.set_config_value("uri", "password", parsed.password)
                    self.set_config_value("uri", "host", parsed.hostname)
                    self.set_config_value("uri", "port", parsed.port or 6432)
                    # For PgBouncer, database should be 'pgbouncer' for admin commands
                    db = parsed.path.lstrip("/") if parsed.path and parsed.path != "/" else "pgbouncer"
                    self.set_config_value("uri", "database", db)
                else:
                    self.exit("Invalid URI scheme: Use postgresql:// or pgbouncer://")
            except Exception as e:
                self.exit(f"Invalid URI: {e}")

        if self.config.exclude_notify_global_vars:
            self.config.exclude_notify_global_vars = self.config.exclude_notify_global_vars.split(",")

        try:
            self.config.startup_panels = self.panels.validate_panels(self.config.startup_panels, self.panels.all())
            self.config.daemon_mode_panels = self.panels.validate_panels(
                self.config.daemon_mode_panels, self.panels.get_all_daemon_panel_names()
            )
        except ValueError as e:
            self.exit(str(e))

        if os.path.exists(self.config.tab_setup_file):
            with open(self.config.tab_setup_file) as f:
                self.config.tab_setup_available_hosts = [line.strip() for line in f]

        if self.debug_options:
            self.console.print(self.debug_options_table)
            sys.exit()

        if self.config.daemon_mode:
            self.config.record_for_replay = True
            if not self.config.replay_dir:
                self.exit("Daemon mode requires --replay-dir")

        if self.config.replay_file and not os.path.isfile(self.config.replay_file):
            self.exit(f"Replay file not found: {self.config.replay_file}")

        if self.config.record_for_replay and not self.config.replay_dir:
            self.exit("--record requires --replay-dir")

        if self.config.replay_file and not self.config.replay_dir:
            self.config.replay_dir = os.path.dirname(os.path.dirname(self.config.replay_file))

    def parse_hostgroup(self, cfg, section, config_file) -> list[HostGroupMember]:
        hosts = []
        for key in cfg.options(section):
            try:
                host_data = json.loads(cfg.get(section, key).strip())
                host = host_data.get("host")
                port = self.config.port
                if ":" in host:
                    host, port = host.split(":")
                hosts.append(
                    HostGroupMember(
                        tab_title=host_data.get("tab_title"),
                        host=host,
                        port=int(port),
                        credential_profile=host_data.get("credential_profile"),
                    )
                )
            except json.JSONDecodeError:
                self.exit(f"Invalid JSON in hostgroup {section}, key {key}")
        return hosts

    def parse_credential_profile(self, cfg: RawConfigParser, section: str):
        credential_options = ["host", "port", "user", "password", "database", "ssl_mode"]
        credential_name = section.split("credential_profile_")[1]
        credential = CredentialProfile(name=credential_name)

        for key in cfg.options(section):
            if key in credential_options:
                setattr(credential, key, cfg.get(section, key).strip())

        self.config.credential_profiles[credential_name] = credential

    def verify_config_value(self, option, value, data_type):
        if data_type is bool:
            return value.lower() == "true"
        elif data_type is int:
            try:
                return int(value)
            except ValueError:
                self.exit(f"Config error: {option} must be an integer")
        return value

    def exit(self, message):
        self.console.print(f"[indian_red]{message}[/indian_red]")
        sys.exit()
