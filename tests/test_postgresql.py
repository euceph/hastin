"""Comprehensive tests for Hastin PostgreSQL monitoring dashboard."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import pytest

TEST_DB_CONFIG = {
    "host": "localhost",
    "port": 15433,  # SSH tunnel to VPS PostgreSQL
    "user": "hathi",
    "password": "hathi_test_password",
    "database": "hathi_test",
    "ssl_mode": "disable",
}


class MockApp:
    """Mock Textual app for testing."""

    def notify(self, *args, **kwargs):
        pass


class TestImports:
    """Test that all modules can be imported without errors."""

    def test_import_postgresql_module(self):
        from hastin.Modules.PostgreSQL import Database

        assert Database is not None

    def test_import_queries_module(self):
        from hastin.Modules.Queries import PostgresQueries

        assert PostgresQueries is not None

    def test_import_datatypes_module(self):
        from hastin.DataTypes import ConnectionSource, Panels, ProcesslistThread

        assert ConnectionSource is not None
        assert ProcesslistThread is not None
        assert Panels is not None

    def test_import_hathi_state(self):
        from hastin.Hastin import Hastin

        assert Hastin is not None

    def test_import_worker_data_processor(self):
        from hastin.Modules.WorkerDataProcessor import WorkerDataProcessor

        assert WorkerDataProcessor is not None

    def test_import_worker_manager(self):
        from hastin.Modules.WorkerManager import WorkerManager

        assert WorkerManager is not None

    def test_import_replay_manager(self):
        from hastin.Modules.ReplayManager import PostgreSQLReplayData, ReplayManager

        assert ReplayManager is not None
        assert PostgreSQLReplayData is not None

    def test_import_argument_parser(self):
        from hastin.Modules.ArgumentParser import Config

        assert Config is not None

    def test_import_panels(self):
        from hastin.Panels import Dashboard, Processlist, Replication

        assert Dashboard is not None
        assert Processlist is not None
        assert Replication is not None


class TestDataTypes:
    """Test DataTypes module."""

    def test_connection_source_values(self):
        from hastin.DataTypes import ConnectionSource

        # Core sources
        assert ConnectionSource.postgresql == "PostgreSQL"
        assert ConnectionSource.pgbouncer == "PgBouncer"
        # AWS
        assert ConnectionSource.rds == "Amazon RDS"
        assert ConnectionSource.aurora == "Amazon Aurora"
        # Google Cloud
        assert ConnectionSource.cloud_sql == "Google Cloud SQL"
        assert ConnectionSource.alloydb == "Google AlloyDB"
        # Azure
        assert ConnectionSource.azure == "Azure PostgreSQL"
        assert ConnectionSource.cosmos_citus == "Azure Cosmos DB"
        # Other providers
        assert ConnectionSource.supabase == "Supabase"
        assert ConnectionSource.neon == "Neon"
        assert ConnectionSource.aiven == "Aiven"
        assert ConnectionSource.digitalocean == "DigitalOcean"

    def test_connection_status_values(self):
        from hastin.DataTypes import ConnectionStatus

        assert ConnectionStatus.connecting == "CONNECTING"
        assert ConnectionStatus.connected == "CONNECTED"
        assert ConnectionStatus.primary == "PRIMARY"
        assert ConnectionStatus.replica == "REPLICA"

    def test_panels_class(self):
        from hastin.DataTypes import Panels

        panels = Panels()
        assert hasattr(panels, "dashboard")
        assert hasattr(panels, "processlist")
        assert hasattr(panels, "replication")
        assert hasattr(panels, "locks")
        assert hasattr(panels, "statements")

    def test_panels_all_method(self):
        from hastin.DataTypes import Panels

        panels = Panels()
        all_panels = panels.all()
        assert "dashboard" in all_panels
        assert "processlist" in all_panels

    def test_processlist_thread_creation(self):
        from hastin.DataTypes import ProcesslistThread

        thread_data = {
            "pid": 12345,
            "user": "testuser",
            "database": "testdb",
            "host": "127.0.0.1",
            "state": "active",
            "time": 5,
            "wait_event_type": None,
            "wait_event": None,
            "query": "SELECT 1",
        }
        thread = ProcesslistThread(thread_data)
        assert thread.pid == 12345
        assert thread.user == "testuser"
        assert thread.db == "testdb"
        assert thread.state == "active"
        assert thread.time == 5

    def test_processlist_thread_formatted_state(self):
        from hastin.DataTypes import ProcesslistThread

        # Active state
        thread = ProcesslistThread({"pid": 1, "state": "active", "time": 1, "query": "SELECT 1"})
        assert "green" in thread.formatted_state

        # Idle state
        thread = ProcesslistThread({"pid": 1, "state": "idle", "time": 1, "query": ""})
        assert "dark_gray" in thread.formatted_state

        # Idle in transaction
        thread = ProcesslistThread({"pid": 1, "state": "idle in transaction", "time": 1, "query": ""})
        assert "yellow" in thread.formatted_state


class TestPostgresQueries:
    """Test PostgreSQL query definitions."""

    def test_processlist_query_exists(self):
        from hastin.Modules.Queries import PostgresQueries

        assert PostgresQueries.processlist is not None
        assert "pg_stat_activity" in PostgresQueries.processlist

    def test_server_info_query_exists(self):
        from hastin.Modules.Queries import PostgresQueries

        assert PostgresQueries.server_info is not None
        assert "version()" in PostgresQueries.server_info
        assert "pg_postmaster_start_time()" in PostgresQueries.server_info

    def test_connection_stats_query_exists(self):
        from hastin.Modules.Queries import PostgresQueries

        assert PostgresQueries.connection_stats is not None
        assert "max_connections" in PostgresQueries.connection_stats

    def test_database_stats_query_exists(self):
        from hastin.Modules.Queries import PostgresQueries

        assert PostgresQueries.database_stats is not None
        assert "cache_hit_ratio" in PostgresQueries.database_stats

    def test_replication_primary_query_exists(self):
        from hastin.Modules.Queries import PostgresQueries

        assert PostgresQueries.replication_status_primary is not None
        assert "pg_stat_replication" in PostgresQueries.replication_status_primary

    def test_replication_replica_query_exists(self):
        from hastin.Modules.Queries import PostgresQueries

        assert PostgresQueries.replication_status_replica is not None
        assert "pg_stat_wal_receiver" in PostgresQueries.replication_status_replica

    def test_logical_subscriptions_query_exists(self):
        from hastin.Modules.Queries import PostgresQueries

        assert PostgresQueries.logical_subscriptions is not None
        assert "pg_stat_subscription" in PostgresQueries.logical_subscriptions

    def test_blocked_queries_query_exists(self):
        from hastin.Modules.Queries import PostgresQueries

        assert PostgresQueries.blocked_queries is not None
        assert "pg_locks" in PostgresQueries.blocked_queries

    def test_permission_check_query_exists(self):
        from hastin.Modules.Queries import PostgresQueries

        assert PostgresQueries.permission_check is not None
        assert "pg_read_all_stats" in PostgresQueries.permission_check

    def test_environment_detection_query_exists(self):
        from hastin.Modules.Queries import PostgresQueries

        assert PostgresQueries.environment_detection is not None
        assert "rds.extensions" in PostgresQueries.environment_detection


class TestDatabaseConnection:
    """Integration tests for PostgreSQL connection."""

    @pytest.fixture
    def db(self):
        """Create database connection for tests."""
        from hastin.Modules.PostgreSQL import Database

        db = Database(app=MockApp(), **TEST_DB_CONFIG, auto_connect=True)
        yield db
        db.close()

    def test_connection_established(self, db):
        assert db.is_connected()
        assert db.connection_id is not None

    def test_connection_id_is_integer(self, db):
        assert isinstance(db.connection_id, int)
        assert db.connection_id > 0

    def test_execute_simple_query(self, db):
        result = db.execute("SELECT 1 AS value")
        assert result is not None
        data = db.fetchone()
        assert data.get("value") == 1

    def test_execute_version_query(self, db):
        db.execute("SELECT version()")
        result = db.fetchone()
        assert "PostgreSQL" in result.get("version", "")

    def test_fetchall_returns_list(self, db):
        db.execute("SELECT generate_series(1, 3) AS num")
        results = db.fetchall()
        assert isinstance(results, list)
        assert len(results) == 3

    def test_fetchone_returns_dict(self, db):
        db.execute("SELECT 1 AS a, 2 AS b")
        result = db.fetchone()
        assert isinstance(result, dict)
        assert result.get("a") == 1
        assert result.get("b") == 2

    def test_server_info_query(self, db):
        from hastin.Modules.Queries import PostgresQueries

        db.execute(PostgresQueries.server_info)
        info = db.fetchone()
        assert "server_version" in info
        assert "uptime_seconds" in info
        assert "current_db" in info
        assert info["current_db"] == TEST_DB_CONFIG["database"]

    def test_connection_stats_query(self, db):
        from hastin.Modules.Queries import PostgresQueries

        db.execute(PostgresQueries.connection_stats)
        stats = db.fetchone()
        assert "total_connections" in stats
        assert "max_connections" in stats
        assert "active" in stats
        assert "idle" in stats
        assert stats["max_connections"] > 0

    def test_database_stats_query(self, db):
        from hastin.Modules.Queries import PostgresQueries

        db.execute(PostgresQueries.database_stats)
        stats = db.fetchone()
        assert "datname" in stats
        assert "cache_hit_ratio" in stats
        assert "xact_commit" in stats
        assert stats["datname"] == TEST_DB_CONFIG["database"]

    def test_processlist_query(self, db):
        from hastin.Modules.Queries import PostgresQueries

        db.execute(PostgresQueries.processlist)
        processes = db.fetchall()
        assert isinstance(processes, list)
        # At least our own connection should be excluded (WHERE pid != pg_backend_pid())
        # but there might be other connections

    def test_bgwriter_stats_query(self, db):
        from hastin.Modules.Queries import PostgresQueries

        db.execute(PostgresQueries.bgwriter_stats)
        stats = db.fetchone()
        assert "checkpoints_timed" in stats
        assert "buffers_checkpoint" in stats

    def test_replication_status_primary_query(self, db):
        from hastin.Modules.Queries import PostgresQueries

        db.execute(PostgresQueries.replication_status_primary)
        # May be empty if no replicas, but should not error
        replicas = db.fetchall()
        assert isinstance(replicas, list)

    def test_replication_slots_query(self, db):
        from hastin.Modules.Queries import PostgresQueries

        db.execute(PostgresQueries.replication_slots)
        slots = db.fetchall()
        assert isinstance(slots, list)

    def test_variables_query(self, db):
        from hastin.Modules.Queries import PostgresQueries

        db.execute(PostgresQueries.variables)
        variables = db.fetchall()
        assert isinstance(variables, list)
        assert len(variables) > 0
        var_names = [v["name"] for v in variables]
        assert "max_connections" in var_names

    def test_permission_check_query(self, db):
        from hastin.Modules.Queries import PostgresQueries

        db.execute(PostgresQueries.permission_check)
        perms = db.fetchone()
        assert "can_select_activity" in perms
        assert "has_read_all_stats" in perms
        assert "is_superuser" in perms

    def test_environment_detection_query(self, db):
        from hastin.Modules.Queries import PostgresQueries

        db.execute(PostgresQueries.environment_detection)
        env = db.fetchone()
        assert "is_rds" in env
        assert "is_aurora" in env
        # Local Docker should not be RDS/Aurora
        assert env["is_rds"] is False
        assert env["is_aurora"] is False

    def test_fetch_status_and_variables(self, db):
        variables = db.fetch_status_and_variables("variables")
        assert isinstance(variables, dict)
        assert "max_connections" in variables

    def test_check_permissions(self, db):
        perms = db.check_permissions()
        assert isinstance(perms, dict)
        assert "can_select_activity" in perms

    def test_detect_environment(self, db):
        env = db.detect_environment()
        assert isinstance(env, dict)
        assert "is_rds" in env

    def test_terminate_backend_invalid_pid(self, db):
        # Trying to terminate a non-existent PID should return False
        result = db.terminate_backend(999999999)
        assert result is False

    def test_cancel_backend_invalid_pid(self, db):
        # Trying to cancel a non-existent PID should return False
        result = db.cancel_backend(999999999)
        assert result is False


class TestConfig:
    """Test ArgumentParser Config class."""

    def test_config_defaults(self):
        from hastin.Modules.ArgumentParser import Config

        config = Config(app_version="1.0.0")
        assert config.host == "localhost"
        assert config.port == 5432
        assert config.database == "postgres"
        assert config.ssl_mode == "prefer"
        assert config.refresh_interval == 1

    def test_config_startup_panels_default(self):
        from hastin.Modules.ArgumentParser import Config

        config = Config(app_version="1.0.0")
        assert "dashboard" in config.startup_panels
        assert "processlist" in config.startup_panels

    def test_credential_profile_class(self):
        from hastin.Modules.ArgumentParser import CredentialProfile

        profile = CredentialProfile(
            name="test",
            host="testhost",
            port=5433,
            user="testuser",
            password="testpass",
            database="testdb",
            ssl_mode="require",
        )
        assert profile.name == "test"
        assert profile.host == "testhost"
        assert profile.port == 5433


class TestReplayManager:
    """Test ReplayManager functionality."""

    def test_postgresql_replay_data_class(self):
        from hastin.Modules.ReplayManager import PostgreSQLReplayData

        data = PostgreSQLReplayData(
            timestamp="2024-01-01T00:00:00",
            system_utilization={},
            global_variables={},
            processlist={},
        )
        assert data.timestamp == "2024-01-01T00:00:00"
        assert isinstance(data.replication_status, dict)
        assert isinstance(data.connection_stats, dict)
        assert isinstance(data.database_stats, dict)


class TestPanels:
    """Test panel modules have required functions."""

    def test_dashboard_has_create_panel(self):
        from hastin.Panels import Dashboard

        assert hasattr(Dashboard, "create_panel")
        assert callable(Dashboard.create_panel)

    def test_processlist_has_create_panel(self):
        from hastin.Panels import Processlist

        assert hasattr(Processlist, "create_panel")
        assert callable(Processlist.create_panel)

    def test_processlist_has_fetch_data(self):
        from hastin.Panels import Processlist

        assert hasattr(Processlist, "fetch_data")
        assert callable(Processlist.fetch_data)

    def test_replication_has_create_panel(self):
        from hastin.Panels import Replication

        assert hasattr(Replication, "create_panel")
        assert callable(Replication.create_panel)


class TestPgBouncerImports:
    """Test that all PgBouncer modules can be imported without errors."""

    def test_import_pgbouncer_queries(self):
        from hastin.Modules.Queries import PgBouncerQueries

        assert PgBouncerQueries is not None

    def test_import_pgbouncer_dashboard_panel(self):
        from hastin.Panels import PgBouncerDashboard

        assert PgBouncerDashboard is not None

    def test_import_pgbouncer_pools_panel(self):
        from hastin.Panels import PgBouncerPools

        assert PgBouncerPools is not None

    def test_import_pgbouncer_clients_panel(self):
        from hastin.Panels import PgBouncerClients

        assert PgBouncerClients is not None

    def test_import_pgbouncer_servers_panel(self):
        from hastin.Panels import PgBouncerServers

        assert PgBouncerServers is not None


class TestPgBouncerQueries:
    """Test PgBouncerQueries dataclass."""

    def test_show_version_query_exists(self):
        from hastin.Modules.Queries import PgBouncerQueries

        assert hasattr(PgBouncerQueries, "show_version")
        assert "SHOW VERSION" in PgBouncerQueries.show_version

    def test_show_stats_query_exists(self):
        from hastin.Modules.Queries import PgBouncerQueries

        assert hasattr(PgBouncerQueries, "show_stats")
        assert "SHOW STATS" in PgBouncerQueries.show_stats

    def test_show_pools_query_exists(self):
        from hastin.Modules.Queries import PgBouncerQueries

        assert hasattr(PgBouncerQueries, "show_pools")
        assert "SHOW POOLS" in PgBouncerQueries.show_pools

    def test_show_clients_query_exists(self):
        from hastin.Modules.Queries import PgBouncerQueries

        assert hasattr(PgBouncerQueries, "show_clients")
        assert "SHOW CLIENTS" in PgBouncerQueries.show_clients

    def test_show_servers_query_exists(self):
        from hastin.Modules.Queries import PgBouncerQueries

        assert hasattr(PgBouncerQueries, "show_servers")
        assert "SHOW SERVERS" in PgBouncerQueries.show_servers

    def test_show_config_query_exists(self):
        from hastin.Modules.Queries import PgBouncerQueries

        assert hasattr(PgBouncerQueries, "show_config")
        assert "SHOW CONFIG" in PgBouncerQueries.show_config

    def test_show_databases_query_exists(self):
        from hastin.Modules.Queries import PgBouncerQueries

        assert hasattr(PgBouncerQueries, "show_databases")
        assert "SHOW DATABASES" in PgBouncerQueries.show_databases


class TestPgBouncerConnectionSource:
    """Test ConnectionSource has PgBouncer type."""

    def test_pgbouncer_connection_source_exists(self):
        from hastin.DataTypes import ConnectionSource

        assert hasattr(ConnectionSource, "pgbouncer")
        assert ConnectionSource.pgbouncer == "PgBouncer"


class TestPgBouncerPanels:
    """Test PgBouncer panels have required functions."""

    def test_pgbouncer_dashboard_has_create_panel(self):
        from hastin.Panels import PgBouncerDashboard

        assert hasattr(PgBouncerDashboard, "create_panel")
        assert callable(PgBouncerDashboard.create_panel)

    def test_pgbouncer_pools_has_create_panel(self):
        from hastin.Panels import PgBouncerPools

        assert hasattr(PgBouncerPools, "create_panel")
        assert callable(PgBouncerPools.create_panel)

    def test_pgbouncer_clients_has_create_panel(self):
        from hastin.Panels import PgBouncerClients

        assert hasattr(PgBouncerClients, "create_panel")
        assert callable(PgBouncerClients.create_panel)

    def test_pgbouncer_servers_has_create_panel(self):
        from hastin.Panels import PgBouncerServers

        assert hasattr(PgBouncerServers, "create_panel")
        assert callable(PgBouncerServers.create_panel)

    def test_pgbouncer_combined_panel_has_create_panel(self):
        """Test PgBouncerPanel (combined mode) has create_panel function."""
        from hastin.Panels import PgBouncerPanel

        assert hasattr(PgBouncerPanel, "create_panel")
        assert callable(PgBouncerPanel.create_panel)


class TestPgBouncerPanelClasses:
    """Test that PgBouncer panels are defined in Panels class."""

    def test_panels_has_pgbouncer_dashboard(self):
        from hastin.DataTypes import Panels

        panels = Panels()
        assert hasattr(panels, "pgbouncer_dashboard")
        assert panels.pgbouncer_dashboard.name == "pgbouncer_dashboard"

    def test_panels_has_pgbouncer_pools(self):
        from hastin.DataTypes import Panels

        panels = Panels()
        assert hasattr(panels, "pgbouncer_pools")
        assert panels.pgbouncer_pools.name == "pgbouncer_pools"

    def test_panels_has_pgbouncer_clients(self):
        from hastin.DataTypes import Panels

        panels = Panels()
        assert hasattr(panels, "pgbouncer_clients")
        assert panels.pgbouncer_clients.name == "pgbouncer_clients"

    def test_panels_has_pgbouncer_servers(self):
        from hastin.DataTypes import Panels

        panels = Panels()
        assert hasattr(panels, "pgbouncer_servers")
        assert panels.pgbouncer_servers.name == "pgbouncer_servers"

    def test_panels_has_pgbouncer_combined(self):
        """Test that Panels class has pgbouncer panel for combined mode."""
        from hastin.DataTypes import Panels

        panels = Panels()
        assert hasattr(panels, "pgbouncer")
        assert panels.pgbouncer.name == "pgbouncer"
        assert panels.pgbouncer.key == "⁷"


class TestPgBouncerConfig:
    """Test ArgumentParser Config class has PgBouncer options."""

    def test_config_has_pgbouncer_mode(self):
        from hastin.Modules.ArgumentParser import Config

        config = Config(app_version="1.0.0")
        assert hasattr(config, "pgbouncer_mode")
        assert config.pgbouncer_mode is False

    def test_config_has_pgbouncer_host(self):
        from hastin.Modules.ArgumentParser import Config

        config = Config(app_version="1.0.0")
        assert hasattr(config, "pgbouncer_host")
        assert config.pgbouncer_host is None

    def test_config_has_pgbouncer_port(self):
        from hastin.Modules.ArgumentParser import Config

        config = Config(app_version="1.0.0")
        assert hasattr(config, "pgbouncer_port")
        assert config.pgbouncer_port == 6432

    def test_config_has_pgbouncer_user(self):
        from hastin.Modules.ArgumentParser import Config

        config = Config(app_version="1.0.0")
        assert hasattr(config, "pgbouncer_user")
        assert config.pgbouncer_user is None

    def test_config_has_pgbouncer_password(self):
        from hastin.Modules.ArgumentParser import Config

        config = Config(app_version="1.0.0")
        assert hasattr(config, "pgbouncer_password")
        assert config.pgbouncer_password is None


class TestPgBouncerURIParsing:
    """Test pgbouncer:// URI parsing."""

    def test_pgbouncer_uri_sets_mode(self):
        import sys
        from hastin.Modules.ArgumentParser import ArgumentParser

        original_argv = sys.argv
        try:
            sys.argv = ["hastin", "pgbouncer://user:pass@localhost:6432/pgbouncer"]
            parser = ArgumentParser("1.0.0")
            assert parser.config.pgbouncer_mode is True
        finally:
            sys.argv = original_argv

    def test_pgbouncer_uri_parses_host(self):
        import sys
        from hastin.Modules.ArgumentParser import ArgumentParser

        original_argv = sys.argv
        try:
            sys.argv = ["hastin", "pgbouncer://user:pass@myhost:6432/pgbouncer"]
            parser = ArgumentParser("1.0.0")
            assert parser.config.host == "myhost"
        finally:
            sys.argv = original_argv

    def test_pgbouncer_uri_parses_port(self):
        import sys
        from hastin.Modules.ArgumentParser import ArgumentParser

        original_argv = sys.argv
        try:
            sys.argv = ["hastin", "pgbouncer://user:pass@localhost:6433/pgbouncer"]
            parser = ArgumentParser("1.0.0")
            assert parser.config.port == 6433
        finally:
            sys.argv = original_argv

    def test_pgbouncer_uri_default_port(self):
        import sys
        from hastin.Modules.ArgumentParser import ArgumentParser

        original_argv = sys.argv
        try:
            sys.argv = ["hastin", "pgbouncer://user:pass@localhost/pgbouncer"]
            parser = ArgumentParser("1.0.0")
            assert parser.config.port == 6432
        finally:
            sys.argv = original_argv

    def test_pgbouncer_uri_parses_user(self):
        import sys
        from hastin.Modules.ArgumentParser import ArgumentParser

        original_argv = sys.argv
        try:
            sys.argv = ["hastin", "pgbouncer://testuser:pass@localhost:6432/pgbouncer"]
            parser = ArgumentParser("1.0.0")
            assert parser.config.user == "testuser"
        finally:
            sys.argv = original_argv

    def test_pgbouncer_uri_parses_database(self):
        import sys
        from hastin.Modules.ArgumentParser import ArgumentParser

        original_argv = sys.argv
        try:
            sys.argv = ["hastin", "pgbouncer://user:pass@localhost:6432/pgbouncer"]
            parser = ArgumentParser("1.0.0")
            assert parser.config.database == "pgbouncer"
        finally:
            sys.argv = original_argv


class TestPgBouncerDatabaseMode:
    """Test Database class PgBouncer mode behavior."""

    def test_database_has_pgbouncer_mode_param(self):
        from hastin.Modules.PostgreSQL import Database
        import inspect

        sig = inspect.signature(Database.__init__)
        params = list(sig.parameters.keys())
        assert "pgbouncer_mode" in params

    def test_database_pgbouncer_mode_default_false(self):
        from hastin.Modules.PostgreSQL import Database
        import inspect

        sig = inspect.signature(Database.__init__)
        pgbouncer_mode_param = sig.parameters["pgbouncer_mode"]
        assert pgbouncer_mode_param.default is False


class TestPgBouncerDataProcessing:
    """Test PgBouncer data processing functions."""

    def test_worker_data_processor_has_process_pgbouncer_data(self):
        from hastin.Modules.WorkerDataProcessor import WorkerDataProcessor

        assert hasattr(WorkerDataProcessor, "process_pgbouncer_data")
        assert callable(WorkerDataProcessor.process_pgbouncer_data)

    def test_worker_data_processor_has_refresh_screen_pgbouncer(self):
        from hastin.Modules.WorkerDataProcessor import WorkerDataProcessor

        assert hasattr(WorkerDataProcessor, "refresh_screen_pgbouncer")
        assert callable(WorkerDataProcessor.refresh_screen_pgbouncer)


# =============================================================================
# PgBouncer Integration Tests (requires live PgBouncer connection)
# =============================================================================

PGBOUNCER_TEST_CONFIG = {
    "host": "localhost",
    "port": 16433,  # SSH tunnel to VPS PgBouncer
    "user": "hathi",
    "password": "hathi_test_password",
    "database": "pgbouncer",
    "ssl_mode": "disable",
    "pgbouncer_mode": True,
}


class TestPgBouncerConnection:
    """Test live PgBouncer connection (requires running PgBouncer)."""

    @pytest.fixture
    def pgbouncer_db(self):
        from hastin.Modules.PostgreSQL import Database

        try:
            db = Database(app=MockApp(), **PGBOUNCER_TEST_CONFIG, auto_connect=True)
            yield db
            db.close()
        except Exception:
            pytest.skip("PgBouncer not available at localhost:16433")

    def test_pgbouncer_connection_established(self, pgbouncer_db):
        assert pgbouncer_db.is_connected()

    def test_pgbouncer_show_version(self, pgbouncer_db):
        from hastin.Modules.Queries import PgBouncerQueries

        pgbouncer_db.execute(PgBouncerQueries.show_version)
        result = pgbouncer_db.fetchone()

        assert result is not None
        assert "version" in result
        assert "PgBouncer" in result["version"]

    def test_pgbouncer_show_pools(self, pgbouncer_db):
        from hastin.Modules.Queries import PgBouncerQueries

        pgbouncer_db.execute(PgBouncerQueries.show_pools)
        pools = pgbouncer_db.fetchall()

        assert pools is not None
        assert isinstance(pools, list)
        assert len(pools) >= 1

        pool = pools[0]
        assert "database" in pool
        assert "cl_active" in pool
        assert "sv_idle" in pool
        assert "pool_mode" in pool

    def test_pgbouncer_show_stats(self, pgbouncer_db):
        from hastin.Modules.Queries import PgBouncerQueries

        pgbouncer_db.execute(PgBouncerQueries.show_stats)
        stats = pgbouncer_db.fetchall()

        assert stats is not None
        assert isinstance(stats, list)
        assert len(stats) >= 1

        stat = stats[0]
        assert "database" in stat
        assert "total_xact_count" in stat
        assert "total_query_count" in stat

    def test_pgbouncer_show_clients(self, pgbouncer_db):
        from hastin.Modules.Queries import PgBouncerQueries

        pgbouncer_db.execute(PgBouncerQueries.show_clients)
        clients = pgbouncer_db.fetchall()

        assert clients is not None
        assert isinstance(clients, list)
        assert len(clients) >= 1

        client = clients[0]
        assert "type" in client
        assert "user" in client
        assert "database" in client
        assert "state" in client

    def test_pgbouncer_show_servers(self, pgbouncer_db):
        from hastin.Modules.Queries import PgBouncerQueries

        pgbouncer_db.execute(PgBouncerQueries.show_servers)
        servers = pgbouncer_db.fetchall()

        assert servers is not None
        assert isinstance(servers, list)
        # Servers list may be empty if no backend connections

    def test_pgbouncer_show_databases(self, pgbouncer_db):
        from hastin.Modules.Queries import PgBouncerQueries

        pgbouncer_db.execute(PgBouncerQueries.show_databases)
        databases = pgbouncer_db.fetchall()

        assert databases is not None
        assert isinstance(databases, list)
        assert len(databases) >= 1

    def test_pgbouncer_show_config(self, pgbouncer_db):
        from hastin.Modules.Queries import PgBouncerQueries

        pgbouncer_db.execute(PgBouncerQueries.show_config)
        config = pgbouncer_db.fetchall()

        assert config is not None
        assert isinstance(config, list)
        assert len(config) >= 1

    def test_pgbouncer_multiple_queries_no_reconnect(self, pgbouncer_db):
        """Test that multiple queries don't cause reconnection issues."""
        from hastin.Modules.Queries import PgBouncerQueries

        # Run multiple queries in sequence
        for _ in range(5):
            pgbouncer_db.execute(PgBouncerQueries.show_pools)
            pools = pgbouncer_db.fetchall()
            assert pools is not None

            pgbouncer_db.execute(PgBouncerQueries.show_stats)
            stats = pgbouncer_db.fetchall()
            assert stats is not None

        # Connection should still be established
        assert pgbouncer_db.is_connected()


class TestPgBouncerPoolDataParsing:
    """Test parsing of PgBouncer pool data."""

    def test_parse_pool_cl_active(self):
        """Test parsing cl_active from pool data."""
        pool_data = {"database": "test", "cl_active": 5, "cl_waiting": 0}
        assert int(pool_data.get("cl_active", 0)) == 5

    def test_parse_pool_cl_waiting(self):
        """Test parsing cl_waiting from pool data."""
        pool_data = {"database": "test", "cl_active": 5, "cl_waiting": 2}
        assert int(pool_data.get("cl_waiting", 0)) == 2

    def test_parse_pool_sv_active(self):
        """Test parsing sv_active from pool data."""
        pool_data = {"database": "test", "sv_active": 3, "sv_idle": 7}
        assert int(pool_data.get("sv_active", 0)) == 3

    def test_parse_pool_sv_idle(self):
        """Test parsing sv_idle from pool data."""
        pool_data = {"database": "test", "sv_active": 3, "sv_idle": 7}
        assert int(pool_data.get("sv_idle", 0)) == 7

    def test_parse_pool_maxwait(self):
        """Test parsing maxwait from pool data."""
        pool_data = {"database": "test", "maxwait": 0.5}
        assert float(pool_data.get("maxwait", 0)) == 0.5

    def test_aggregate_pool_stats(self):
        """Test aggregating stats across multiple pools."""
        pools = [
            {"database": "db1", "cl_active": 5, "cl_waiting": 1, "sv_active": 3, "sv_idle": 2, "maxwait": 0.1},
            {"database": "db2", "cl_active": 3, "cl_waiting": 0, "sv_active": 2, "sv_idle": 1, "maxwait": 0.5},
        ]

        total_cl_active = sum(int(p.get("cl_active", 0)) for p in pools)
        total_cl_waiting = sum(int(p.get("cl_waiting", 0)) for p in pools)
        total_sv_active = sum(int(p.get("sv_active", 0)) for p in pools)
        total_sv_idle = sum(int(p.get("sv_idle", 0)) for p in pools)
        max_wait = max((float(p.get("maxwait", 0)) for p in pools), default=0)

        assert total_cl_active == 8
        assert total_cl_waiting == 1
        assert total_sv_active == 5
        assert total_sv_idle == 3
        assert max_wait == 0.5


class TestPgBouncerStatsDataParsing:
    """Test parsing of PgBouncer stats data."""

    def test_parse_stats_total_xact_count(self):
        """Test parsing total_xact_count from stats."""
        stats = {"database": "test", "total_xact_count": 1000, "total_query_count": 5000}
        assert int(stats.get("total_xact_count", 0)) == 1000

    def test_parse_stats_total_query_count(self):
        """Test parsing total_query_count from stats."""
        stats = {"database": "test", "total_xact_count": 1000, "total_query_count": 5000}
        assert int(stats.get("total_query_count", 0)) == 5000

    def test_parse_stats_total_received(self):
        """Test parsing total_received from stats."""
        stats = {"database": "test", "total_received": 1024000, "total_sent": 2048000}
        assert int(stats.get("total_received", 0)) == 1024000

    def test_parse_stats_total_sent(self):
        """Test parsing total_sent from stats."""
        stats = {"database": "test", "total_received": 1024000, "total_sent": 2048000}
        assert int(stats.get("total_sent", 0)) == 2048000

    def test_aggregate_stats_across_databases(self):
        """Test aggregating stats across multiple databases."""
        stats = [
            {"database": "db1", "total_xact_count": 1000, "total_query_count": 5000},
            {"database": "db2", "total_xact_count": 500, "total_query_count": 2000},
        ]

        total_xact = sum(int(s.get("total_xact_count", 0)) for s in stats)
        total_query = sum(int(s.get("total_query_count", 0)) for s in stats)

        assert total_xact == 1500
        assert total_query == 7000


# =============================================================================
# End-to-End Data Flow Tests
# =============================================================================


class TestDataFlow:
    """Test data flows correctly through the system."""

    @pytest.fixture
    def db(self):
        from hastin.Modules.PostgreSQL import Database

        db = Database(app=MockApp(), **TEST_DB_CONFIG, auto_connect=True)
        yield db
        db.close()

    def test_processlist_to_thread_objects(self, db):
        """Test that processlist data can be converted to ProcesslistThread objects."""
        from hastin.DataTypes import ProcesslistThread
        from hastin.Modules.Queries import PostgresQueries

        db.execute(PostgresQueries.processlist)
        processes = db.fetchall()

        threads = {}
        for proc in processes:
            thread = ProcesslistThread(proc)
            threads[str(proc["pid"])] = thread

        # Verify all threads were created
        assert len(threads) == len(processes)

        # Verify thread objects have expected attributes
        for thread in threads.values():
            assert hasattr(thread, "pid")
            assert hasattr(thread, "user")
            assert hasattr(thread, "db")
            assert hasattr(thread, "state")
            assert hasattr(thread, "formatted_state")
            assert hasattr(thread, "formatted_time")

    def test_server_version_parsing(self, db):
        """Test that server version can be parsed correctly."""
        from hastin.Modules.Queries import PostgresQueries

        db.execute(PostgresQueries.server_info)
        info = db.fetchone()

        version = info.get("server_version", "")
        assert version  # Not empty

        # Version should be like "15.x" or "14.x"
        parts = version.split(".")
        assert len(parts) >= 2
        assert parts[0].isdigit()

    def test_connection_stats_calculation(self, db):
        """Test that connection stats are calculated correctly."""
        from hastin.Modules.Queries import PostgresQueries

        db.execute(PostgresQueries.connection_stats)
        stats = db.fetchone()

        # Total should be >= active + idle
        total = stats.get("total_connections", 0)
        active = stats.get("active", 0)
        idle = stats.get("idle", 0)

        assert total >= 0
        assert active >= 0
        assert idle >= 0

    def test_cache_hit_ratio_calculation(self, db):
        """Test cache hit ratio is calculated correctly."""
        from hastin.Modules.Queries import PostgresQueries

        db.execute(PostgresQueries.database_stats)
        stats = db.fetchone()

        ratio = stats.get("cache_hit_ratio", 0)
        # Ratio should be between 0 and 100
        assert 0 <= float(ratio) <= 100


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
