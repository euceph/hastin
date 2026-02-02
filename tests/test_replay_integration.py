"""Integration tests for replay functionality with real PostgreSQL database.

These tests require:
- SSH tunnel to the test database: ssh -f -N -L 15433:localhost:5433 vps
"""

import os
import tempfile
import time

import pytest

# Skip all tests if tunnel not available
pytestmark = pytest.mark.skipif(
    os.environ.get("SKIP_INTEGRATION_TESTS") == "1",
    reason="Integration tests skipped by environment variable"
)


class TestReplayIntegration:
    """Integration tests for replay with real database."""

    @pytest.fixture
    def test_db_config(self):
        """Test database configuration."""
        return {
            "host": "localhost",
            "port": 15433,
            "user": "hathi",
            "password": "hathi_test_password",
            "database": "hathi_test",
        }

    @pytest.fixture
    def check_tunnel(self, test_db_config):
        """Check if the SSH tunnel is available."""
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        try:
            result = sock.connect_ex((test_db_config["host"], test_db_config["port"]))
            if result != 0:
                pytest.skip("SSH tunnel to test database not available (port 15433)")
        finally:
            sock.close()

    def test_record_and_replay_with_real_db(self, test_db_config, check_tunnel):
        """Test recording and replaying with the real test database."""
        import psycopg
        from psycopg.rows import dict_row

        from hastin.DataTypes import ProcesslistThread
        from hastin.Modules.ReplayManager import ReplayManager, PostgreSQLReplayData
        from hastin.Modules.MetricManager import MetricData
        from collections import deque
        from dataclasses import dataclass, field

        # Create a minimal mock that matches what ReplayManager expects
        @dataclass
        class MockMetricInstance:
            test_metric: MetricData = field(default_factory=lambda: MetricData(
                label="Test", color=(0, 0, 0), values=deque([10, 20])
            ))

        @dataclass
        class MockMetricInstances:
            system_cpu: MockMetricInstance = field(default_factory=MockMetricInstance)

        class MockMetricManager:
            def __init__(self):
                self.datetimes = deque(["01/01/24 12:00:00"])
                self.metrics = MockMetricInstances()

        class MockHastin:
            def __init__(self, replay_dir, replay_file=None):
                self.host = test_db_config["host"]
                self.port = test_db_config["port"]
                self.record_for_replay = replay_file is None
                self.replay_file = replay_file
                self.replay_dir = replay_dir
                self.system_utilization = {}
                self.global_variables = {}
                self.processlist_threads = {}
                self.replication_status = {}
                self.connection_stats = {}
                self.database_stats = {}
                self.metric_manager = MockMetricManager()
                self.worker_processing_time = 0.0

        with tempfile.TemporaryDirectory() as tmpdir:
            # Connect to real database and get real data
            conninfo = (
                f"host={test_db_config['host']} "
                f"port={test_db_config['port']} "
                f"user={test_db_config['user']} "
                f"password={test_db_config['password']} "
                f"dbname={test_db_config['database']}"
            )

            with psycopg.connect(conninfo, row_factory=dict_row) as conn:
                with conn.cursor() as cur:
                    # Get real processlist
                    cur.execute("""
                        SELECT
                            pid,
                            usename as user,
                            datname as database,
                            client_addr::text as host,
                            application_name as application,
                            state,
                            wait_event_type,
                            wait_event,
                            query,
                            EXTRACT(EPOCH FROM (now() - query_start))::int as time
                        FROM pg_stat_activity
                        WHERE pid != pg_backend_pid()
                    """)
                    processlist_rows = cur.fetchall()

                    # Get real connection stats
                    cur.execute("""
                        SELECT
                            COUNT(*) FILTER (WHERE state = 'active') as active,
                            COUNT(*) FILTER (WHERE state = 'idle') as idle,
                            COUNT(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction
                        FROM pg_stat_activity
                    """)
                    connection_stats = cur.fetchone()

                    # Get database stats
                    cur.execute("""
                        SELECT
                            xact_commit, xact_rollback,
                            blks_hit, blks_read,
                            tup_returned, tup_fetched,
                            tup_inserted, tup_updated, tup_deleted
                        FROM pg_stat_database
                        WHERE datname = current_database()
                    """)
                    database_stats = cur.fetchone()

            # Set up mock hastin with real data
            hathi_record = MockHastin(replay_dir=tmpdir)
            hathi_record.system_utilization = {"cpu_percent": 25, "memory_percent": 50}
            hathi_record.global_variables = {"server_version": "16.0", "max_connections": "100"}
            hathi_record.connection_stats = dict(connection_stats) if connection_stats else {}
            hathi_record.database_stats = dict(database_stats) if database_stats else {}

            # Convert processlist rows to ProcesslistThread objects
            for row in processlist_rows:
                pid = str(row["pid"])
                hathi_record.processlist_threads[pid] = ProcesslistThread(row)

            # Record state
            manager_record = ReplayManager(hathi_record)
            manager_record.capture_state()
            manager_record.close()

            recording_file = manager_record.recording_file

            # Verify file was created
            assert os.path.exists(recording_file)
            assert os.path.getsize(recording_file) > 0

            # Load and replay
            hathi_replay = MockHastin(replay_dir=tmpdir, replay_file=recording_file)
            manager_replay = ReplayManager(hathi_replay)

            # Verify data loaded
            assert len(manager_replay.replay_data) == 1

            # Get the replay event
            result = manager_replay.get_next_refresh_interval()

            assert result is not None
            assert isinstance(result, PostgreSQLReplayData)

            # Verify data was preserved
            assert result.system_utilization["cpu_percent"] == 25
            assert result.system_utilization["memory_percent"] == 50
            assert result.global_variables["server_version"] == "16.0"
            assert result.connection_stats == hathi_record.connection_stats
            assert result.database_stats == hathi_record.database_stats

            # Verify processlist was preserved
            assert len(result.processlist) == len(processlist_rows)
            for pid_str in result.processlist:
                thread = result.processlist[pid_str]
                assert isinstance(thread, ProcesslistThread)

    def test_record_multiple_frames_with_activity(self, test_db_config, check_tunnel):
        """Test recording multiple frames while generating database activity."""
        import psycopg
        from psycopg.rows import dict_row

        from hastin.DataTypes import ProcesslistThread
        from hastin.Modules.ReplayManager import ReplayManager
        from hastin.Modules.MetricManager import MetricData
        from collections import deque
        from dataclasses import dataclass, field

        @dataclass
        class MockMetricInstance:
            test_metric: MetricData = field(default_factory=lambda: MetricData(
                label="Test", color=(0, 0, 0), values=deque([10])
            ))

        @dataclass
        class MockMetricInstances:
            system_cpu: MockMetricInstance = field(default_factory=MockMetricInstance)

        class MockMetricManager:
            def __init__(self):
                self.datetimes = deque()
                self.metrics = MockMetricInstances()

        class MockHastin:
            def __init__(self, replay_dir, replay_file=None):
                self.host = test_db_config["host"]
                self.port = test_db_config["port"]
                self.record_for_replay = replay_file is None
                self.replay_file = replay_file
                self.replay_dir = replay_dir
                self.system_utilization = {}
                self.global_variables = {}
                self.processlist_threads = {}
                self.replication_status = {}
                self.connection_stats = {}
                self.database_stats = {}
                self.metric_manager = MockMetricManager()
                self.worker_processing_time = 0.0

        with tempfile.TemporaryDirectory() as tmpdir:
            conninfo = (
                f"host={test_db_config['host']} "
                f"port={test_db_config['port']} "
                f"user={test_db_config['user']} "
                f"password={test_db_config['password']} "
                f"dbname={test_db_config['database']}"
            )

            hathi_record = MockHastin(replay_dir=tmpdir)
            manager_record = ReplayManager(hathi_record)

            # Record 5 frames with database queries in between
            with psycopg.connect(conninfo, row_factory=dict_row) as conn:
                for i in range(5):
                    # Execute some queries to change database stats
                    with conn.cursor() as cur:
                        cur.execute("SELECT COUNT(*) FROM pg_stat_activity")

                        # Get current database stats
                        cur.execute("""
                            SELECT xact_commit, xact_rollback
                            FROM pg_stat_database
                            WHERE datname = current_database()
                        """)
                        stats = cur.fetchone()

                    # Update mock with real stats
                    hathi_record.database_stats = dict(stats) if stats else {}
                    hathi_record.system_utilization = {"frame": i}

                    # Capture frame
                    manager_record.capture_state()

                    # Small delay to get different timestamps
                    time.sleep(0.1)

            manager_record.close()
            recording_file = manager_record.recording_file

            # Load and verify all frames
            hathi_replay = MockHastin(replay_dir=tmpdir, replay_file=recording_file)
            manager_replay = ReplayManager(hathi_replay)

            assert len(manager_replay.replay_data) == 5

            # Verify each frame has unique data
            frames = []
            for i in range(5):
                result = manager_replay.get_next_refresh_interval()
                assert result is not None
                frames.append(result)
                assert result.system_utilization["frame"] == i

            # Verify we've exhausted all frames
            assert manager_replay.get_next_refresh_interval() is None

    def test_sensitive_data_filtering_with_real_variables(self, test_db_config, check_tunnel):
        """Test that sensitive variables are filtered from real database data."""
        import psycopg
        from psycopg.rows import dict_row

        from hastin.Modules.ReplayManager import ReplayManager
        from hastin.Modules.MetricManager import MetricData
        from collections import deque
        from dataclasses import dataclass, field

        @dataclass
        class MockMetricInstance:
            test_metric: MetricData = field(default_factory=lambda: MetricData(
                label="Test", color=(0, 0, 0), values=deque([10])
            ))

        @dataclass
        class MockMetricInstances:
            system_cpu: MockMetricInstance = field(default_factory=MockMetricInstance)

        class MockMetricManager:
            def __init__(self):
                self.datetimes = deque()
                self.metrics = MockMetricInstances()

        class MockHastin:
            def __init__(self, replay_dir, replay_file=None):
                self.host = test_db_config["host"]
                self.port = test_db_config["port"]
                self.record_for_replay = replay_file is None
                self.replay_file = replay_file
                self.replay_dir = replay_dir
                self.system_utilization = {}
                self.global_variables = {}
                self.processlist_threads = {}
                self.replication_status = {}
                self.connection_stats = {}
                self.database_stats = {}
                self.metric_manager = MockMetricManager()
                self.worker_processing_time = 0.0

        with tempfile.TemporaryDirectory() as tmpdir:
            conninfo = (
                f"host={test_db_config['host']} "
                f"port={test_db_config['port']} "
                f"user={test_db_config['user']} "
                f"password={test_db_config['password']} "
                f"dbname={test_db_config['database']}"
            )

            # Get real PostgreSQL settings
            with psycopg.connect(conninfo, row_factory=dict_row) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT name, setting FROM pg_settings")
                    settings = {row["name"]: row["setting"] for row in cur.fetchall()}

            hathi_record = MockHastin(replay_dir=tmpdir)
            hathi_record.global_variables = settings

            manager_record = ReplayManager(hathi_record)
            manager_record.capture_state()
            manager_record.close()

            # Load and check sensitive data is filtered
            hathi_replay = MockHastin(replay_dir=tmpdir, replay_file=manager_record.recording_file)
            manager_replay = ReplayManager(hathi_replay)

            result = manager_replay.get_next_refresh_interval()

            # These variables should be filtered out
            sensitive_patterns = ["ssl", "password", "key"]
            for var_name in result.global_variables:
                for pattern in sensitive_patterns:
                    assert pattern not in var_name.lower(), f"Sensitive variable {var_name} was not filtered"

            assert len(result.global_variables) > 0
            if "server_version" in settings:
                assert "server_version" in result.global_variables


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
