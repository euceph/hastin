"""Comprehensive tests for the Hastin replay functionality."""

import os
import tempfile
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import orjson
import pytest
import zstandard as zstd

from hastin.DataTypes import ProcesslistThread
from hastin.Modules.MetricManager import MetricData
from hastin.Modules.ReplayManager import PostgreSQLReplayData, ReplayManager


@dataclass
class MockMetricInstance:
    """Mock metric instance for testing."""
    test_metric: MetricData = field(default_factory=lambda: MetricData(
        label="Test", color=(0, 0, 0), values=deque([10, 20])
    ))


@dataclass
class MockMetricInstances:
    """Mock MetricInstances container."""
    system_cpu: MockMetricInstance = field(default_factory=MockMetricInstance)


class MockMetricManager:
    """Mock MetricManager for testing."""

    def __init__(self):
        self.datetimes = deque(["01/01/24 12:00:00", "01/01/24 12:00:01"])
        self.metrics = MockMetricInstances()
        self.metrics.system_cpu.test_metric.values = deque([10, 20])


class MockHastin:
    """Mock Hastin object for testing."""

    def __init__(
        self,
        host="localhost",
        port=5432,
        record_for_replay=False,
        replay_file=None,
        replay_dir=None,
    ):
        self.host = host
        self.port = port
        self.record_for_replay = record_for_replay
        self.replay_file = replay_file
        self.replay_dir = replay_dir

        # Mock state
        self.system_utilization = {"cpu_percent": 25.5, "memory_percent": 60.0}
        self.global_variables = {
            "server_version": "16.0",
            "max_connections": "100",
            "ssl_cert_file": "/path/to/cert",  # Should be filtered
            "password_encryption": "scram-sha-256",  # Should be filtered
            "shared_buffers": "128MB",
        }
        self.processlist_threads = {}
        self.replication_status = {"primary_host": "primary.example.com"}
        self.connection_stats = {"active": 5, "idle": 10}
        self.database_stats = {"xact_commit": 1000, "xact_rollback": 5}
        self.metric_manager = MockMetricManager()
        self.worker_processing_time = 0.1


class TestPostgreSQLReplayData:
    """Tests for the PostgreSQLReplayData dataclass."""

    def test_basic_instantiation(self):
        """Test basic dataclass instantiation."""
        data = PostgreSQLReplayData(
            timestamp="2024-01-01T12:00:00+00:00",
            system_utilization={"cpu": 25},
            global_variables={"version": "16.0"},
            processlist={},
        )
        assert data.timestamp == "2024-01-01T12:00:00+00:00"
        assert data.system_utilization == {"cpu": 25}
        assert data.global_variables == {"version": "16.0"}
        assert data.processlist == {}

    def test_default_values(self):
        """Test default factory values."""
        data = PostgreSQLReplayData(
            timestamp="2024-01-01T12:00:00+00:00",
            system_utilization={},
            global_variables={},
            processlist={},
        )
        assert data.replication_status == {}
        assert data.connection_stats == {}
        assert data.database_stats == {}
        assert data.metric_manager == {}

    def test_full_instantiation(self):
        """Test full instantiation with all fields."""
        data = PostgreSQLReplayData(
            timestamp="2024-01-01T12:00:00+00:00",
            system_utilization={"cpu": 25},
            global_variables={"version": "16.0"},
            processlist={"123": ProcesslistThread({"pid": 123, "user": "test"})},
            replication_status={"lag": 0},
            connection_stats={"active": 5},
            database_stats={"xact_commit": 1000},
            metric_manager={"datetimes": []},
        )
        assert "123" in data.processlist
        assert data.replication_status == {"lag": 0}
        assert data.connection_stats == {"active": 5}


class TestReplayManagerInit:
    """Tests for ReplayManager initialization."""

    def test_init_no_replay_no_record(self):
        """Test initialization without replay or recording."""
        hastin = MockHastin()
        manager = ReplayManager(hastin)

        assert manager.hastin == hastin
        assert manager.replay_file is None
        assert manager.replay_dir is None
        assert manager.current_replay_id == 0
        assert manager.replay_data == []
        assert manager.global_variable_changes == {}

    def test_init_with_replay_dir_only(self):
        """Test initialization with replay_dir but no recording enabled."""
        hastin = MockHastin(replay_dir="/tmp/replays")
        manager = ReplayManager(hastin)

        assert manager.replay_dir == "/tmp/replays"
        assert not hasattr(manager, "recording_file")

    def test_init_with_recording_enabled(self):
        """Test initialization with recording enabled."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hastin = MockHastin(
                host="testhost",
                port=5433,
                record_for_replay=True,
                replay_dir=tmpdir,
            )
            manager = ReplayManager(hastin)

            assert hasattr(manager, "recording_file")
            assert hasattr(manager, "compressor")
            assert hasattr(manager, "compressed_writer")

            expected_host_dir = os.path.join(tmpdir, "testhost_5433")
            assert os.path.isdir(expected_host_dir)

            assert manager.recording_file.endswith(".zst")
            assert "testhost_5433" in manager.recording_file

            manager.close()

    def test_init_with_default_port(self):
        """Test directory naming with default port (5432)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hastin = MockHastin(
                host="localhost",
                port=5432,  # Default port
                record_for_replay=True,
                replay_dir=tmpdir,
            )
            manager = ReplayManager(hastin)

            # Default port should not be included in directory name
            expected_host_dir = os.path.join(tmpdir, "localhost")
            assert os.path.isdir(expected_host_dir)

            manager.close()

    def test_host_name_sanitization(self):
        """Test host name sanitization for directory names."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hastin = MockHastin(
                host="192.168.1.100",  # Dots should be replaced
                port=5432,
                record_for_replay=True,
                replay_dir=tmpdir,
            )
            manager = ReplayManager(hastin)

            # Dots should be replaced with underscores
            expected_host_dir = os.path.join(tmpdir, "192_168_1_100")
            assert os.path.isdir(expected_host_dir)

            manager.close()


class TestVariableFiltering:
    """Tests for security-sensitive variable filtering."""

    def test_filter_ssl_variables(self):
        """Test that SSL-related variables are filtered."""
        hastin = MockHastin()
        manager = ReplayManager(hastin)

        variables = {
            "ssl_cert_file": "/path/to/cert",
            "ssl_key_file": "/path/to/key",
            "ssl_ca_file": "/path/to/ca",
            "server_version": "16.0",
        }

        filtered = manager._filter_variables(variables)

        assert "ssl_cert_file" not in filtered
        assert "ssl_key_file" not in filtered
        assert "ssl_ca_file" not in filtered
        assert "server_version" in filtered

    def test_filter_password_variables(self):
        """Test that password-related variables are filtered."""
        hastin = MockHastin()
        manager = ReplayManager(hastin)

        variables = {
            "password_encryption": "scram-sha-256",
            "pg_password": "secret",
            "my_password_setting": "value",
            "max_connections": "100",
        }

        filtered = manager._filter_variables(variables)

        assert "password_encryption" not in filtered
        assert "pg_password" not in filtered
        assert "my_password_setting" not in filtered
        assert "max_connections" in filtered

    def test_filter_key_variables(self):
        """Test that key-related variables are filtered."""
        hastin = MockHastin()
        manager = ReplayManager(hastin)

        variables = {
            "api_key": "secret123",
            "private_key_path": "/path/to/key",
            "server_version": "16.0",
        }

        filtered = manager._filter_variables(variables)

        assert "api_key" not in filtered
        assert "private_key_path" not in filtered
        assert "server_version" in filtered

    def test_case_insensitive_filtering(self):
        """Test that filtering is case-insensitive."""
        hastin = MockHastin()
        manager = ReplayManager(hastin)

        variables = {
            "SSL_CERT": "/path",
            "Password": "secret",
            "API_KEY": "key123",
            "normal_var": "value",
        }

        filtered = manager._filter_variables(variables)

        assert "SSL_CERT" not in filtered
        assert "Password" not in filtered
        assert "API_KEY" not in filtered
        assert "normal_var" in filtered

    def test_empty_variables(self):
        """Test filtering with empty dict."""
        hastin = MockHastin()
        manager = ReplayManager(hastin)

        filtered = manager._filter_variables({})
        assert filtered == {}


class TestProcesslistSerialization:
    """Tests for processlist thread serialization."""

    def test_serialize_empty_processlist(self):
        """Test serialization of empty processlist."""
        hastin = MockHastin()
        manager = ReplayManager(hastin)

        result = manager._serialize_processlist()
        assert result == {}

    def test_serialize_single_thread(self):
        """Test serialization of a single thread."""
        hastin = MockHastin()
        thread_data = {
            "pid": 12345,
            "user": "postgres",
            "database": "test_db",
            "host": "192.168.1.1",
            "state": "active",
            "query": "SELECT * FROM users",
            "time": 5,
        }
        hastin.processlist_threads = {
            "12345": ProcesslistThread(thread_data)
        }

        manager = ReplayManager(hastin)
        result = manager._serialize_processlist()

        assert "12345" in result
        assert result["12345"]["pid"] == 12345
        assert result["12345"]["user"] == "postgres"
        assert result["12345"]["database"] == "test_db"

    def test_serialize_multiple_threads(self):
        """Test serialization of multiple threads."""
        hastin = MockHastin()
        hastin.processlist_threads = {
            "100": ProcesslistThread({"pid": 100, "user": "user1", "state": "active"}),
            "101": ProcesslistThread({"pid": 101, "user": "user2", "state": "idle"}),
            "102": ProcesslistThread({"pid": 102, "user": "user3", "state": "idle_in_transaction"}),
        }

        manager = ReplayManager(hastin)
        result = manager._serialize_processlist()

        assert len(result) == 3
        assert "100" in result
        assert "101" in result
        assert "102" in result


class TestMetricsSerialization:
    """Tests for metric manager serialization."""

    def test_serialize_metrics_basic(self):
        """Test basic metrics serialization."""
        hastin = MockHastin()
        manager = ReplayManager(hastin)

        result = manager._serialize_metrics()

        # Should include datetimes
        assert "datetimes" in result
        assert list(result["datetimes"]) == ["01/01/24 12:00:00", "01/01/24 12:00:01"]

    def test_serialize_metrics_with_values(self):
        """Test metrics serialization includes metric values."""
        hastin = MockHastin()
        manager = ReplayManager(hastin)

        result = manager._serialize_metrics()

        # Should include metric instances with values
        assert "system_cpu" in result
        assert "test_metric" in result["system_cpu"]


class TestStateCapture:
    """Tests for capturing state during recording."""

    def test_capture_state_when_not_recording(self):
        """Test that capture_state does nothing when not recording."""
        hastin = MockHastin(record_for_replay=False)
        manager = ReplayManager(hastin)

        # Should not raise, should do nothing
        manager.capture_state()

        assert not hasattr(manager, "recording_file")

    def test_capture_state_when_recording(self):
        """Test state capture when recording is enabled."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hastin = MockHastin(
                record_for_replay=True,
                replay_dir=tmpdir,
            )
            manager = ReplayManager(hastin)

            # Capture state
            manager.capture_state()

            # Close to flush
            manager.close()

            # Verify file was written
            assert os.path.exists(manager.recording_file)
            assert os.path.getsize(manager.recording_file) > 0

    def test_captured_state_structure(self):
        """Test the structure of captured state."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hastin = MockHastin(
                record_for_replay=True,
                replay_dir=tmpdir,
            )
            hastin.processlist_threads = {
                "123": ProcesslistThread({"pid": 123, "user": "test"})
            }

            manager = ReplayManager(hastin)
            manager.capture_state()
            manager.close()

            # Read and decompress the file using stream_reader
            decompressor = zstd.ZstdDecompressor()
            with open(manager.recording_file, "rb") as f:
                with decompressor.stream_reader(f) as reader:
                    data = reader.read()

            # Parse the JSON
            event = orjson.loads(data.strip())

            # Verify structure
            assert "timestamp" in event
            assert "system_utilization" in event
            assert "global_variables" in event
            assert "processlist" in event
            assert "replication_status" in event
            assert "connection_stats" in event
            assert "database_stats" in event
            assert "metric_manager" in event
            assert "replay_polling_latency" in event

            # Verify filtered variables
            assert "ssl_cert_file" not in event["global_variables"]
            assert "password_encryption" not in event["global_variables"]
            assert "server_version" in event["global_variables"]

    def test_multiple_captures(self):
        """Test capturing multiple states creates multiple lines."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hastin = MockHastin(
                record_for_replay=True,
                replay_dir=tmpdir,
            )
            manager = ReplayManager(hastin)

            # Capture multiple states
            for _ in range(5):
                manager.capture_state()

            manager.close()

            # Read and decompress using stream_reader
            decompressor = zstd.ZstdDecompressor()
            with open(manager.recording_file, "rb") as f:
                with decompressor.stream_reader(f) as reader:
                    data = reader.read().decode("utf-8")

            # Count lines
            lines = [l for l in data.strip().split("\n") if l]
            assert len(lines) == 5


class TestZstandardCompression:
    """Tests for Zstandard compression functionality."""

    def test_compression_creates_valid_zst_file(self):
        """Test that compression creates a valid .zst file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hastin = MockHastin(
                record_for_replay=True,
                replay_dir=tmpdir,
            )
            manager = ReplayManager(hastin)
            manager.capture_state()
            manager.close()

            # File should end with .zst
            assert manager.recording_file.endswith(".zst")

            # Should be decompressible using stream_reader
            decompressor = zstd.ZstdDecompressor()
            with open(manager.recording_file, "rb") as f:
                with decompressor.stream_reader(f) as reader:
                    data = reader.read()

            assert len(data) > 0

    def test_compression_level(self):
        """Test that compression is configured correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hastin = MockHastin(
                record_for_replay=True,
                replay_dir=tmpdir,
            )
            manager = ReplayManager(hastin)

            # Verify compressor exists (level is set to 3 in _setup_recording)
            assert manager.compressor is not None

            manager.close()


class TestReplayLoading:
    """Tests for loading replay files."""

    def test_load_nonexistent_file(self):
        """Test loading a file that doesn't exist."""
        hastin = MockHastin(replay_file="/nonexistent/file.zst")

        # Should not raise, should log error and have empty replay_data
        manager = ReplayManager(hastin)
        assert manager.replay_data == []

    def test_load_valid_replay_file(self):
        """Test loading a valid replay file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # First, create a replay file
            hastin_record = MockHastin(
                record_for_replay=True,
                replay_dir=tmpdir,
            )
            hastin_record.processlist_threads = {
                "123": ProcesslistThread({"pid": 123, "user": "test"})
            }

            manager_record = ReplayManager(hastin_record)
            manager_record.capture_state()
            manager_record.capture_state()
            manager_record.close()

            replay_file = manager_record.recording_file

            # Now load it
            hastin_replay = MockHastin(replay_file=replay_file)
            manager_replay = ReplayManager(hastin_replay)

            assert len(manager_replay.replay_data) == 2
            assert manager_replay.current_replay_id == 0

    def test_load_preserves_data_structure(self):
        """Test that loading preserves the data structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create replay with specific data
            hastin_record = MockHastin(
                record_for_replay=True,
                replay_dir=tmpdir,
            )
            hastin_record.system_utilization = {"cpu": 50, "memory": 75}
            hastin_record.processlist_threads = {
                "999": ProcesslistThread({
                    "pid": 999,
                    "user": "admin",
                    "database": "production",
                    "query": "UPDATE users SET active = true",
                })
            }

            manager_record = ReplayManager(hastin_record)
            manager_record.capture_state()
            manager_record.close()

            # Load and verify
            hastin_replay = MockHastin(replay_file=manager_record.recording_file)
            manager_replay = ReplayManager(hastin_replay)

            event = manager_replay.replay_data[0]
            assert event["system_utilization"]["cpu"] == 50
            assert "999" in event["processlist"]


class TestPlayback:
    """Tests for replay playback functionality."""

    def test_get_next_refresh_interval_empty(self):
        """Test get_next_refresh_interval with no data."""
        hastin = MockHastin()
        manager = ReplayManager(hastin)

        result = manager.get_next_refresh_interval()
        assert result is None

    def test_get_next_refresh_interval_returns_data(self):
        """Test get_next_refresh_interval returns PostgreSQLReplayData."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create replay file
            hastin_record = MockHastin(
                record_for_replay=True,
                replay_dir=tmpdir,
            )
            manager_record = ReplayManager(hastin_record)
            manager_record.capture_state()
            manager_record.close()

            # Load and play
            hastin_replay = MockHastin(replay_file=manager_record.recording_file)
            manager_replay = ReplayManager(hastin_replay)

            result = manager_replay.get_next_refresh_interval()

            assert isinstance(result, PostgreSQLReplayData)
            assert result.timestamp is not None
            assert isinstance(result.system_utilization, dict)
            assert isinstance(result.processlist, dict)

    def test_get_next_refresh_interval_increments_id(self):
        """Test that get_next_refresh_interval increments current_replay_id."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create replay file with multiple events
            hastin_record = MockHastin(
                record_for_replay=True,
                replay_dir=tmpdir,
            )
            manager_record = ReplayManager(hastin_record)
            for _ in range(5):
                manager_record.capture_state()
            manager_record.close()

            # Load
            hastin_replay = MockHastin(replay_file=manager_record.recording_file)
            manager_replay = ReplayManager(hastin_replay)

            assert manager_replay.current_replay_id == 0

            manager_replay.get_next_refresh_interval()
            assert manager_replay.current_replay_id == 1

            manager_replay.get_next_refresh_interval()
            assert manager_replay.current_replay_id == 2

    def test_get_next_refresh_interval_end_of_replay(self):
        """Test behavior at end of replay."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create replay file with 2 events
            hastin_record = MockHastin(
                record_for_replay=True,
                replay_dir=tmpdir,
            )
            manager_record = ReplayManager(hastin_record)
            manager_record.capture_state()
            manager_record.capture_state()
            manager_record.close()

            # Load
            hastin_replay = MockHastin(replay_file=manager_record.recording_file)
            manager_replay = ReplayManager(hastin_replay)

            # Get all events
            result1 = manager_replay.get_next_refresh_interval()
            result2 = manager_replay.get_next_refresh_interval()
            result3 = manager_replay.get_next_refresh_interval()

            assert result1 is not None
            assert result2 is not None
            assert result3 is None  # End of replay

    def test_processlist_deserialization(self):
        """Test that processlist is properly deserialized to ProcesslistThread objects."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create replay with processlist
            hastin_record = MockHastin(
                record_for_replay=True,
                replay_dir=tmpdir,
            )
            hastin_record.processlist_threads = {
                "123": ProcesslistThread({
                    "pid": 123,
                    "user": "testuser",
                    "database": "testdb",
                    "state": "active",
                    "query": "SELECT 1",
                })
            }

            manager_record = ReplayManager(hastin_record)
            manager_record.capture_state()
            manager_record.close()

            # Load and verify
            hastin_replay = MockHastin(replay_file=manager_record.recording_file)
            manager_replay = ReplayManager(hastin_replay)

            result = manager_replay.get_next_refresh_interval()

            assert "123" in result.processlist
            thread = result.processlist["123"]
            assert isinstance(thread, ProcesslistThread)
            assert thread.pid == 123
            assert thread.user == "testuser"


class TestGlobalVariableTracking:
    """Tests for global variable change tracking."""

    def test_capture_global_variable_change_when_recording(self):
        """Test capturing variable changes during recording."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hastin = MockHastin(
                record_for_replay=True,
                replay_dir=tmpdir,
            )
            manager = ReplayManager(hastin)

            manager.capture_global_variable_change(
                "max_connections", "100", "200"
            )

            assert 0 in manager.global_variable_changes
            assert len(manager.global_variable_changes[0]) == 1
            change = manager.global_variable_changes[0][0]
            assert change["variable"] == "max_connections"
            assert change["old_value"] == "100"
            assert change["new_value"] == "200"

            manager.close()

    def test_capture_global_variable_change_when_not_recording(self):
        """Test that changes are not captured when not recording."""
        hastin = MockHastin(record_for_replay=False)
        manager = ReplayManager(hastin)

        manager.capture_global_variable_change(
            "max_connections", "100", "200"
        )

        assert manager.global_variable_changes == {}

    def test_multiple_variable_changes(self):
        """Test capturing multiple variable changes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hastin = MockHastin(
                record_for_replay=True,
                replay_dir=tmpdir,
            )
            manager = ReplayManager(hastin)

            manager.capture_global_variable_change("var1", "a", "b")
            manager.capture_global_variable_change("var2", "c", "d")

            assert len(manager.global_variable_changes[0]) == 2

            manager.close()


class TestFileSizeTracking:
    """Tests for recording file size tracking."""

    def test_get_file_size_no_recording(self):
        """Test file size when not recording."""
        hastin = MockHastin()
        manager = ReplayManager(hastin)

        assert manager.get_file_size() == 0

    def test_get_file_size_during_recording(self):
        """Test file size during active recording."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hastin = MockHastin(
                record_for_replay=True,
                replay_dir=tmpdir,
            )
            manager = ReplayManager(hastin)

            # Capture some state
            for _ in range(10):
                manager.capture_state()

            # Flush the buffer
            manager.compressed_writer.flush()

            size = manager.get_file_size()
            assert size > 0

            manager.close()

    def test_get_file_size_after_close(self):
        """Test file size after closing recording."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hastin = MockHastin(
                record_for_replay=True,
                replay_dir=tmpdir,
            )
            manager = ReplayManager(hastin)
            manager.capture_state()
            manager.close()

            size = manager.get_file_size()
            assert size > 0


class TestCloseCleanup:
    """Tests for proper cleanup on close."""

    def test_close_without_recording(self):
        """Test close when not recording."""
        hastin = MockHastin()
        manager = ReplayManager(hastin)

        # Should not raise
        manager.close()

    def test_close_with_recording(self):
        """Test close properly closes handles when recording."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hastin = MockHastin(
                record_for_replay=True,
                replay_dir=tmpdir,
            )
            manager = ReplayManager(hastin)
            manager.capture_state()

            recording_file = manager.recording_file

            manager.close()

            # File should still exist and be readable
            assert os.path.exists(recording_file)

            # Should be properly closed (can open for reading)
            with open(recording_file, "rb") as f:
                data = f.read()
            assert len(data) > 0

    def test_double_close(self):
        """Test that double close doesn't raise."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hastin = MockHastin(
                record_for_replay=True,
                replay_dir=tmpdir,
            )
            manager = ReplayManager(hastin)
            manager.capture_state()

            manager.close()
            manager.close()


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_processlist(self):
        """Test handling of empty processlist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hastin = MockHastin(
                record_for_replay=True,
                replay_dir=tmpdir,
            )
            hastin.processlist_threads = {}

            manager = ReplayManager(hastin)
            manager.capture_state()
            manager.close()

            # Load and verify
            hastin_replay = MockHastin(replay_file=manager.recording_file)
            manager_replay = ReplayManager(hastin_replay)
            result = manager_replay.get_next_refresh_interval()

            assert result.processlist == {}

    def test_special_characters_in_data(self):
        """Test handling of special characters in data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hastin = MockHastin(
                record_for_replay=True,
                replay_dir=tmpdir,
            )
            hastin.processlist_threads = {
                "123": ProcesslistThread({
                    "pid": 123,
                    "user": "test",
                    "query": "SELECT * FROM users WHERE name = 'O\\'Brien' AND data = '{\"key\": \"value\"}'",
                })
            }

            manager = ReplayManager(hastin)
            manager.capture_state()
            manager.close()

            # Load and verify
            hastin_replay = MockHastin(replay_file=manager.recording_file)
            manager_replay = ReplayManager(hastin_replay)
            result = manager_replay.get_next_refresh_interval()

            assert "123" in result.processlist

    def test_unicode_in_data(self):
        """Test handling of unicode characters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hastin = MockHastin(
                record_for_replay=True,
                replay_dir=tmpdir,
            )
            hastin.processlist_threads = {
                "123": ProcesslistThread({
                    "pid": 123,
                    "user": "utilisateur",
                    "query": "SELECT * FROM users WHERE name = '\u4e2d\u6587' AND emoji = '\U0001f600'",
                })
            }

            manager = ReplayManager(hastin)
            manager.capture_state()
            manager.close()

            # Load and verify
            hastin_replay = MockHastin(replay_file=manager.recording_file)
            manager_replay = ReplayManager(hastin_replay)
            result = manager_replay.get_next_refresh_interval()

            assert "123" in result.processlist

    def test_large_processlist(self):
        """Test handling of large processlist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hastin = MockHastin(
                record_for_replay=True,
                replay_dir=tmpdir,
            )

            # Create 100 threads
            hastin.processlist_threads = {
                str(i): ProcesslistThread({
                    "pid": i,
                    "user": f"user_{i}",
                    "database": "testdb",
                    "query": f"SELECT {i} FROM table_{i}",
                })
                for i in range(100)
            }

            manager = ReplayManager(hastin)
            manager.capture_state()
            manager.close()

            # Load and verify
            hastin_replay = MockHastin(replay_file=manager.recording_file)
            manager_replay = ReplayManager(hastin_replay)
            result = manager_replay.get_next_refresh_interval()

            assert len(result.processlist) == 100

    def test_very_long_query(self):
        """Test handling of very long queries."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hastin = MockHastin(
                record_for_replay=True,
                replay_dir=tmpdir,
            )

            # Create a very long query
            long_query = "SELECT " + ", ".join([f"col_{i}" for i in range(1000)]) + " FROM big_table"

            hastin.processlist_threads = {
                "123": ProcesslistThread({
                    "pid": 123,
                    "user": "test",
                    "query": long_query,
                })
            }

            manager = ReplayManager(hastin)
            manager.capture_state()
            manager.close()

            # Verify file was created
            assert os.path.exists(manager.recording_file)
            assert os.path.getsize(manager.recording_file) > 0


class TestRoundTrip:
    """Integration tests for full record/replay round-trips."""

    def test_full_round_trip(self):
        """Test complete record and replay cycle."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Record phase
            hastin_record = MockHastin(
                host="test.example.com",
                port=5433,
                record_for_replay=True,
                replay_dir=tmpdir,
            )
            hastin_record.system_utilization = {"cpu": 45, "memory": 70}
            hastin_record.global_variables = {
                "server_version": "16.1",
                "max_connections": "200",
            }
            hastin_record.processlist_threads = {
                "1001": ProcesslistThread({
                    "pid": 1001,
                    "user": "app_user",
                    "database": "myapp",
                    "state": "active",
                    "query": "INSERT INTO logs VALUES ($1, $2)",
                    "time": 1,
                })
            }
            hastin_record.connection_stats = {"active": 10, "idle": 25}
            hastin_record.database_stats = {"xact_commit": 50000, "xact_rollback": 10}

            manager_record = ReplayManager(hastin_record)
            manager_record.capture_state()
            manager_record.close()

            # Replay phase
            hastin_replay = MockHastin(replay_file=manager_record.recording_file)
            manager_replay = ReplayManager(hastin_replay)

            result = manager_replay.get_next_refresh_interval()

            # Verify all data matches
            assert result.system_utilization["cpu"] == 45
            assert result.system_utilization["memory"] == 70
            assert result.global_variables["server_version"] == "16.1"
            assert result.global_variables["max_connections"] == "200"
            assert "1001" in result.processlist
            thread = result.processlist["1001"]
            assert thread.pid == 1001
            assert thread.user == "app_user"
            assert result.connection_stats["active"] == 10
            assert result.database_stats["xact_commit"] == 50000

    def test_multiple_events_round_trip(self):
        """Test recording and replaying multiple events."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Record multiple events with changing data
            hastin_record = MockHastin(
                record_for_replay=True,
                replay_dir=tmpdir,
            )

            manager_record = ReplayManager(hastin_record)

            for i in range(10):
                hastin_record.system_utilization = {"cpu": i * 10}
                hastin_record.processlist_threads = {
                    str(i): ProcesslistThread({"pid": i, "user": f"user_{i}"})
                }
                manager_record.capture_state()

            manager_record.close()

            # Replay and verify sequence
            hastin_replay = MockHastin(replay_file=manager_record.recording_file)
            manager_replay = ReplayManager(hastin_replay)

            for i in range(10):
                result = manager_replay.get_next_refresh_interval()
                assert result is not None
                assert result.system_utilization["cpu"] == i * 10
                assert str(i) in result.processlist

            # Should be end of replay
            assert manager_replay.get_next_refresh_interval() is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
