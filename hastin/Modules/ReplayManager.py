"""Replay manager for Hastin PostgreSQL monitoring - handles recording and playback."""

import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import orjson
import zstandard as zstd
from loguru import logger

from hastin.DataTypes import ProcesslistThread
from hastin.Modules.MetricManager import MetricData

if TYPE_CHECKING:
    from hastin.Hastin import Hastin


@dataclass
class PostgreSQLReplayData:
    """Data structure for PostgreSQL replay events."""

    timestamp: str
    system_utilization: dict
    global_variables: dict
    processlist: dict
    replication_status: dict = field(default_factory=dict)
    connection_stats: dict = field(default_factory=dict)
    database_stats: dict = field(default_factory=dict)
    metric_manager: dict = field(default_factory=dict)


class ReplayManager:
    """Manages recording and playback of Hastin monitoring data."""

    def __init__(self, hastin: "Hastin"):
        self.hastin = hastin
        self.replay_file = hastin.replay_file
        self.replay_dir = hastin.replay_dir

        self.global_variable_changes = {}
        self.current_replay_id = 0
        self.replay_data = []

        if self.replay_file:
            self._load_replay_file()

        if hastin.record_for_replay and self.replay_dir:
            self._setup_recording()

    def _setup_recording(self):
        """Set up the recording file."""
        host_dir = self.hastin.host.replace(".", "_")[:30]
        if self.hastin.port != 5432:
            host_dir += f"_{self.hastin.port}"

        replay_dir_path = os.path.join(self.replay_dir, host_dir)
        os.makedirs(replay_dir_path, exist_ok=True)

        timestamp = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
        self.recording_file = os.path.join(replay_dir_path, f"{timestamp}.zst")
        self.compressor = zstd.ZstdCompressor(level=3)
        self.recording_handle = open(self.recording_file, "wb")
        self.compressed_writer = self.compressor.stream_writer(self.recording_handle)

        logger.info(f"Recording to: {self.recording_file}")

    def _load_replay_file(self):
        """Load replay data from a compressed file."""
        try:
            decompressor = zstd.ZstdDecompressor()
            with open(self.replay_file, "rb") as f:
                # Use stream_reader for compatibility with streaming compression
                with decompressor.stream_reader(f) as reader:
                    decompressed_data = reader.read()

            for line in decompressed_data.decode("utf-8").strip().split("\n"):
                if line:
                    self.replay_data.append(orjson.loads(line))

            logger.info(f"Loaded {len(self.replay_data)} replay events from {self.replay_file}")
        except Exception as e:
            logger.error(f"Failed to load replay file: {e}")
            self.replay_data = []

    def capture_state(self):
        """Capture the current state for recording."""
        if not self.hastin.record_for_replay:
            return

        data = {
            "timestamp": datetime.now(tz=UTC).isoformat(),
            "system_utilization": self.hastin.system_utilization,
            "global_variables": self._filter_variables(self.hastin.global_variables),
            "processlist": self._serialize_processlist(),
            "replication_status": self.hastin.replication_status,
            "connection_stats": self.hastin.connection_stats,
            "database_stats": self.hastin.database_stats,
            "metric_manager": self._serialize_metrics(),
            "replay_polling_latency": self.hastin.worker_processing_time,
        }

        try:
            line = orjson.dumps(data) + b"\n"
            self.compressed_writer.write(line)
        except Exception as e:
            logger.error(f"Failed to write replay data: {e}")

    def _filter_variables(self, variables: dict) -> dict:
        """Filter out variables that shouldn't be recorded."""
        exclude_patterns = ["ssl", "password", "key"]
        return {k: v for k, v in variables.items() if not any(p in k.lower() for p in exclude_patterns)}

    def _serialize_processlist(self) -> dict:
        """Serialize processlist threads to a dict."""
        result = {}
        for thread_id, thread in self.hastin.processlist_threads.items():
            result[thread_id] = thread.thread_data
        return result

    def _serialize_metrics(self) -> dict:
        """Serialize metric manager data."""
        # Convert deques to lists for JSON serialization
        metrics_data = {"datetimes": list(self.hastin.metric_manager.datetimes)}

        for metric_name, metric_instance in self.hastin.metric_manager.metrics.__dict__.items():
            metric_dict = {}
            for attr_name, attr in metric_instance.__dict__.items():
                # Only serialize MetricData objects
                if isinstance(attr, MetricData):
                    # Convert deque to list for JSON serialization
                    metric_dict[attr_name] = list(attr.values)
            if metric_dict:
                metrics_data[metric_name] = metric_dict

        return metrics_data

    def get_next_refresh_interval(self) -> PostgreSQLReplayData | None:
        """Get the next replay event."""
        if self.current_replay_id >= len(self.replay_data):
            return None

        data = self.replay_data[self.current_replay_id]
        self.current_replay_id += 1

        processlist = {}
        for thread_id, thread_data in data.get("processlist", {}).items():
            processlist[thread_id] = ProcesslistThread(thread_data)

        return PostgreSQLReplayData(
            timestamp=data.get("timestamp", ""),
            system_utilization=data.get("system_utilization", {}),
            global_variables=data.get("global_variables", {}),
            processlist=processlist,
            replication_status=data.get("replication_status", {}),
            connection_stats=data.get("connection_stats", {}),
            database_stats=data.get("database_stats", {}),
            metric_manager=data.get("metric_manager", {}),
        )

    def fetch_global_variable_changes_for_current_replay_id(self):
        """Fetch any global variable changes for the current replay position."""
        pass

    def capture_global_variable_change(self, variable: str, old_value, new_value):
        """Record a global variable change."""
        if self.hastin.record_for_replay:
            if self.current_replay_id not in self.global_variable_changes:
                self.global_variable_changes[self.current_replay_id] = []
            self.global_variable_changes[self.current_replay_id].append(
                {
                    "variable": variable,
                    "old_value": old_value,
                    "new_value": new_value,
                }
            )

    def close(self):
        """Close the recording file."""
        if hasattr(self, "compressed_writer"):
            self.compressed_writer.close()
        if hasattr(self, "recording_handle"):
            self.recording_handle.close()

    def get_file_size(self) -> int:
        """Get the size of the recording file."""
        if hasattr(self, "recording_file") and os.path.exists(self.recording_file):
            return os.path.getsize(self.recording_file)
        return 0
