"""System metrics collection providers for Hastin PostgreSQL monitoring dashboard.

Provides abstraction for collecting system metrics from multiple sources:
- Local: psutil (localhost only)
- Extension: system_stats PostgreSQL extension
- AWS: CloudWatch API (RDS/Aurora)
- GCP: Cloud Monitoring API (Cloud SQL/AlloyDB)
- Azure: Azure Monitor API
- NoOp: Disabled/fallback
"""

from __future__ import annotations

import socket
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

import psycopg
from loguru import logger

if TYPE_CHECKING:
    from hastin.Modules.PostgreSQL import Database


@dataclass
class SystemMetrics:
    """System metrics data structure."""

    cpu_percent: float | None = None
    cpu_count: int | None = None
    cpu_load_avg: tuple[float, float, float] | None = None
    memory_total: int | None = None
    memory_used: int | None = None
    swap_total: int | None = None
    swap_used: int | None = None
    disk_read_iops: int | None = None
    disk_write_iops: int | None = None
    network_bytes_sent: int | None = None
    network_bytes_recv: int | None = None
    uptime_seconds: int | None = None
    provider_name: str = "unknown"


class SystemMetricsProvider(ABC):
    """Abstract base class for system metrics providers."""

    @abstractmethod
    def is_available(self) -> bool:
        """Check if this provider is available and can collect metrics."""
        ...

    @abstractmethod
    def collect(self) -> SystemMetrics | None:
        """Collect and return system metrics, or None if collection fails."""
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the provider name for display purposes."""
        ...


class LocalSystemMetricsProvider(SystemMetricsProvider):
    """Collect system metrics from local system using psutil."""

    def __init__(self) -> None:
        """Initialize local system metrics provider."""
        self._psutil = None

    def is_available(self) -> bool:
        """Check if psutil is available."""
        try:
            import psutil

            self._psutil = psutil
            return True
        except ImportError:
            return False

    def collect(self) -> SystemMetrics | None:
        """Collect metrics using psutil."""
        if not self._psutil:
            return None

        try:
            virtual_memory = self._psutil.virtual_memory()
            swap_memory = self._psutil.swap_memory()
            network_io = self._psutil.net_io_counters()
            disk_io = self._psutil.disk_io_counters()

            metrics = SystemMetrics(
                cpu_percent=self._psutil.cpu_percent(interval=0),
                cpu_count=self._psutil.cpu_count(logical=True),
                memory_total=virtual_memory.total,
                memory_used=virtual_memory.used,
                swap_total=swap_memory.total,
                swap_used=swap_memory.used,
                network_bytes_sent=network_io.bytes_sent if network_io else None,
                network_bytes_recv=network_io.bytes_recv if network_io else None,
                disk_read_iops=disk_io.read_count if disk_io else None,
                disk_write_iops=disk_io.write_count if disk_io else None,
                uptime_seconds=int(time.time() - self._psutil.boot_time()),
                provider_name=self.name,
            )

            # Try to get load average (not available on Windows)
            try:
                metrics.cpu_load_avg = self._psutil.getloadavg()
            except AttributeError:
                pass

            return metrics
        except Exception as e:
            logger.warning(f"Local system metrics collection failed: {e}")
            return None

    @property
    def name(self) -> str:
        """Return provider name."""
        return "local"


class ExtensionSystemMetricsProvider(SystemMetricsProvider):
    """Collect system metrics from PostgreSQL system_stats extension."""

    def __init__(self, db_connection: Database) -> None:
        """Initialize extension-based metrics provider.

        Args:
            db_connection: PostgreSQL database connection
        """
        self._db_connection = db_connection
        self._available: bool | None = None

    def is_available(self) -> bool:
        """Check if system_stats extension is installed."""
        if self._available is not None:
            return self._available

        try:
            self._db_connection.execute(
                "SELECT 1 FROM pg_extension WHERE extname = 'system_stats'"
            )
            result = self._db_connection.fetchone()
            self._available = result is not None
            return self._available
        except psycopg.Error as e:
            logger.debug(f"system_stats extension check failed: {e}")
            self._available = False
            return False

    def collect(self) -> SystemMetrics | None:
        """Collect metrics from system_stats extension."""
        if not self.is_available():
            return None

        try:
            metrics = SystemMetrics(provider_name=self.name)

            # Get CPU usage info
            try:
                self._db_connection.execute("SELECT * FROM pg_sys_cpu_usage_info()")
                cpu_info = self._db_connection.fetchone()
                if cpu_info:
                    # Calculate CPU percent from idle (100 - idle = used)
                    idle = cpu_info.get("idle_mode_percent", 100)
                    if idle is not None:
                        metrics.cpu_percent = 100.0 - float(idle)
            except psycopg.Error as e:
                logger.debug(f"pg_sys_cpu_usage_info() failed: {e}")

            # Get CPU count
            try:
                self._db_connection.execute("SELECT COUNT(*) as cpu_count FROM pg_sys_cpu_info()")
                cpu_count_info = self._db_connection.fetchone()
                if cpu_count_info:
                    metrics.cpu_count = cpu_count_info.get("cpu_count")
            except psycopg.Error as e:
                logger.debug(f"pg_sys_cpu_info() failed: {e}")

            # Get memory info
            try:
                self._db_connection.execute("SELECT * FROM pg_sys_memory_info()")
                memory_info = self._db_connection.fetchone()
                if memory_info:
                    metrics.memory_total = memory_info.get("total_memory")
                    metrics.memory_used = memory_info.get("used_memory")
                    metrics.swap_total = memory_info.get("swap_total")
                    metrics.swap_used = memory_info.get("swap_used")
            except psycopg.Error as e:
                logger.debug(f"pg_sys_memory_info() failed: {e}")

            # Get load average
            try:
                self._db_connection.execute("SELECT * FROM pg_sys_load_avg_info()")
                load_info = self._db_connection.fetchone()
                if load_info:
                    metrics.cpu_load_avg = (
                        float(load_info.get("load_avg_one_minute", 0) or 0),
                        float(load_info.get("load_avg_five_minutes", 0) or 0),
                        float(load_info.get("load_avg_fifteen_minutes", 0) or 0),
                    )
            except psycopg.Error as e:
                logger.debug(f"pg_sys_load_avg_info() failed: {e}")

            # Get IO analysis info (for disk IOPS)
            try:
                self._db_connection.execute(
                    "SELECT SUM(total_reads)::bigint as reads, SUM(total_writes)::bigint as writes "
                    "FROM pg_sys_io_analysis_info()"
                )
                io_info = self._db_connection.fetchone()
                if io_info:
                    metrics.disk_read_iops = int(io_info.get("reads") or 0)
                    metrics.disk_write_iops = int(io_info.get("writes") or 0)
            except psycopg.Error as e:
                logger.debug(f"pg_sys_io_analysis_info() failed: {e}")

            # Get network info
            try:
                self._db_connection.execute(
                    "SELECT SUM(rx_bytes)::bigint as rx, SUM(tx_bytes)::bigint as tx "
                    "FROM pg_sys_network_info()"
                )
                net_info = self._db_connection.fetchone()
                if net_info:
                    metrics.network_bytes_recv = int(net_info.get("rx") or 0)
                    metrics.network_bytes_sent = int(net_info.get("tx") or 0)
            except psycopg.Error as e:
                logger.debug(f"pg_sys_network_info() failed: {e}")

            # Get OS info for uptime
            try:
                self._db_connection.execute("SELECT os_up_since_seconds::bigint FROM pg_sys_os_info()")
                os_info = self._db_connection.fetchone()
                if os_info:
                    uptime = os_info.get("os_up_since_seconds")
                    metrics.uptime_seconds = int(uptime) if uptime else None
            except psycopg.Error as e:
                logger.debug(f"pg_sys_os_info() failed: {e}")

            return metrics
        except psycopg.Error as e:
            logger.warning(f"Extension system metrics collection failed: {e}")
            return None

    @property
    def name(self) -> str:
        """Return provider name."""
        return "extension"


class AWSSystemMetricsProvider(SystemMetricsProvider):
    """Collect system metrics from AWS CloudWatch for RDS/Aurora instances."""

    def __init__(
        self,
        region: str | None = None,
        db_identifier: str | None = None,
    ) -> None:
        """Initialize AWS CloudWatch metrics provider.

        Args:
            region: AWS region (auto-detected if not provided)
            db_identifier: RDS/Aurora DB instance identifier
        """
        self._region = region
        self._db_identifier = db_identifier
        self._client = None
        self._available: bool | None = None

    def is_available(self) -> bool:
        """Check if boto3 is available and credentials work."""
        if self._available is not None:
            return self._available

        try:
            import boto3

            # Try to create a CloudWatch client
            self._client = boto3.client(
                "cloudwatch",
                region_name=self._region,
            )
            # Test with a simple API call
            self._client.list_metrics(Limit=1)
            self._available = True
            return True
        except ImportError:
            logger.debug("boto3 not installed - AWS provider unavailable")
            self._available = False
            return False
        except Exception as e:
            logger.debug(f"AWS CloudWatch client creation failed: {e}")
            self._available = False
            return False

    def collect(self) -> SystemMetrics | None:
        """Collect metrics from CloudWatch."""
        if not self._client or not self._db_identifier:
            return None

        try:
            from datetime import UTC, datetime, timedelta

            end_time = datetime.now(UTC)
            start_time = end_time - timedelta(minutes=5)

            metrics = SystemMetrics(provider_name=self.name)

            # Define metrics to collect
            cloudwatch_metrics = [
                ("CPUUtilization", "cpu_percent"),
                ("FreeableMemory", "memory_free"),
                ("ReadIOPS", "disk_read_iops"),
                ("WriteIOPS", "disk_write_iops"),
                ("NetworkReceiveThroughput", "network_bytes_recv"),
                ("NetworkTransmitThroughput", "network_bytes_sent"),
            ]

            for metric_name, attr_name in cloudwatch_metrics:
                try:
                    response = self._client.get_metric_statistics(
                        Namespace="AWS/RDS",
                        MetricName=metric_name,
                        Dimensions=[
                            {"Name": "DBInstanceIdentifier", "Value": self._db_identifier}
                        ],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=60,
                        Statistics=["Average"],
                    )

                    datapoints = response.get("Datapoints", [])
                    if datapoints:
                        latest = max(datapoints, key=lambda x: x["Timestamp"])
                        value = latest.get("Average")

                        if attr_name == "cpu_percent":
                            metrics.cpu_percent = value
                        elif attr_name == "memory_free":
                            pass
                        elif attr_name == "disk_read_iops":
                            metrics.disk_read_iops = int(value) if value else None
                        elif attr_name == "disk_write_iops":
                            metrics.disk_write_iops = int(value) if value else None
                        elif attr_name == "network_bytes_recv":
                            metrics.network_bytes_recv = int(value) if value else None
                        elif attr_name == "network_bytes_sent":
                            metrics.network_bytes_sent = int(value) if value else None
                except Exception as e:
                    logger.debug(f"Failed to get CloudWatch metric {metric_name}: {e}")

            return metrics
        except Exception as e:
            logger.warning(f"AWS CloudWatch metrics collection failed: {e}")
            return None

    @property
    def name(self) -> str:
        """Return provider name."""
        return "aws"


class GCPSystemMetricsProvider(SystemMetricsProvider):
    """Collect system metrics from GCP Cloud Monitoring for Cloud SQL/AlloyDB."""

    def __init__(
        self,
        project_id: str | None = None,
        instance_id: str | None = None,
    ) -> None:
        """Initialize GCP Cloud Monitoring metrics provider.

        Args:
            project_id: GCP project ID
            instance_id: Cloud SQL or AlloyDB instance ID
        """
        self._project_id = project_id
        self._instance_id = instance_id
        self._client = None
        self._available: bool | None = None

    def is_available(self) -> bool:
        """Check if google-cloud-monitoring is available and credentials work."""
        if self._available is not None:
            return self._available

        try:
            from google.cloud import monitoring_v3

            self._client = monitoring_v3.MetricServiceClient()
            # Test with project name
            if self._project_id:
                # Just creating the client is enough to validate credentials
                self._available = True
                return True
            self._available = False
            return False
        except ImportError:
            logger.debug("google-cloud-monitoring not installed - GCP provider unavailable")
            self._available = False
            return False
        except Exception as e:
            logger.debug(f"GCP Cloud Monitoring client creation failed: {e}")
            self._available = False
            return False

    def collect(self) -> SystemMetrics | None:
        """Collect metrics from Cloud Monitoring."""
        if not self._client or not self._project_id or not self._instance_id:
            return None

        try:
            from datetime import UTC, datetime

            from google.cloud.monitoring_v3 import Aggregation, ListTimeSeriesRequest
            from google.protobuf.timestamp_pb2 import Timestamp

            metrics = SystemMetrics(provider_name=self.name)

            now = datetime.now(UTC)
            end_time = Timestamp()
            end_time.FromDatetime(now)

            start_time = Timestamp()
            start_time.FromDatetime(now.replace(minute=now.minute - 5))

            # Cloud SQL metrics
            gcp_metrics = [
                ("cloudsql.googleapis.com/database/cpu/utilization", "cpu_percent", 100),
                ("cloudsql.googleapis.com/database/memory/utilization", "memory_percent", 100),
                ("cloudsql.googleapis.com/database/disk/read_ops_count", "disk_read_iops", 1),
                ("cloudsql.googleapis.com/database/disk/write_ops_count", "disk_write_iops", 1),
                ("cloudsql.googleapis.com/database/network/received_bytes_count", "network_bytes_recv", 1),
                ("cloudsql.googleapis.com/database/network/sent_bytes_count", "network_bytes_sent", 1),
            ]

            project_name = f"projects/{self._project_id}"

            for metric_type, attr_name, multiplier in gcp_metrics:
                try:
                    request = ListTimeSeriesRequest(
                        name=project_name,
                        filter=f'metric.type="{metric_type}" AND resource.labels.database_id="{self._instance_id}"',
                        interval={
                            "start_time": start_time,
                            "end_time": end_time,
                        },
                        aggregation=Aggregation(
                            alignment_period={"seconds": 60},
                            per_series_aligner=Aggregation.Aligner.ALIGN_MEAN,
                        ),
                    )

                    results = self._client.list_time_series(request=request)

                    for series in results:
                        if series.points:
                            value = series.points[0].value.double_value * multiplier

                            if attr_name == "cpu_percent":
                                metrics.cpu_percent = value
                            elif attr_name == "memory_percent":
                                # GCP provides utilization percentage
                                pass
                            elif attr_name == "disk_read_iops":
                                metrics.disk_read_iops = int(value)
                            elif attr_name == "disk_write_iops":
                                metrics.disk_write_iops = int(value)
                            elif attr_name == "network_bytes_recv":
                                metrics.network_bytes_recv = int(value)
                            elif attr_name == "network_bytes_sent":
                                metrics.network_bytes_sent = int(value)
                        break
                except Exception as e:
                    logger.debug(f"Failed to get GCP metric {metric_type}: {e}")

            return metrics
        except Exception as e:
            logger.warning(f"GCP Cloud Monitoring metrics collection failed: {e}")
            return None

    @property
    def name(self) -> str:
        """Return provider name."""
        return "gcp"


class AzureSystemMetricsProvider(SystemMetricsProvider):
    """Collect system metrics from Azure Monitor for Azure Database for PostgreSQL."""

    def __init__(
        self,
        subscription_id: str | None = None,
        resource_group: str | None = None,
        server_name: str | None = None,
    ) -> None:
        """Initialize Azure Monitor metrics provider.

        Args:
            subscription_id: Azure subscription ID
            resource_group: Azure resource group name
            server_name: PostgreSQL server name
        """
        self._subscription_id = subscription_id
        self._resource_group = resource_group
        self._server_name = server_name
        self._client = None
        self._credential = None
        self._available: bool | None = None

    def is_available(self) -> bool:
        """Check if azure-monitor-query is available and credentials work."""
        if self._available is not None:
            return self._available

        try:
            from azure.identity import DefaultAzureCredential
            from azure.monitor.query import MetricsQueryClient

            self._credential = DefaultAzureCredential()
            self._client = MetricsQueryClient(self._credential)

            if self._subscription_id and self._resource_group and self._server_name:
                self._available = True
                return True
            self._available = False
            return False
        except ImportError:
            logger.debug("azure-monitor-query not installed - Azure provider unavailable")
            self._available = False
            return False
        except Exception as e:
            logger.debug(f"Azure Monitor client creation failed: {e}")
            self._available = False
            return False

    def collect(self) -> SystemMetrics | None:
        """Collect metrics from Azure Monitor."""
        if not self._client:
            return None

        try:
            from datetime import timedelta

            metrics = SystemMetrics(provider_name=self.name)

            # Azure PostgreSQL Flexible Server resource URI
            resource_uri = (
                f"/subscriptions/{self._subscription_id}"
                f"/resourceGroups/{self._resource_group}"
                f"/providers/Microsoft.DBforPostgreSQL/flexibleServers/{self._server_name}"
            )

            # Azure metrics to collect
            azure_metrics = [
                "cpu_percent",
                "memory_percent",
                "iops",
                "network_bytes_egress",
                "network_bytes_ingress",
            ]

            try:
                response = self._client.query_resource(
                    resource_uri=resource_uri,
                    metric_names=azure_metrics,
                    timespan=timedelta(minutes=5),
                )

                for metric in response.metrics:
                    if metric.timeseries:
                        for ts in metric.timeseries:
                            if ts.data:
                                latest = ts.data[-1]
                                value = latest.average

                                if metric.name == "cpu_percent" and value is not None:
                                    metrics.cpu_percent = value
                                elif metric.name == "iops" and value is not None:
                                    # Azure combines read/write IOPS
                                    metrics.disk_read_iops = int(value / 2)
                                    metrics.disk_write_iops = int(value / 2)
                                elif metric.name == "network_bytes_ingress" and value is not None:
                                    metrics.network_bytes_recv = int(value)
                                elif metric.name == "network_bytes_egress" and value is not None:
                                    metrics.network_bytes_sent = int(value)
            except Exception as e:
                logger.debug(f"Failed to query Azure metrics: {e}")

            return metrics
        except Exception as e:
            logger.warning(f"Azure Monitor metrics collection failed: {e}")
            return None

    @property
    def name(self) -> str:
        """Return provider name."""
        return "azure"


class NoOpSystemMetricsProvider(SystemMetricsProvider):
    """No-op provider that returns no metrics (disabled state)."""

    def is_available(self) -> bool:
        """Always available (as a fallback)."""
        return True

    def collect(self) -> SystemMetrics | None:
        """Return None - metrics collection is disabled."""
        return None

    @property
    def name(self) -> str:
        """Return provider name."""
        return "none"


def _is_localhost(host: str) -> bool:
    """Check if the host is localhost."""
    try:
        monitored_ip = socket.gethostbyname(host)
        local_ip = socket.gethostbyname(socket.gethostname())
        return monitored_ip == "127.0.0.1" or monitored_ip == local_ip
    except socket.gaierror:
        return False


def get_system_metrics_provider(
    detected_cloud_provider: str | None,
    db_connection: Database,
    host: str,
    manual_override: str | None = None,
    ssh_tunnel_active: bool = False,
    # Cloud-specific params
    aws_region: str | None = None,
    aws_db_identifier: str | None = None,
    gcp_project: str | None = None,
    gcp_instance: str | None = None,
    azure_subscription: str | None = None,
    azure_resource_group: str | None = None,
    azure_server_name: str | None = None,
) -> SystemMetricsProvider:
    """Factory function to get the appropriate system metrics provider.

    Detection/selection flow:
    1. If manual_override is set, use that provider
    2. If SSH tunnel active, skip local and try extension (for remote system metrics)
    3. If localhost (and no SSH tunnel), use LocalSystemMetricsProvider
    4. If cloud provider detected, try cloud provider API
    5. Try system_stats extension
    6. Fall back to NoOpSystemMetricsProvider

    Args:
        detected_cloud_provider: Cloud provider detected from hostname/settings
        db_connection: PostgreSQL database connection
        host: Database host
        manual_override: Manual provider selection (aws, gcp, azure, extension, local, none)
        ssh_tunnel_active: Whether connection is through SSH tunnel
        aws_region: AWS region for CloudWatch
        aws_db_identifier: RDS/Aurora DB instance identifier
        gcp_project: GCP project ID
        gcp_instance: Cloud SQL/AlloyDB instance ID
        azure_subscription: Azure subscription ID
        azure_resource_group: Azure resource group name
        azure_server_name: Azure PostgreSQL server name

    Returns:
        SystemMetricsProvider instance
    """
    # Manual override takes precedence
    if manual_override:
        if manual_override == "none":
            return NoOpSystemMetricsProvider()
        elif manual_override == "local":
            provider = LocalSystemMetricsProvider()
            if provider.is_available():
                return provider
            logger.warning("Local provider requested but psutil not available")
            return NoOpSystemMetricsProvider()
        elif manual_override == "extension":
            provider = ExtensionSystemMetricsProvider(db_connection)
            if provider.is_available():
                return provider
            logger.warning("Extension provider requested but system_stats not installed")
            return NoOpSystemMetricsProvider()
        elif manual_override == "aws":
            provider = AWSSystemMetricsProvider(
                region=aws_region,
                db_identifier=aws_db_identifier,
            )
            if provider.is_available():
                return provider
            logger.warning("AWS provider requested but not available (check boto3 and credentials)")
            return NoOpSystemMetricsProvider()
        elif manual_override == "gcp":
            provider = GCPSystemMetricsProvider(
                project_id=gcp_project,
                instance_id=gcp_instance,
            )
            if provider.is_available():
                return provider
            logger.warning("GCP provider requested but not available (check google-cloud-monitoring and credentials)")
            return NoOpSystemMetricsProvider()
        elif manual_override == "azure":
            provider = AzureSystemMetricsProvider(
                subscription_id=azure_subscription,
                resource_group=azure_resource_group,
                server_name=azure_server_name,
            )
            if provider.is_available():
                return provider
            logger.warning("Azure provider requested but not available (check azure-monitor-query and credentials)")
            return NoOpSystemMetricsProvider()
        else:
            logger.warning(f"Unknown system metrics provider: {manual_override}")
            return NoOpSystemMetricsProvider()

    # Auto-detection flow

    # 1. SSH tunnel active - skip local provider, try extension for remote metrics
    if ssh_tunnel_active:
        logger.debug("SSH tunnel active - checking for system_stats extension on remote")
        extension_provider = ExtensionSystemMetricsProvider(db_connection)
        if extension_provider.is_available():
            logger.info("Using system_stats extension for remote system metrics (via SSH tunnel)")
            return extension_provider
        logger.debug("system_stats extension not available on remote - system metrics disabled")
        return NoOpSystemMetricsProvider()

    # 2. Check if localhost (only when not using SSH tunnel)
    if _is_localhost(host):
        provider = LocalSystemMetricsProvider()
        if provider.is_available():
            logger.debug("Using local system metrics provider (localhost detected)")
            return provider

    # 3. Cloud provider detection
    if detected_cloud_provider in ("rds", "aurora"):
        provider = AWSSystemMetricsProvider(
            region=aws_region,
            db_identifier=aws_db_identifier,
        )
        if provider.is_available():
            logger.debug("Using AWS CloudWatch system metrics provider")
            return provider
        logger.debug("AWS CloudWatch not available, trying extension fallback")

    elif detected_cloud_provider in ("cloud_sql", "alloydb"):
        provider = GCPSystemMetricsProvider(
            project_id=gcp_project,
            instance_id=gcp_instance,
        )
        if provider.is_available():
            logger.debug("Using GCP Cloud Monitoring system metrics provider")
            return provider
        logger.debug("GCP Cloud Monitoring not available, trying extension fallback")

    elif detected_cloud_provider in ("azure", "cosmos_citus"):
        provider = AzureSystemMetricsProvider(
            subscription_id=azure_subscription,
            resource_group=azure_resource_group,
            server_name=azure_server_name,
        )
        if provider.is_available():
            logger.debug("Using Azure Monitor system metrics provider")
            return provider
        logger.debug("Azure Monitor not available, trying extension fallback")

    # 4. Try system_stats extension as fallback
    extension_provider = ExtensionSystemMetricsProvider(db_connection)
    if extension_provider.is_available():
        logger.debug("Using system_stats extension for system metrics")
        return extension_provider

    # 5. No provider available
    logger.debug("No system metrics provider available")
    return NoOpSystemMetricsProvider()
