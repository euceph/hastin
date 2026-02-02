"""Tests for System Metrics Providers."""

import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestImports:
    """Test that system metrics modules can be imported."""

    def test_import_system_metrics_providers(self):
        from hastin.Modules.SystemMetricsProviders import (
            AWSSystemMetricsProvider,
            AzureSystemMetricsProvider,
            ExtensionSystemMetricsProvider,
            GCPSystemMetricsProvider,
            LocalSystemMetricsProvider,
            NoOpSystemMetricsProvider,
            SystemMetrics,
            SystemMetricsProvider,
            get_system_metrics_provider,
        )

        assert SystemMetrics is not None
        assert SystemMetricsProvider is not None
        assert LocalSystemMetricsProvider is not None
        assert ExtensionSystemMetricsProvider is not None
        assert AWSSystemMetricsProvider is not None
        assert GCPSystemMetricsProvider is not None
        assert AzureSystemMetricsProvider is not None
        assert NoOpSystemMetricsProvider is not None
        assert get_system_metrics_provider is not None


class TestSystemMetrics:
    """Test SystemMetrics dataclass."""

    def test_default_values(self):
        from hastin.Modules.SystemMetricsProviders import SystemMetrics

        metrics = SystemMetrics()
        assert metrics.cpu_percent is None
        assert metrics.cpu_count is None
        assert metrics.memory_total is None
        assert metrics.provider_name == "unknown"

    def test_custom_values(self):
        from hastin.Modules.SystemMetricsProviders import SystemMetrics

        metrics = SystemMetrics(
            cpu_percent=50.5,
            cpu_count=4,
            memory_total=16000000000,
            memory_used=8000000000,
            provider_name="test",
        )
        assert metrics.cpu_percent == 50.5
        assert metrics.cpu_count == 4
        assert metrics.memory_total == 16000000000
        assert metrics.memory_used == 8000000000
        assert metrics.provider_name == "test"


class TestNoOpProvider:
    """Test NoOpSystemMetricsProvider."""

    def test_is_available(self):
        from hastin.Modules.SystemMetricsProviders import NoOpSystemMetricsProvider

        provider = NoOpSystemMetricsProvider()
        assert provider.is_available() is True

    def test_collect_returns_none(self):
        from hastin.Modules.SystemMetricsProviders import NoOpSystemMetricsProvider

        provider = NoOpSystemMetricsProvider()
        assert provider.collect() is None

    def test_name(self):
        from hastin.Modules.SystemMetricsProviders import NoOpSystemMetricsProvider

        provider = NoOpSystemMetricsProvider()
        assert provider.name == "none"


class TestLocalProvider:
    """Test LocalSystemMetricsProvider."""

    def test_is_available(self):
        from hastin.Modules.SystemMetricsProviders import LocalSystemMetricsProvider

        provider = LocalSystemMetricsProvider()
        # Should be available since psutil is a dependency
        assert provider.is_available() is True

    def test_collect_returns_metrics(self):
        from hastin.Modules.SystemMetricsProviders import LocalSystemMetricsProvider

        provider = LocalSystemMetricsProvider()
        if provider.is_available():
            metrics = provider.collect()
            assert metrics is not None
            assert metrics.provider_name == "local"
            assert metrics.cpu_percent is not None
            assert metrics.cpu_count is not None
            assert metrics.memory_total is not None
            assert metrics.memory_used is not None

    def test_name(self):
        from hastin.Modules.SystemMetricsProviders import LocalSystemMetricsProvider

        provider = LocalSystemMetricsProvider()
        assert provider.name == "local"


class TestExtensionProvider:
    """Test ExtensionSystemMetricsProvider."""

    def test_is_available_without_extension(self):
        from hastin.Modules.SystemMetricsProviders import ExtensionSystemMetricsProvider

        mock_db = MagicMock()
        mock_db.fetchone.return_value = None

        provider = ExtensionSystemMetricsProvider(mock_db)
        assert provider.is_available() is False

    def test_is_available_with_extension(self):
        from hastin.Modules.SystemMetricsProviders import ExtensionSystemMetricsProvider

        mock_db = MagicMock()
        mock_db.fetchone.return_value = {"extname": "system_stats"}

        provider = ExtensionSystemMetricsProvider(mock_db)
        assert provider.is_available() is True

    def test_name(self):
        from hastin.Modules.SystemMetricsProviders import ExtensionSystemMetricsProvider

        mock_db = MagicMock()
        provider = ExtensionSystemMetricsProvider(mock_db)
        assert provider.name == "extension"


class TestAWSProvider:
    """Test AWSSystemMetricsProvider."""

    def test_is_available_without_boto3(self):
        from hastin.Modules.SystemMetricsProviders import AWSSystemMetricsProvider

        with patch.dict("sys.modules", {"boto3": None}):
            provider = AWSSystemMetricsProvider(region="us-east-1")
            # This will try to import boto3, which we've mocked to None
            # The actual behavior depends on the implementation
            # Just verify it doesn't crash
            _ = provider.is_available()

    def test_name(self):
        from hastin.Modules.SystemMetricsProviders import AWSSystemMetricsProvider

        provider = AWSSystemMetricsProvider()
        assert provider.name == "aws"

    def test_collect_without_db_identifier(self):
        from hastin.Modules.SystemMetricsProviders import AWSSystemMetricsProvider

        provider = AWSSystemMetricsProvider(region="us-east-1")
        # Without db_identifier, collect should return None
        result = provider.collect()
        assert result is None


class TestGCPProvider:
    """Test GCPSystemMetricsProvider."""

    def test_name(self):
        from hastin.Modules.SystemMetricsProviders import GCPSystemMetricsProvider

        provider = GCPSystemMetricsProvider()
        assert provider.name == "gcp"

    def test_collect_without_project(self):
        from hastin.Modules.SystemMetricsProviders import GCPSystemMetricsProvider

        provider = GCPSystemMetricsProvider()
        # Without project_id, collect should return None
        result = provider.collect()
        assert result is None


class TestAzureProvider:
    """Test AzureSystemMetricsProvider."""

    def test_name(self):
        from hastin.Modules.SystemMetricsProviders import AzureSystemMetricsProvider

        provider = AzureSystemMetricsProvider()
        assert provider.name == "azure"

    def test_collect_without_config(self):
        from hastin.Modules.SystemMetricsProviders import AzureSystemMetricsProvider

        provider = AzureSystemMetricsProvider()
        # Without configuration, collect should return None
        result = provider.collect()
        assert result is None


class TestGetSystemMetricsProvider:
    """Test get_system_metrics_provider factory function."""

    def test_manual_override_none(self):
        from hastin.Modules.SystemMetricsProviders import (
            NoOpSystemMetricsProvider,
            get_system_metrics_provider,
        )

        mock_db = MagicMock()
        provider = get_system_metrics_provider(
            detected_cloud_provider=None,
            db_connection=mock_db,
            host="example.com",
            manual_override="none",
        )
        assert isinstance(provider, NoOpSystemMetricsProvider)

    def test_manual_override_local(self):
        from hastin.Modules.SystemMetricsProviders import (
            LocalSystemMetricsProvider,
            get_system_metrics_provider,
        )

        mock_db = MagicMock()
        provider = get_system_metrics_provider(
            detected_cloud_provider=None,
            db_connection=mock_db,
            host="example.com",
            manual_override="local",
        )
        # Should be Local if psutil is available
        assert isinstance(provider, LocalSystemMetricsProvider)

    def test_localhost_detection(self):
        from hastin.Modules.SystemMetricsProviders import (
            LocalSystemMetricsProvider,
            get_system_metrics_provider,
        )

        mock_db = MagicMock()
        provider = get_system_metrics_provider(
            detected_cloud_provider=None,
            db_connection=mock_db,
            host="localhost",
        )
        # Should use local provider for localhost
        assert isinstance(provider, LocalSystemMetricsProvider)

    def test_127_0_0_1_detection(self):
        from hastin.Modules.SystemMetricsProviders import (
            LocalSystemMetricsProvider,
            get_system_metrics_provider,
        )

        mock_db = MagicMock()
        provider = get_system_metrics_provider(
            detected_cloud_provider=None,
            db_connection=mock_db,
            host="127.0.0.1",
        )
        # Should use local provider for 127.0.0.1
        assert isinstance(provider, LocalSystemMetricsProvider)

    def test_extension_fallback(self):
        from hastin.Modules.SystemMetricsProviders import (
            ExtensionSystemMetricsProvider,
            get_system_metrics_provider,
        )

        mock_db = MagicMock()
        # Mock extension being available
        mock_db.fetchone.return_value = {"extname": "system_stats"}

        provider = get_system_metrics_provider(
            detected_cloud_provider=None,
            db_connection=mock_db,
            host="remote-host.example.com",
        )
        # Should try extension provider for remote hosts without cloud
        assert isinstance(provider, ExtensionSystemMetricsProvider)

    def test_noop_fallback(self):
        from hastin.Modules.SystemMetricsProviders import (
            NoOpSystemMetricsProvider,
            get_system_metrics_provider,
        )

        mock_db = MagicMock()
        # Mock extension not available
        mock_db.fetchone.return_value = None

        provider = get_system_metrics_provider(
            detected_cloud_provider=None,
            db_connection=mock_db,
            host="remote-host.example.com",
        )
        # Should fall back to NoOp when nothing else available
        assert isinstance(provider, NoOpSystemMetricsProvider)

    def test_unknown_override(self):
        from hastin.Modules.SystemMetricsProviders import (
            NoOpSystemMetricsProvider,
            get_system_metrics_provider,
        )

        mock_db = MagicMock()
        provider = get_system_metrics_provider(
            detected_cloud_provider=None,
            db_connection=mock_db,
            host="example.com",
            manual_override="unknown_provider",
        )
        # Should fall back to NoOp for unknown provider
        assert isinstance(provider, NoOpSystemMetricsProvider)


class TestIntegration:
    """Integration tests for system metrics providers."""

    def test_local_provider_full_cycle(self):
        """Test full collection cycle with local provider."""
        from hastin.Modules.SystemMetricsProviders import LocalSystemMetricsProvider

        provider = LocalSystemMetricsProvider()
        if provider.is_available():
            metrics = provider.collect()
            assert metrics is not None

            # Verify all expected fields are present
            assert metrics.uptime_seconds is not None
            assert metrics.cpu_percent is not None
            assert metrics.cpu_count is not None
            assert metrics.memory_total is not None
            assert metrics.memory_used is not None
            assert metrics.swap_total is not None
            assert metrics.swap_used is not None
            # Disk/network may be None on some systems
            assert metrics.provider_name == "local"

    def test_provider_names_are_unique(self):
        """Verify all providers have unique names."""
        from hastin.Modules.SystemMetricsProviders import (
            AWSSystemMetricsProvider,
            AzureSystemMetricsProvider,
            ExtensionSystemMetricsProvider,
            GCPSystemMetricsProvider,
            LocalSystemMetricsProvider,
            NoOpSystemMetricsProvider,
        )

        mock_db = MagicMock()

        providers = [
            LocalSystemMetricsProvider(),
            ExtensionSystemMetricsProvider(mock_db),
            AWSSystemMetricsProvider(),
            GCPSystemMetricsProvider(),
            AzureSystemMetricsProvider(),
            NoOpSystemMetricsProvider(),
        ]

        names = [p.name for p in providers]
        assert len(names) == len(set(names)), "Provider names must be unique"

        expected_names = {"local", "extension", "aws", "gcp", "azure", "none"}
        assert set(names) == expected_names
