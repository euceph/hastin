"""Tests for SSH Tunnel support."""

import os
import sys
from unittest.mock import MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# =============================================================================
# Import Tests
# =============================================================================


class TestSSHTunnelImports:
    """Test that SSH tunnel modules can be imported."""

    def test_import_ssh_tunnel_module(self):
        from hastin.Modules.SSHTunnel import SSHTunnel, SSHTunnelManager, TunnelConfig

        assert SSHTunnel is not None
        assert SSHTunnelManager is not None
        assert TunnelConfig is not None


# =============================================================================
# TunnelConfig Tests
# =============================================================================


class TestTunnelConfig:
    """Test TunnelConfig dataclass."""

    def test_default_values(self):
        from hastin.Modules.SSHTunnel import TunnelConfig

        config = TunnelConfig(ssh_host="example.com")
        assert config.ssh_host == "example.com"
        assert config.remote_host == "localhost"
        assert config.remote_port == 5432
        assert config.local_port is None
        assert config.ssh_user is None
        assert config.ssh_port == 22
        assert config.ssh_key is None

    def test_custom_values(self):
        from hastin.Modules.SSHTunnel import TunnelConfig

        config = TunnelConfig(
            ssh_host="myserver",
            remote_host="db.internal",
            remote_port=5433,
            local_port=15433,
            ssh_user="deploy",
            ssh_port=2222,
            ssh_key="/path/to/key",
        )
        assert config.ssh_host == "myserver"
        assert config.remote_host == "db.internal"
        assert config.remote_port == 5433
        assert config.local_port == 15433
        assert config.ssh_user == "deploy"
        assert config.ssh_port == 2222
        assert config.ssh_key == "/path/to/key"


# =============================================================================
# SSHTunnel Tests
# =============================================================================


class TestSSHTunnel:
    """Test SSHTunnel class."""

    def test_tunnel_not_active_initially(self):
        from hastin.Modules.SSHTunnel import SSHTunnel, TunnelConfig

        config = TunnelConfig(ssh_host="example.com")
        tunnel = SSHTunnel(config)
        assert tunnel.is_active is False
        assert tunnel.local_port is None

    def test_tunnel_name_property(self):
        from hastin.Modules.SSHTunnel import SSHTunnel, TunnelConfig

        config = TunnelConfig(ssh_host="example.com")
        tunnel = SSHTunnel(config)
        assert tunnel.config.ssh_host == "example.com"

    def test_find_free_port(self):
        from hastin.Modules.SSHTunnel import SSHTunnel, TunnelConfig

        config = TunnelConfig(ssh_host="example.com")
        tunnel = SSHTunnel(config)
        port = tunnel._find_free_port()
        assert isinstance(port, int)
        assert port > 0
        assert port < 65536


# =============================================================================
# SSHTunnelManager Tests
# =============================================================================


class TestSSHTunnelManager:
    """Test SSHTunnelManager class."""

    def test_manager_initialization(self):
        from hastin.Modules.SSHTunnel import SSHTunnelManager

        manager = SSHTunnelManager(
            ssh_host="vps",
            ssh_user="root",
            ssh_port=22,
        )
        assert manager.ssh_host == "vps"
        assert manager.ssh_user == "root"
        assert manager.ssh_port == 22

    def test_get_tunnel_returns_none_for_unknown(self):
        from hastin.Modules.SSHTunnel import SSHTunnelManager

        manager = SSHTunnelManager(ssh_host="vps")
        assert manager.get_tunnel("nonexistent") is None
        assert manager.get_local_port("nonexistent") is None

    def test_close_all_empty(self):
        from hastin.Modules.SSHTunnel import SSHTunnelManager

        manager = SSHTunnelManager(ssh_host="vps")
        # Should not raise
        manager.close_all()


# =============================================================================
# ArgumentParser SSH Options Tests
# =============================================================================


class TestSSHConfigOptions:
    """Test SSH config options in Config dataclass."""

    def test_config_has_ssh_options(self):
        from hastin.Modules.ArgumentParser import Config

        config = Config(app_version="test")
        assert hasattr(config, "ssh")
        assert hasattr(config, "ssh_user")
        assert hasattr(config, "ssh_port")
        assert hasattr(config, "ssh_key")

    def test_config_ssh_defaults(self):
        from hastin.Modules.ArgumentParser import Config

        config = Config(app_version="test")
        assert config.ssh is None
        assert config.ssh_user is None
        assert config.ssh_port == 22
        assert config.ssh_key is None


# =============================================================================
# SystemMetricsProvider SSH Detection Tests
# =============================================================================


class TestSSHTunnelMetricsDetection:
    """Test system metrics provider selection with SSH tunnel."""

    def test_ssh_tunnel_skips_local_provider(self):
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
            host="localhost",  # Would normally use local provider
            ssh_tunnel_active=True,  # But SSH tunnel is active
        )
        # Should fall back to NoOp since extension not available
        assert isinstance(provider, NoOpSystemMetricsProvider)

    def test_ssh_tunnel_uses_extension_when_available(self):
        from hastin.Modules.SystemMetricsProviders import (
            ExtensionSystemMetricsProvider,
            get_system_metrics_provider,
        )

        mock_db = MagicMock()
        # Mock extension available
        mock_db.fetchone.return_value = {"extname": "system_stats"}

        provider = get_system_metrics_provider(
            detected_cloud_provider=None,
            db_connection=mock_db,
            host="localhost",
            ssh_tunnel_active=True,
        )
        # Should use extension provider
        assert isinstance(provider, ExtensionSystemMetricsProvider)

    def test_no_ssh_tunnel_uses_local_for_localhost(self):
        from hastin.Modules.SystemMetricsProviders import (
            LocalSystemMetricsProvider,
            get_system_metrics_provider,
        )

        mock_db = MagicMock()

        provider = get_system_metrics_provider(
            detected_cloud_provider=None,
            db_connection=mock_db,
            host="localhost",
            ssh_tunnel_active=False,
        )
        # Should use local provider
        assert isinstance(provider, LocalSystemMetricsProvider)


# =============================================================================
# Integration Tests (require SSH access)
# =============================================================================


class TestSSHTunnelIntegration:
    """Integration tests for SSH tunnel (skipped without SSH access)."""

    def test_tunnel_command_building(self):
        """Test that SSH command is built correctly."""
        from hastin.Modules.SSHTunnel import SSHTunnel, TunnelConfig

        config = TunnelConfig(
            ssh_host="vps",
            remote_host="localhost",
            remote_port=5433,
            ssh_user="root",
        )
        tunnel = SSHTunnel(config)
        cmd = tunnel._build_ssh_command()

        assert "ssh" in cmd
        assert "-N" in cmd
        assert "root@vps" in cmd
        # Should contain port forwarding
        assert any("5433" in arg for arg in cmd)

    def test_tunnel_command_without_user(self):
        """Test SSH command uses host alias when no user specified."""
        from hastin.Modules.SSHTunnel import SSHTunnel, TunnelConfig

        config = TunnelConfig(
            ssh_host="vps",
            remote_port=5432,
        )
        tunnel = SSHTunnel(config)
        cmd = tunnel._build_ssh_command()

        # Should use just the host alias, not user@host
        assert "vps" in cmd
        assert "@" not in cmd[-1]  # Last arg should be just "vps"

    def test_tunnel_command_with_key(self):
        """Test SSH command includes key file."""
        from hastin.Modules.SSHTunnel import SSHTunnel, TunnelConfig

        config = TunnelConfig(
            ssh_host="vps",
            ssh_key="/home/user/.ssh/id_rsa",
        )
        tunnel = SSHTunnel(config)
        cmd = tunnel._build_ssh_command()

        assert "-i" in cmd
        assert "/home/user/.ssh/id_rsa" in cmd

    def test_tunnel_command_with_custom_port(self):
        """Test SSH command includes custom SSH port."""
        from hastin.Modules.SSHTunnel import SSHTunnel, TunnelConfig

        config = TunnelConfig(
            ssh_host="vps",
            ssh_port=2222,
        )
        tunnel = SSHTunnel(config)
        cmd = tunnel._build_ssh_command()

        assert "-p" in cmd
        assert "2222" in cmd
