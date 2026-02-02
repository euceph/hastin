"""SSH tunnel management for Hastin PostgreSQL monitoring dashboard.

Provides secure tunneled connections to remote PostgreSQL instances.
"""

from __future__ import annotations

import atexit
import shutil
import socket
import subprocess
import time
from dataclasses import dataclass, field

from loguru import logger


@dataclass
class TunnelConfig:
    """Configuration for an SSH tunnel."""

    ssh_host: str  # SSH host (can be alias from ~/.ssh/config)
    remote_host: str = "localhost"  # Host on the remote side
    remote_port: int = 5432  # Port on the remote side
    local_port: int | None = None  # Local port (auto-assigned if None)
    ssh_user: str | None = None  # SSH user (uses SSH config default if None)
    ssh_port: int = 22  # SSH port
    ssh_key: str | None = None  # Path to SSH private key


@dataclass
class SSHTunnel:
    """Manages SSH tunnel connections.

    Uses subprocess with ssh command to leverage user's SSH config and keys.
    """

    config: TunnelConfig
    _process: subprocess.Popen | None = field(default=None, init=False, repr=False)
    _local_port: int | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        """Register cleanup on exit."""
        atexit.register(self.close)

    @property
    def local_port(self) -> int | None:
        """Get the local port for this tunnel."""
        return self._local_port

    @property
    def is_active(self) -> bool:
        """Check if the tunnel is active."""
        if self._process is None:
            return False
        return self._process.poll() is None

    def _find_free_port(self) -> int:
        """Find a free local port."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    def _build_ssh_command(self) -> list[str]:
        """Build the SSH command for the tunnel."""
        local_port = self.config.local_port or self._find_free_port()
        self._local_port = local_port

        cmd = [
            "ssh",
            "-N",  # Don't execute remote command
            "-L", f"{local_port}:{self.config.remote_host}:{self.config.remote_port}",
            # Connection settings
            "-o", "ServerAliveInterval=30",
            "-o", "ServerAliveCountMax=3",
            "-o", "ConnectTimeout=10",
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ExitOnForwardFailure=yes",
            # Batch mode - don't prompt for passwords
            "-o", "BatchMode=yes",
        ]

        # Add SSH port if not default
        if self.config.ssh_port != 22:
            cmd.extend(["-p", str(self.config.ssh_port)])

        # Add SSH key if specified
        if self.config.ssh_key:
            cmd.extend(["-i", self.config.ssh_key])

        # Add user@host or just host (uses SSH config)
        if self.config.ssh_user:
            cmd.append(f"{self.config.ssh_user}@{self.config.ssh_host}")
        else:
            cmd.append(self.config.ssh_host)

        return cmd

    def open(self, timeout: float = 10.0) -> int:
        """Open the SSH tunnel.

        Args:
            timeout: Maximum time to wait for tunnel to establish

        Returns:
            The local port number for the tunnel

        Raises:
            RuntimeError: If tunnel fails to establish
        """
        if self.is_active:
            logger.debug(f"Tunnel already active on port {self._local_port}")
            return self._local_port

        if not shutil.which("ssh"):
            raise RuntimeError("SSH client not found in PATH")

        cmd = self._build_ssh_command()
        logger.debug(f"Starting SSH tunnel: {' '.join(cmd)}")

        try:
            self._process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL,
            )
        except OSError as e:
            raise RuntimeError(f"Failed to start SSH process: {e}") from e

        # Wait for tunnel to be ready
        start_time = time.time()
        last_error = None

        while time.time() - start_time < timeout:
            returncode = self._process.poll()
            if returncode is not None and returncode != 0:
                # Process exited with error - get error message
                try:
                    _, stderr = self._process.communicate(timeout=1)
                    error_msg = stderr.decode().strip() if stderr else ""
                except Exception:
                    error_msg = ""

                if not error_msg:
                    error_msg = f"SSH process exited with code {returncode}"
                raise RuntimeError(f"SSH tunnel failed to start: {error_msg}")

            # Try to connect to the local port
            try:
                with socket.create_connection(("127.0.0.1", self._local_port), timeout=0.5):
                    logger.info(
                        f"SSH tunnel established: localhost:{self._local_port} -> "
                        f"{self.config.ssh_host}:{self.config.remote_port}"
                    )
                    return self._local_port
            except (OSError, ConnectionRefusedError) as e:
                last_error = e
                time.sleep(0.2)

        # Timeout reached
        self.close()
        raise RuntimeError(
            f"SSH tunnel failed to establish within {timeout}s. "
            f"Last connection error: {last_error}"
        )

    def close(self) -> None:
        """Close the SSH tunnel."""
        if self._process is not None:
            if self._process.poll() is None:
                logger.debug(f"Closing SSH tunnel on port {self._local_port}")
                self._process.terminate()
                try:
                    self._process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self._process.kill()
                    self._process.wait()
            self._process = None

    def __enter__(self) -> SSHTunnel:
        """Context manager entry."""
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()


@dataclass
class SSHTunnelManager:
    """Manages multiple SSH tunnels for a connection."""

    ssh_host: str
    ssh_user: str | None = None
    ssh_port: int = 22
    ssh_key: str | None = None

    _tunnels: dict[str, SSHTunnel] = field(default_factory=dict, init=False, repr=False)

    def create_tunnel(
        self,
        name: str,
        remote_host: str,
        remote_port: int,
        local_port: int | None = None,
    ) -> SSHTunnel:
        """Create and open a new tunnel.

        Args:
            name: Identifier for this tunnel (e.g., "postgres", "pgbouncer")
            remote_host: Host on the remote side
            remote_port: Port on the remote side
            local_port: Local port (auto-assigned if None)

        Returns:
            The SSHTunnel instance
        """
        config = TunnelConfig(
            ssh_host=self.ssh_host,
            ssh_user=self.ssh_user,
            ssh_port=self.ssh_port,
            ssh_key=self.ssh_key,
            remote_host=remote_host,
            remote_port=remote_port,
            local_port=local_port,
        )
        tunnel = SSHTunnel(config)
        tunnel.open()
        self._tunnels[name] = tunnel
        return tunnel

    def get_tunnel(self, name: str) -> SSHTunnel | None:
        """Get a tunnel by name."""
        return self._tunnels.get(name)

    def get_local_port(self, name: str) -> int | None:
        """Get the local port for a tunnel."""
        tunnel = self._tunnels.get(name)
        return tunnel.local_port if tunnel else None

    def close_all(self) -> None:
        """Close all tunnels."""
        for tunnel in self._tunnels.values():
            tunnel.close()
        self._tunnels.clear()

    def __enter__(self) -> SSHTunnelManager:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close_all()
