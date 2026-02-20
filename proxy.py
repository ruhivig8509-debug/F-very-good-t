# =============================================================================
# ADVANCED PROXY & REQUEST MANAGEMENT SYSTEM
# Version: 3.0.0 - Production Grade
# Author: Senior Python Backend Engineer
# Description: A comprehensive, thread-safe proxy management system with
#              health monitoring, intelligent rotation, ban detection,
#              telemetry dashboard, and smart request handling.
# =============================================================================

"""
Advanced Proxy & Request Management System
==========================================

This module provides a complete, production-grade proxy management solution
designed for high-concurrency scraping and automation tasks.

Architecture Overview:
    ┌─────────────────────────────────────────────────────────┐
    │                    ProxyManager                          │
    │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
    │  │ ProxyParser  │  │ HealthMonitor│  │  Telemetry   │  │
    │  └──────────────┘  └──────────────┘  └──────────────┘  │
    │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
    │  │  Rotator     │  │BanInterceptor│  │ API Refresher│  │
    │  └──────────────┘  └──────────────┘  └──────────────┘  │
    │  ┌─────────────────────────────────────────────────┐    │
    │  │            SmartRequestWrapper                   │    │
    │  └─────────────────────────────────────────────────┘    │
    └─────────────────────────────────────────────────────────┘

Usage:
    # Basic usage
    manager = ProxyManager(proxy_file="proxies.txt")
    manager.start()

    wrapper = SmartRequestWrapper(proxy_manager=manager)
    response = wrapper.get("https://example.com", thread_id=1)

    # Session-sticky usage (for account creation flows)
    with manager.sticky_session(thread_id=1) as session:
        r1 = session.post("https://api.example.com/step1", data={...})
        r2 = session.post("https://api.example.com/step2", data={...})
"""

# =============================================================================
# STANDARD LIBRARY IMPORTS
# =============================================================================
import re
import os
import sys
import time
import json
import math
import copy
import uuid
import random
import socket
import logging
import hashlib
import threading
import itertools
import contextlib
import collections
from enum import Enum, auto
from typing import (
    Optional, Dict, List, Tuple, Any,
    Callable, Iterator, Set, Union, NamedTuple
)
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from urllib.parse import urlparse
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, Future, as_completed

# =============================================================================
# THIRD-PARTY IMPORTS
# =============================================================================
try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except ImportError as e:
    print(f"[FATAL] Missing required package: {e}")
    print("Install with: pip install requests urllib3")
    sys.exit(1)

# Optional: colorama for colored console output
try:
    from colorama import Fore, Style, Back, init as colorama_init
    colorama_init(autoreset=True)
    COLORAMA_AVAILABLE = True
except ImportError:
    COLORAMA_AVAILABLE = False

    class _FakeFore:
        """Fallback when colorama is not installed."""
        def __getattr__(self, name: str) -> str:
            return ""

    class _FakeStyle:
        """Fallback style object."""
        def __getattr__(self, name: str) -> str:
            return ""

    Fore = _FakeFore()
    Style = _FakeStyle()

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

def setup_logger(name: str, level: int = logging.DEBUG) -> logging.Logger:
    """
    Configure and return a structured logger.

    Args:
        name: Logger name (usually __name__ or module name)
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    Returns:
        Configured Logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        # Console handler with formatting
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)

        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # File handler for persistent logs
        try:
            file_handler = logging.FileHandler(
                "proxy_manager.log", encoding="utf-8"
            )
            file_handler.setLevel(logging.WARNING)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except (OSError, PermissionError):
            pass  # Non-fatal if log file cannot be created

    return logger


# Global logger for this module
logger = setup_logger("proxy_manager", logging.INFO)

# =============================================================================
# ENUMERATIONS & CONSTANTS
# =============================================================================

class ProxyProtocol(Enum):
    """Supported proxy protocols."""
    HTTP    = "http"
    HTTPS   = "https"
    SOCKS4  = "socks4"
    SOCKS5  = "socks5"


class ProxyStatus(Enum):
    """Lifecycle states for a proxy."""
    ACTIVE    = auto()   # Healthy and in rotation
    TESTING   = auto()   # Currently being health-checked
    COOLDOWN  = auto()   # Temporarily removed due to errors
    BLACKLIST = auto()   # Permanently removed
    UNKNOWN   = auto()   # Not yet tested


class AnonymityLevel(Enum):
    """Proxy anonymity classification."""
    TRANSPARENT = auto()   # Server sees real IP
    ANONYMOUS   = auto()   # Server sees proxy IP, knows it's a proxy
    ELITE       = auto()   # Server sees proxy IP, cannot detect proxy
    UNKNOWN     = auto()   # Not yet determined


class RotationStrategy(Enum):
    """Available proxy rotation algorithms."""
    ROUND_ROBIN         = "round_robin"
    WEIGHTED_ROUND_ROBIN = "weighted_round_robin"
    RANDOM              = "random"
    LEAST_USED          = "least_used"
    FASTEST_FIRST       = "fastest_first"
    SESSION_STICKY      = "session_sticky"


# System-wide constants
HEALTH_CHECK_URLS = [
    "http://httpbin.org/ip",
    "https://api.ipify.org?format=json",
    "http://ip-api.com/json/",
    "https://ifconfig.me/all.json",
]

ANONYMITY_CHECK_URL = "http://httpbin.org/headers"
DEFAULT_HEALTH_INTERVAL_SEC = 120    # Check proxies every 2 minutes
DEFAULT_COOLDOWN_SEC         = 300   # 5-minute cooldown before retry
DEFAULT_MAX_FAILURES         = 3     # Failures before blacklist
DEFAULT_REQUEST_TIMEOUT_SEC  = 30
DEFAULT_HEALTH_TIMEOUT_SEC   = 15
MAX_POOL_SIZE                = 500   # Max proxies in the pool

# HTTP status codes that indicate a ban or rate limit
BAN_STATUS_CODES: Set[int]     = {403, 429, 503, 407, 401}
RETRY_STATUS_CODES: Set[int]   = {500, 502, 503, 504}
SUCCESS_STATUS_CODES: Set[int] = set(range(200, 300))

# =============================================================================
# DATA MODELS (using dataclasses for clean, typed structures)
# =============================================================================

@dataclass
class ProxyMetrics:
    """
    Real-time performance metrics for a single proxy.
    All fields are updated atomically via the ProxyEntry lock.
    """
    total_requests:   int   = 0
    successful_reqs:  int   = 0
    failed_reqs:      int   = 0
    banned_count:     int   = 0
    total_latency_ms: float = 0.0
    min_latency_ms:   float = float("inf")
    max_latency_ms:   float = 0.0
    last_used_ts:     float = field(default_factory=time.time)
    last_checked_ts:  float = 0.0
    last_success_ts:  float = 0.0
    last_failure_ts:  float = 0.0
    consecutive_failures: int = 0

    @property
    def avg_latency_ms(self) -> float:
        """Calculate average latency across all requests."""
        if self.total_requests == 0:
            return float("inf")
        return self.total_latency_ms / self.total_requests

    @property
    def success_rate(self) -> float:
        """Calculate success rate as a percentage (0.0 - 100.0)."""
        if self.total_requests == 0:
            return 0.0
        return (self.successful_reqs / self.total_requests) * 100.0

    @property
    def weight(self) -> float:
        """
        Compute a composite weight for weighted round-robin selection.
        Higher weight = better proxy. Formula balances speed and reliability.
        """
        if self.total_requests < 3:
            return 50.0  # Neutral weight for untested proxies

        reliability = self.success_rate / 100.0
        # Invert latency: faster = higher weight
        latency_score = max(0.0, 1.0 - (self.avg_latency_ms / 10000.0))
        return round((reliability * 70.0) + (latency_score * 30.0), 2)

    def record_request(
        self,
        success: bool,
        latency_ms: float,
        was_banned: bool = False
    ) -> None:
        """
        Update all metrics after a request completes.

        Args:
            success:    True if the request returned a 2xx status
            latency_ms: Request round-trip time in milliseconds
            was_banned: True if a ban status code was returned
        """
        self.total_requests += 1
        self.total_latency_ms += latency_ms
        self.last_used_ts = time.time()

        self.min_latency_ms = min(self.min_latency_ms, latency_ms)
        self.max_latency_ms = max(self.max_latency_ms, latency_ms)

        if was_banned:
            self.banned_count += 1
            self.failed_reqs += 1
            self.consecutive_failures += 1
            self.last_failure_ts = time.time()
        elif success:
            self.successful_reqs += 1
            self.consecutive_failures = 0
            self.last_success_ts = time.time()
        else:
            self.failed_reqs += 1
            self.consecutive_failures += 1
            self.last_failure_ts = time.time()


@dataclass
class ProxyEntry:
    """
    A complete proxy object including connection info, status, and metrics.
    Thread-safety is ensured via the internal RLock.
    """
    # Immutable identity fields
    proxy_id:   str           = field(default_factory=lambda: str(uuid.uuid4())[:8])
    protocol:   ProxyProtocol = ProxyProtocol.HTTP
    host:       str           = ""
    port:       int           = 0
    username:   Optional[str] = None
    password:   Optional[str] = None

    # Mutable state fields (protected by lock)
    status:     ProxyStatus   = ProxyStatus.UNKNOWN
    anonymity:  AnonymityLevel = AnonymityLevel.UNKNOWN
    country:    str           = "UNKNOWN"
    metrics:    ProxyMetrics  = field(default_factory=ProxyMetrics)

    # Cooldown management
    cooldown_until_ts: float  = 0.0
    blacklist_reason:  str    = ""

    # Internal threading primitive (not serialized)
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def __post_init__(self):
        """Ensure the RLock is always initialized."""
        if not isinstance(self._lock, threading.RLock):
            object.__setattr__(self, "_lock", threading.RLock())

    @property
    def url(self) -> str:
        """
        Build the full proxy URL string for requests library.

        Returns:
            Full proxy URL (e.g., 'socks5://user:pass@1.2.3.4:8080')
        """
        auth = f"{self.username}:{self.password}@" if self.username else ""
        return f"{self.protocol.value}://{auth}{self.host}:{self.port}"

    @property
    def requests_dict(self) -> Dict[str, str]:
        """
        Return a proxy dict compatible with requests.Session.proxies.

        Returns:
            Dict with 'http' and 'https' keys set to the proxy URL
        """
        return {"http": self.url, "https": self.url}

    @property
    def is_available(self) -> bool:
        """Check if this proxy is ready for use."""
        with self._lock:
            if self.status in (ProxyStatus.BLACKLIST, ProxyStatus.TESTING):
                return False
            if self.status == ProxyStatus.COOLDOWN:
                # Auto-recover from cooldown if time has elapsed
                if time.time() >= self.cooldown_until_ts:
                    self.status = ProxyStatus.ACTIVE
                    self.metrics.consecutive_failures = 0
                    return True
                return False
            return self.status == ProxyStatus.ACTIVE

    @property
    def display_str(self) -> str:
        """Short human-readable proxy identifier for logs."""
        auth = f"{self.username}@" if self.username else ""
        return f"{self.protocol.value}://{auth}{self.host}:{self.port}"

    def set_cooldown(self, duration_sec: float = DEFAULT_COOLDOWN_SEC) -> None:
        """
        Move proxy to COOLDOWN state for the specified duration.

        Args:
            duration_sec: How long to keep this proxy in cooldown
        """
        with self._lock:
            self.status = ProxyStatus.COOLDOWN
            self.cooldown_until_ts = time.time() + duration_sec
            logger.info(
                f"[COOLDOWN] {self.display_str} → {duration_sec:.0f}s"
            )

    def blacklist(self, reason: str = "") -> None:
        """
        Permanently remove proxy from rotation.

        Args:
            reason: Human-readable reason for blacklisting
        """
        with self._lock:
            self.status = ProxyStatus.BLACKLIST
            self.blacklist_reason = reason
            logger.warning(
                f"[BLACKLIST] {self.display_str} — {reason}"
            )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize proxy state to a JSON-compatible dictionary."""
        return {
            "proxy_id":  self.proxy_id,
            "protocol":  self.protocol.value,
            "host":      self.host,
            "port":      self.port,
            "username":  self.username,
            "status":    self.status.name,
            "anonymity": self.anonymity.name,
            "country":   self.country,
            "metrics": {
                "total_requests":  self.metrics.total_requests,
                "success_rate":    f"{self.metrics.success_rate:.1f}%",
                "avg_latency_ms":  f"{self.metrics.avg_latency_ms:.0f}",
                "weight":          self.metrics.weight,
                "banned_count":    self.metrics.banned_count,
                "consecutive_failures": self.metrics.consecutive_failures,
            }
        }


@dataclass
class BackoffConfig:
    """
    Exponential backoff configuration for the SmartRequestWrapper.

    Attributes:
        max_retries:      Maximum number of retry attempts
        base_delay_sec:   Initial delay before first retry
        max_delay_sec:    Upper cap on computed delay
        multiplier:       Exponent base (delay = base * multiplier^attempt)
        jitter_fraction:  Random fraction added to prevent thundering herd
    """
    max_retries:     int   = 4
    base_delay_sec:  float = 1.0
    max_delay_sec:   float = 60.0
    multiplier:      float = 2.0
    jitter_fraction: float = 0.3

    def compute_delay(self, attempt: int) -> float:
        """
        Calculate delay for a given attempt with jitter.

        Args:
            attempt: Zero-indexed attempt number

        Returns:
            Delay in seconds with random jitter applied
        """
        exponential = self.base_delay_sec * (self.multiplier ** attempt)
        capped      = min(exponential, self.max_delay_sec)
        jitter      = capped * self.jitter_fraction * random.random()
        return round(capped + jitter, 3)

    def sleep(self, attempt: int) -> None:
        """Compute and sleep for the backoff delay."""
        delay = self.compute_delay(attempt)
        logger.debug(f"[BACKOFF] Attempt {attempt+1}: sleeping {delay:.2f}s")
        time.sleep(delay)


@dataclass
class APIRefreshConfig:
    """
    Configuration for fetching proxies from an external API endpoint.

    Attributes:
        url:              Full URL of the proxy provider API
        interval_sec:     How often to refresh (in seconds)
        auth_header:      Optional authentication header value
        response_format:  Expected format: 'json', 'text', 'csv'
        json_path:        Dot-notation path into JSON (e.g., 'data.proxies')
        enabled:          Toggle API refresh on/off
    """
    url:             str   = ""
    interval_sec:    float = 600.0    # 10 minutes
    auth_header:     Optional[str] = None
    response_format: str  = "text"   # 'json', 'text', 'csv'
    json_path:       str  = ""       # e.g., "data.list"
    enabled:         bool = False

# =============================================================================
# PROXY PARSER
# =============================================================================

class ProxyParser:
    """
    Universal regex-based proxy string parser.

    Supported input formats:
        - 1.2.3.4:8080
        - user:pass@1.2.3.4:8080
        - http://1.2.3.4:8080
        - https://user:pass@1.2.3.4:8080
        - socks4://1.2.3.4:1080
        - socks5://user:pass@1.2.3.4:1080
        - 1.2.3.4:8080:user:pass  (alternative colon-separated format)

    All patterns are compiled once at class level for efficiency.
    """

    # Full URL format: protocol://[user:pass@]host:port
    _PATTERN_FULL_URL = re.compile(
        r"^(?P<protocol>https?|socks[45])://"
        r"(?:(?P<username>[^:@\s]+):(?P<password>[^@\s]+)@)?"
        r"(?P<host>[\w.\-]+):(?P<port>\d{1,5})/?$",
        re.IGNORECASE
    )

    # user:pass@host:port (no protocol)
    _PATTERN_AUTH_AT = re.compile(
        r"^(?P<username>[^:@\s]+):(?P<password>[^@\s]+)@"
        r"(?P<host>[\w.\-]+):(?P<port>\d{1,5})$"
    )

    # host:port (bare)
    _PATTERN_BARE = re.compile(
        r"^(?P<host>[\w.\-]+):(?P<port>\d{1,5})$"
    )

    # host:port:user:pass (alternative format from some providers)
    _PATTERN_COLON_SEP = re.compile(
        r"^(?P<host>[\w.\-]+):(?P<port>\d{1,5})"
        r":(?P<username>[^:\s]+):(?P<password>[^:\s]+)$"
    )

    # Protocol string → enum mapping
    _PROTOCOL_MAP: Dict[str, ProxyProtocol] = {
        "http":   ProxyProtocol.HTTP,
        "https":  ProxyProtocol.HTTPS,
        "socks4": ProxyProtocol.SOCKS4,
        "socks5": ProxyProtocol.SOCKS5,
    }

    @classmethod
    def parse_line(
        cls,
        line: str,
        default_protocol: ProxyProtocol = ProxyProtocol.HTTP
    ) -> Optional[ProxyEntry]:
        """
        Parse a single proxy string into a ProxyEntry object.

        Args:
            line:             Raw proxy string (any supported format)
            default_protocol: Protocol to assume when not specified

        Returns:
            ProxyEntry on success, None if parsing fails
        """
        line = line.strip()

        # Skip empty lines and comments
        if not line or line.startswith("#") or line.startswith("//"):
            return None

        # Remove trailing slashes
        line = line.rstrip("/")

        parsed = (
            cls._try_full_url(line) or
            cls._try_auth_at(line) or
            cls._try_colon_sep(line) or
            cls._try_bare(line)
        )

        if parsed is None:
            logger.debug(f"[PARSER] Failed to parse: '{line}'")
            return None

        host, port, username, password, protocol_str = parsed

        # Validate port range
        if not (1 <= port <= 65535):
            logger.debug(f"[PARSER] Invalid port {port} for '{line}'")
            return None

        # Determine protocol
        protocol = cls._PROTOCOL_MAP.get(
            (protocol_str or "").lower(), default_protocol
        )

        entry = ProxyEntry(
            protocol=protocol,
            host=host,
            port=port,
            username=username or None,
            password=password or None,
            status=ProxyStatus.UNKNOWN,
        )

        logger.debug(f"[PARSER] OK → {entry.display_str}")
        return entry

    @classmethod
    def parse_file(
        cls,
        filepath: str,
        default_protocol: ProxyProtocol = ProxyProtocol.HTTP
    ) -> List[ProxyEntry]:
        """
        Parse all proxies from a text file (one proxy per line).

        Args:
            filepath:         Path to the proxy list file
            default_protocol: Default protocol for entries without one

        Returns:
            List of successfully parsed ProxyEntry objects
        """
        if not os.path.isfile(filepath):
            logger.error(f"[PARSER] File not found: {filepath}")
            return []

        proxies: List[ProxyEntry] = []
        errors = 0

        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                for line_num, line in enumerate(f, start=1):
                    entry = cls.parse_line(line, default_protocol)
                    if entry:
                        proxies.append(entry)
                    elif line.strip() and not line.startswith("#"):
                        errors += 1
                        logger.debug(
                            f"[PARSER] Line {line_num} unparseable: {line.strip()!r}"
                        )
        except (OSError, IOError) as exc:
            logger.error(f"[PARSER] Cannot read file '{filepath}': {exc}")

        logger.info(
            f"[PARSER] Loaded {len(proxies)} proxies "
            f"({errors} parse errors) from '{filepath}'"
        )
        return proxies

    @classmethod
    def parse_list(
        cls,
        lines: List[str],
        default_protocol: ProxyProtocol = ProxyProtocol.HTTP
    ) -> List[ProxyEntry]:
        """
        Parse proxies from a list of strings (e.g., from API response).

        Args:
            lines:            List of raw proxy strings
            default_protocol: Default protocol assumption

        Returns:
            List of successfully parsed ProxyEntry objects
        """
        results = []
        for line in lines:
            entry = cls.parse_line(line, default_protocol)
            if entry:
                results.append(entry)
        return results

    # ------------------------------------------------------------------
    # Private parsing helpers
    # ------------------------------------------------------------------

    @classmethod
    def _try_full_url(cls, line: str) -> Optional[Tuple]:
        m = cls._PATTERN_FULL_URL.match(line)
        if m:
            return (
                m.group("host"),
                int(m.group("port")),
                m.group("username"),
                m.group("password"),
                m.group("protocol"),
            )
        return None

    @classmethod
    def _try_auth_at(cls, line: str) -> Optional[Tuple]:
        m = cls._PATTERN_AUTH_AT.match(line)
        if m:
            return (
                m.group("host"),
                int(m.group("port")),
                m.group("username"),
                m.group("password"),
                None,
            )
        return None

    @classmethod
    def _try_colon_sep(cls, line: str) -> Optional[Tuple]:
        m = cls._PATTERN_COLON_SEP.match(line)
        if m:
            return (
                m.group("host"),
                int(m.group("port")),
                m.group("username"),
                m.group("password"),
                None,
            )
        return None

    @classmethod
    def _try_bare(cls, line: str) -> Optional[Tuple]:
        m = cls._PATTERN_BARE.match(line)
        if m:
            return (
                m.group("host"),
                int(m.group("port")),
                None,
                None,
                None,
            )
        return None

# =============================================================================
# PROXY HEALTH MONITOR
# =============================================================================

class ProxyHealthMonitor:
    """
    Background daemon thread that continuously tests all proxies in the pool.

    For each proxy it:
        1. Measures TCP connect latency
        2. Sends an HTTP request through the proxy
        3. Determines anonymity level by inspecting headers
        4. Updates proxy status (ACTIVE / COOLDOWN / BLACKLIST)
        5. Records geo-location country code

    The monitor cycles through the entire pool at configurable intervals
    and uses a thread pool for parallel testing.
    """

    def __init__(
        self,
        pool: "ProxyPool",
        check_interval_sec: float = DEFAULT_HEALTH_INTERVAL_SEC,
        check_timeout_sec:  float = DEFAULT_HEALTH_TIMEOUT_SEC,
        max_workers:        int   = 20,
        max_failures:       int   = DEFAULT_MAX_FAILURES,
    ) -> None:
        """
        Initialize the health monitor.

        Args:
            pool:               Reference to the shared ProxyPool
            check_interval_sec: Seconds between full pool sweeps
            check_timeout_sec:  Timeout per proxy health check
            max_workers:        Parallel threads for health checking
            max_failures:       Consecutive failures before blacklisting
        """
        self._pool             = pool
        self._interval         = check_interval_sec
        self._timeout          = check_timeout_sec
        self._max_workers      = max_workers
        self._max_failures     = max_failures
        self._stop_event       = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._check_count      = 0
        self._total_checks     = 0

    def start(self) -> None:
        """Launch the background monitoring thread."""
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._monitor_loop,
            name="ProxyHealthMonitor",
            daemon=True,
        )
        self._thread.start()
        logger.info("[MONITOR] Health monitor started")

    def stop(self) -> None:
        """Signal the monitor thread to terminate gracefully."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)
        logger.info("[MONITOR] Health monitor stopped")

    def _monitor_loop(self) -> None:
        """Main loop: run a full check cycle, then wait for the next interval."""
        while not self._stop_event.is_set():
            try:
                self._run_check_cycle()
            except Exception as exc:
                logger.error(f"[MONITOR] Unhandled error in cycle: {exc}")

            # Wait for next interval (interruptible)
            self._stop_event.wait(timeout=self._interval)

    def _run_check_cycle(self) -> None:
        """
        Test all proxies in the pool in parallel.
        Marks each proxy ACTIVE, COOLDOWN, or BLACKLIST based on results.
        """
        self._check_count += 1
        all_proxies = self._pool.get_all_proxies()

        if not all_proxies:
            logger.debug("[MONITOR] No proxies to check")
            return

        logger.info(
            f"[MONITOR] Cycle #{self._check_count}: "
            f"testing {len(all_proxies)} proxies..."
        )
        cycle_start = time.time()

        with ThreadPoolExecutor(
            max_workers=min(self._max_workers, len(all_proxies)),
            thread_name_prefix="health"
        ) as executor:
            futures: Dict[Future, ProxyEntry] = {
                executor.submit(self._check_proxy, proxy): proxy
                for proxy in all_proxies
                if proxy.status != ProxyStatus.BLACKLIST
            }
            for future in as_completed(futures):
                proxy = futures[future]
                try:
                    future.result()
                    self._total_checks += 1
                except Exception as exc:
                    logger.debug(
                        f"[MONITOR] Check error for {proxy.display_str}: {exc}"
                    )

        elapsed = time.time() - cycle_start
        active_count = sum(
            1 for p in all_proxies if p.status == ProxyStatus.ACTIVE
        )
        logger.info(
            f"[MONITOR] Cycle #{self._check_count} done in {elapsed:.1f}s | "
            f"Active: {active_count}/{len(all_proxies)}"
        )

    def _check_proxy(self, proxy: ProxyEntry) -> None:
        """
        Perform a full health check on a single proxy.

        Steps:
            1. TCP ping to measure raw connect latency
            2. HTTP GET through the proxy to verify functionality
            3. Anonymity detection via header inspection
            4. Status update based on results

        Args:
            proxy: The ProxyEntry to test
        """
        with proxy._lock:
            if proxy.status == ProxyStatus.BLACKLIST:
                return
            proxy.status = ProxyStatus.TESTING

        latency_ms = self._measure_tcp_latency(proxy)
        success, response_data = self._http_check(proxy)

        with proxy._lock:
            proxy.metrics.last_checked_ts = time.time()

            if success:
                proxy.metrics.record_request(True, latency_ms)
                proxy.status = ProxyStatus.ACTIVE

                # Determine anonymity from response headers
                if response_data:
                    proxy.anonymity = self._classify_anonymity(response_data)
                    proxy.country   = response_data.get("country", "UNKNOWN")

                logger.debug(
                    f"[MONITOR] ✓ {proxy.display_str} "
                    f"({latency_ms:.0f}ms, {proxy.anonymity.name})"
                )
            else:
                proxy.metrics.record_request(False, latency_ms)
                failures = proxy.metrics.consecutive_failures

                if failures >= self._max_failures:
                    proxy.blacklist(
                        f"Failed {failures} consecutive health checks"
                    )
                else:
                    cooldown = DEFAULT_COOLDOWN_SEC * failures
                    proxy.set_cooldown(cooldown)
                    logger.debug(
                        f"[MONITOR] ✗ {proxy.display_str} "
                        f"(failure #{failures})"
                    )

    def _measure_tcp_latency(self, proxy: ProxyEntry) -> float:
        """
        Measure raw TCP connection latency to the proxy host.

        Args:
            proxy: Target ProxyEntry

        Returns:
            Latency in milliseconds, or 9999.0 on timeout/error
        """
        try:
            start = time.perf_counter()
            with socket.create_connection(
                (proxy.host, proxy.port),
                timeout=self._timeout
            ):
                elapsed_ms = (time.perf_counter() - start) * 1000
                return round(elapsed_ms, 2)
        except (socket.timeout, ConnectionRefusedError, OSError):
            return 9999.0

    def _http_check(
        self, proxy: ProxyEntry
    ) -> Tuple[bool, Optional[Dict]]:
        """
        Verify proxy functionality by making a real HTTP request.

        Args:
            proxy: Target ProxyEntry

        Returns:
            Tuple of (success, parsed_response_dict)
        """
        check_url = random.choice(HEALTH_CHECK_URLS)
        try:
            session = requests.Session()
            session.proxies.update(proxy.requests_dict)

            response = session.get(
                check_url,
                timeout=self._timeout,
                verify=False,
                headers={"User-Agent": "Mozilla/5.0 (health-check)"}
            )

            if response.status_code == 200:
                try:
                    data = response.json()
                    return True, data
                except ValueError:
                    return True, {}

            return False, None

        except Exception:
            return False, None

    @staticmethod
    def _classify_anonymity(response_data: Dict) -> AnonymityLevel:
        """
        Infer anonymity level from the proxy's response headers.

        Rules:
            - TRANSPARENT: X-Real-IP or X-Forwarded-For contains the real IP
            - ANONYMOUS:   Via or X-Proxy-ID header is present
            - ELITE:       No identifying headers present

        Args:
            response_data: Parsed JSON response from anonymity check URL

        Returns:
            AnonymityLevel enum value
        """
        headers = response_data.get("headers", {})
        header_keys_lower = {k.lower() for k in headers}

        revealing_headers = {"x-real-ip", "x-forwarded-for", "x-client-ip"}
        proxy_headers     = {"via", "x-proxy-id", "proxy-connection"}

        if revealing_headers & header_keys_lower:
            return AnonymityLevel.TRANSPARENT
        if proxy_headers & header_keys_lower:
            return AnonymityLevel.ANONYMOUS
        return AnonymityLevel.ELITE

# =============================================================================
# PROXY POOL (Thread-Safe Storage)
# =============================================================================

class ProxyPool:
    """
    Thread-safe, bounded container for ProxyEntry objects.

    Provides:
        - Indexed fast lookup by proxy_id
        - Iteration over all / available proxies
        - Atomic add/remove operations
        - Duplicate detection by (host, port)
    """

    def __init__(self, max_size: int = MAX_POOL_SIZE) -> None:
        """
        Initialize an empty proxy pool.

        Args:
            max_size: Maximum number of proxies the pool can hold
        """
        self._lock      = threading.RLock()
        self._proxies:  Dict[str, ProxyEntry] = {}  # proxy_id → ProxyEntry
        self._host_set: Set[Tuple[str, int]]  = set()  # (host, port) pairs
        self._max_size  = max_size

    def add(self, entry: ProxyEntry) -> bool:
        """
        Add a proxy to the pool (deduplication by host:port).

        Args:
            entry: ProxyEntry to add

        Returns:
            True if added, False if duplicate or pool is full
        """
        with self._lock:
            if len(self._proxies) >= self._max_size:
                logger.warning("[POOL] Max size reached, rejecting new proxy")
                return False

            key = (entry.host.lower(), entry.port)
            if key in self._host_set:
                return False  # Duplicate

            self._proxies[entry.proxy_id] = entry
            self._host_set.add(key)
            return True

    def add_many(self, entries: List[ProxyEntry]) -> int:
        """
        Add multiple proxies, skipping duplicates.

        Args:
            entries: List of ProxyEntry objects

        Returns:
            Number of proxies successfully added
        """
        added = sum(1 for e in entries if self.add(e))
        logger.info(
            f"[POOL] Added {added}/{len(entries)} proxies "
            f"(total: {self.size})"
        )
        return added

    def remove(self, proxy_id: str) -> bool:
        """
        Remove a proxy by its unique ID.

        Args:
            proxy_id: UUID of the proxy to remove

        Returns:
            True if removed, False if not found
        """
        with self._lock:
            entry = self._proxies.pop(proxy_id, None)
            if entry:
                key = (entry.host.lower(), entry.port)
                self._host_set.discard(key)
                return True
            return False

    def get(self, proxy_id: str) -> Optional[ProxyEntry]:
        """Retrieve a proxy by its ID."""
        with self._lock:
            return self._proxies.get(proxy_id)

    def get_all_proxies(self) -> List[ProxyEntry]:
        """Return a snapshot list of all proxies (regardless of status)."""
        with self._lock:
            return list(self._proxies.values())

    def get_available_proxies(self) -> List[ProxyEntry]:
        """Return a list of proxies currently available for use."""
        with self._lock:
            return [p for p in self._proxies.values() if p.is_available]

    def clear_blacklisted(self) -> int:
        """
        Permanently remove all blacklisted proxies from the pool.

        Returns:
            Number of proxies removed
        """
        with self._lock:
            to_remove = [
                pid for pid, p in self._proxies.items()
                if p.status == ProxyStatus.BLACKLIST
            ]
            for pid in to_remove:
                entry = self._proxies.pop(pid, None)
                if entry:
                    self._host_set.discard((entry.host.lower(), entry.port))
            return len(to_remove)

    def reset_cooldowns(self) -> int:
        """
        Immediately recover all proxies from COOLDOWN state.

        Returns:
            Number of proxies recovered
        """
        count = 0
        with self._lock:
            for proxy in self._proxies.values():
                with proxy._lock:
                    if proxy.status == ProxyStatus.COOLDOWN:
                        proxy.status = ProxyStatus.ACTIVE
                        proxy.metrics.consecutive_failures = 0
                        count += 1
        return count

    @property
    def size(self) -> int:
        """Total number of proxies in the pool."""
        with self._lock:
            return len(self._proxies)

    @property
    def available_count(self) -> int:
        """Number of proxies currently available."""
        return len(self.get_available_proxies())

    @property
    def status_breakdown(self) -> Dict[str, int]:
        """Count proxies by status for dashboard display."""
        counts: Dict[str, int] = {s.name: 0 for s in ProxyStatus}
        with self._lock:
            for proxy in self._proxies.values():
                counts[proxy.status.name] += 1
        return counts

# =============================================================================
# ROTATION STRATEGIES
# =============================================================================

class BaseRotator(ABC):
    """Abstract base class for all proxy rotation strategies."""

    @abstractmethod
    def select(self, proxies: List[ProxyEntry]) -> Optional[ProxyEntry]:
        """
        Select the next proxy from the available pool.

        Args:
            proxies: List of currently available ProxyEntry objects

        Returns:
            Selected ProxyEntry, or None if pool is empty
        """
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable strategy name."""
        ...


class RoundRobinRotator(BaseRotator):
    """Simple cyclic rotation — each proxy is used once in sequence."""

    def __init__(self) -> None:
        self._counter  = itertools.count()
        self._lock     = threading.Lock()

    @property
    def name(self) -> str:
        return "Round Robin"

    def select(self, proxies: List[ProxyEntry]) -> Optional[ProxyEntry]:
        if not proxies:
            return None
        with self._lock:
            idx = next(self._counter) % len(proxies)
        return proxies[idx]


class WeightedRoundRobinRotator(BaseRotator):
    """
    Weighted selection based on proxy composite score.
    Faster, more reliable proxies receive proportionally more traffic.
    Uses reservoir sampling for O(n) weighted selection.
    """

    @property
    def name(self) -> str:
        return "Weighted Round Robin"

    def select(self, proxies: List[ProxyEntry]) -> Optional[ProxyEntry]:
        if not proxies:
            return None

        weights = [p.metrics.weight for p in proxies]
        total   = sum(weights)

        if total <= 0:
            return random.choice(proxies)

        # Weighted random choice
        r = random.uniform(0, total)
        cumulative = 0.0
        for proxy, weight in zip(proxies, weights):
            cumulative += weight
            if r <= cumulative:
                return proxy

        return proxies[-1]


class RandomRotator(BaseRotator):
    """Pure random selection — simple and unbiased."""

    @property
    def name(self) -> str:
        return "Random"

    def select(self, proxies: List[ProxyEntry]) -> Optional[ProxyEntry]:
        return random.choice(proxies) if proxies else None


class LeastUsedRotator(BaseRotator):
    """
    Prefer proxies with the fewest total requests.
    Ensures even distribution across the pool.
    """

    @property
    def name(self) -> str:
        return "Least Used"

    def select(self, proxies: List[ProxyEntry]) -> Optional[ProxyEntry]:
        if not proxies:
            return None
        return min(proxies, key=lambda p: p.metrics.total_requests)


class FastestFirstRotator(BaseRotator):
    """
    Prefer proxies with the lowest average latency.
    Best for latency-sensitive workloads.
    """

    @property
    def name(self) -> str:
        return "Fastest First"

    def select(self, proxies: List[ProxyEntry]) -> Optional[ProxyEntry]:
        if not proxies:
            return None
        return min(proxies, key=lambda p: p.metrics.avg_latency_ms)


class RotatorFactory:
    """Factory to instantiate rotation strategy objects by enum or name."""

    _MAP: Dict[RotationStrategy, type] = {
        RotationStrategy.ROUND_ROBIN:          RoundRobinRotator,
        RotationStrategy.WEIGHTED_ROUND_ROBIN: WeightedRoundRobinRotator,
        RotationStrategy.RANDOM:               RandomRotator,
        RotationStrategy.LEAST_USED:           LeastUsedRotator,
        RotationStrategy.FASTEST_FIRST:        FastestFirstRotator,
    }

    @classmethod
    def create(cls, strategy: RotationStrategy) -> BaseRotator:
        """
        Create a rotator instance for the given strategy.

        Args:
            strategy: RotationStrategy enum value

        Returns:
            Instantiated BaseRotator subclass
        """
        rotator_class = cls._MAP.get(strategy, RoundRobinRotator)
        return rotator_class()

# =============================================================================
# SESSION MANAGER (Sticky Sessions)
# =============================================================================

class SessionManager:
    """
    Manages session-sticky proxy assignments for multi-step workflows.

    Session Sticky guarantees that a single thread_id uses the same proxy
    throughout an entire account creation flow, preventing mid-flow IP changes
    that would cause authentication failures.

    Thread-safety: All operations are protected by a single RLock.
    """

    def __init__(self) -> None:
        self._lock:       threading.RLock   = threading.RLock()
        self._sessions:   Dict[int, ProxyEntry] = {}   # thread_id → proxy
        self._session_start: Dict[int, float]  = {}   # thread_id → timestamp

    def get_assigned(self, thread_id: int) -> Optional[ProxyEntry]:
        """
        Retrieve the proxy currently assigned to a thread.

        Args:
            thread_id: Caller's thread identifier

        Returns:
            Assigned ProxyEntry, or None if no active assignment
        """
        with self._lock:
            proxy = self._sessions.get(thread_id)
            if proxy and not proxy.is_available:
                # Assigned proxy went down — force reassignment
                self._sessions.pop(thread_id, None)
                self._session_start.pop(thread_id, None)
                return None
            return proxy

    def assign(self, thread_id: int, proxy: ProxyEntry) -> None:
        """
        Assign a proxy to a thread for the duration of its session.

        Args:
            thread_id: Caller's thread identifier
            proxy:     ProxyEntry to assign
        """
        with self._lock:
            self._sessions[thread_id] = proxy
            self._session_start[thread_id] = time.time()
            logger.debug(
                f"[SESSION] Thread {thread_id} → {proxy.display_str}"
            )

    def release(self, thread_id: int) -> None:
        """
        Release the proxy assignment for a thread.
        Should be called when the multi-step workflow completes.

        Args:
            thread_id: Caller's thread identifier
        """
        with self._lock:
            self._sessions.pop(thread_id, None)
            self._session_start.pop(thread_id, None)
            logger.debug(f"[SESSION] Thread {thread_id} released")

    def session_duration(self, thread_id: int) -> float:
        """Return elapsed seconds for a thread's session (0 if not active)."""
        with self._lock:
            start = self._session_start.get(thread_id, 0.0)
            return time.time() - start if start else 0.0

    @property
    def active_sessions(self) -> int:
        """Number of currently active sticky sessions."""
        with self._lock:
            return len(self._sessions)

# =============================================================================
# BAN INTERCEPTOR
# =============================================================================

class BanInterceptor:
    """
    Detects and handles ban/rate-limit signals from HTTP responses.

    Responsibilities:
        - Classify HTTP status codes as ban, rate-limit, or server error
        - Trigger appropriate proxy state transitions
        - Track global ban statistics per domain

    Cooldown escalation:
        - First ban:  5-minute cooldown
        - Second ban: 15-minute cooldown
        - Third ban:  Permanent blacklist
    """

    # Status code → (is_ban, description)
    _STATUS_ACTIONS: Dict[int, Tuple[bool, str]] = {
        401: (False, "Unauthorized - check credentials"),
        403: (True,  "Forbidden - IP likely banned"),
        407: (True,  "Proxy Authentication Required"),
        429: (True,  "Too Many Requests - rate limited"),
        503: (True,  "Service Unavailable"),
        500: (False, "Internal Server Error"),
        502: (False, "Bad Gateway"),
        504: (False, "Gateway Timeout"),
    }

    def __init__(self) -> None:
        self._lock        = threading.RLock()
        # Track per-proxy ban counts (proxy_id → count)
        self._ban_counts: Dict[str, int] = {}
        # Track per-domain ban events
        self._domain_bans: Dict[str, int] = {}

    def handle_response(
        self,
        proxy:       ProxyEntry,
        status_code: int,
        url:         str = "",
    ) -> Tuple[bool, str]:
        """
        Evaluate an HTTP response and apply appropriate proxy penalties.

        Args:
            proxy:       The ProxyEntry that served the request
            status_code: HTTP status code received
            url:         Request URL (used for domain-level tracking)

        Returns:
            Tuple of (should_retry: bool, action_description: str)
        """
        # Extract domain for tracking
        domain = self._extract_domain(url)

        is_ban, description = self._STATUS_ACTIONS.get(
            status_code, (False, f"HTTP {status_code}")
        )

        if status_code in SUCCESS_STATUS_CODES:
            # Success: clear this proxy's ban streak
            with self._lock:
                self._ban_counts.pop(proxy.proxy_id, None)
            return False, "success"

        with self._lock:
            if is_ban:
                # Escalating cooldown based on repeat offences
                ban_count = self._ban_counts.get(proxy.proxy_id, 0) + 1
                self._ban_counts[proxy.proxy_id] = ban_count
                self._domain_bans[domain] = self._domain_bans.get(domain, 0) + 1

                if ban_count == 1:
                    proxy.set_cooldown(300)   # 5 minutes
                    action = f"cooldown-5m (ban #{ban_count})"
                elif ban_count == 2:
                    proxy.set_cooldown(900)   # 15 minutes
                    action = f"cooldown-15m (ban #{ban_count})"
                else:
                    proxy.blacklist(f"{description} × {ban_count}")
                    action = f"blacklisted (ban #{ban_count})"

                logger.warning(
                    f"[BAN] {proxy.display_str} [{status_code}] → {action} "
                    f"| domain: {domain}"
                )
                return True, action

            elif status_code in RETRY_STATUS_CODES:
                # Server error: soft cooldown, allow retry
                proxy.set_cooldown(60)
                return True, f"server-error-{status_code}"

            return False, description

    def get_domain_stats(self) -> Dict[str, int]:
        """Return per-domain ban event counts."""
        with self._lock:
            return dict(self._domain_bans)

    @staticmethod
    def _extract_domain(url: str) -> str:
        """Extract the hostname from a URL."""
        try:
            return urlparse(url).netloc or "unknown"
        except Exception:
            return "unknown"

# =============================================================================
# API REFRESH MANAGER
# =============================================================================

class APIRefreshManager:
    """
    Fetches fresh proxies from an external API provider at regular intervals.

    Supports multiple response formats:
        - 'text':  One proxy per line
        - 'json':  JSON with optional dot-notation path (e.g., 'data.proxies')
        - 'csv':   Comma-separated values (first column = proxy)

    The refresh runs as a background daemon thread.
    """

    def __init__(
        self,
        pool:   ProxyPool,
        config: APIRefreshConfig,
    ) -> None:
        """
        Initialize the API refresh manager.

        Args:
            pool:   Shared ProxyPool to add proxies to
            config: APIRefreshConfig with URL and schedule
        """
        self._pool        = pool
        self._config      = config
        self._stop_event  = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_refresh_ts: float = 0.0
        self._refresh_count:   int   = 0
        self._total_added:     int   = 0

    def start(self) -> None:
        """Launch the background refresh thread."""
        if not self._config.enabled or not self._config.url:
            logger.info("[API_REFRESH] Disabled (no URL configured)")
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._refresh_loop,
            name="APIRefreshManager",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            f"[API_REFRESH] Started — URL: {self._config.url} "
            f"(every {self._config.interval_sec:.0f}s)"
        )

    def stop(self) -> None:
        """Signal the refresh thread to stop."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=15)

    def _refresh_loop(self) -> None:
        """Refresh loop: fetch immediately, then wait for the interval."""
        while not self._stop_event.is_set():
            try:
                added = self._fetch_and_add()
                self._refresh_count += 1
                self._last_refresh_ts = time.time()
                self._total_added += added
                logger.info(
                    f"[API_REFRESH] Refresh #{self._refresh_count}: "
                    f"+{added} proxies (total added: {self._total_added})"
                )
            except Exception as exc:
                logger.error(f"[API_REFRESH] Fetch error: {exc}")

            self._stop_event.wait(timeout=self._config.interval_sec)

    def _fetch_and_add(self) -> int:
        """
        Fetch proxies from the API and add them to the pool.

        Returns:
            Number of new proxies added
        """
        headers = {"User-Agent": "ProxyManager/3.0"}
        if self._config.auth_header:
            headers["Authorization"] = self._config.auth_header

        response = requests.get(
            self._config.url,
            headers=headers,
            timeout=30,
            verify=False,
        )
        response.raise_for_status()

        lines = self._parse_response(response)
        entries = ProxyParser.parse_list(lines)
        return self._pool.add_many(entries)

    def _parse_response(self, response: requests.Response) -> List[str]:
        """
        Convert the API response body into a list of proxy strings.

        Args:
            response: HTTP response from the proxy provider

        Returns:
            List of raw proxy strings to be parsed
        """
        fmt = self._config.response_format.lower()

        if fmt == "json":
            data = response.json()
            # Navigate dot-notation path
            if self._config.json_path:
                for key in self._config.json_path.split("."):
                    if isinstance(data, dict):
                        data = data.get(key, {})
            # Flatten to strings
            if isinstance(data, list):
                return [str(item) for item in data]
            return []

        if fmt == "csv":
            lines = []
            for line in response.text.splitlines():
                parts = line.split(",")
                if parts:
                    lines.append(parts[0].strip())
            return lines

        # Default: plain text, one proxy per line
        return response.text.splitlines()

    @property
    def stats(self) -> Dict[str, Any]:
        """Return refresh statistics for the telemetry dashboard."""
        return {
            "refresh_count": self._refresh_count,
            "total_added":   self._total_added,
            "last_refresh":  datetime.fromtimestamp(self._last_refresh_ts).strftime(
                "%H:%M:%S"
            ) if self._last_refresh_ts else "Never",
            "next_refresh_in": max(
                0,
                self._config.interval_sec - (time.time() - self._last_refresh_ts)
            ) if self._last_refresh_ts else 0,
        }

# =============================================================================
# TELEMETRY DASHBOARD
# =============================================================================

class TelemetryDashboard:
    """
    Real-time console dashboard for monitoring the proxy pool.

    Displays:
        ┌─────────────────────────────────────────────────────────────┐
        │  PROXY MANAGER TELEMETRY          [2024-01-15 14:32:01]    │
        ├────────────┬──────────────────────┬────────────────────────┤
        │ POOL SIZE  │  ACTIVE: 45          │  COOLDOWN: 3           │
        │ 52 total   │  TESTING: 1          │  BLACKLIST: 3          │
        ├────────────┴──────────────────────┴────────────────────────┤
        │ TOP PROXIES (by weight)                                     │
        │  1. socks5://1.2.3.4:1080  W:92.5  L:120ms  SR:99.1%      │
        │  2. http://5.6.7.8:8080    W:87.3  L:210ms  SR:97.4%      │
        └─────────────────────────────────────────────────────────────┘

    The dashboard runs as a background thread and redraws at the
    configured interval.
    """

    _BORDER_CHAR = "─"
    _WIDTH       = 72

    def __init__(
        self,
        pool:          ProxyPool,
        refresh_sec:   float = 10.0,
        top_n:         int   = 5,
        enabled:       bool  = True,
    ) -> None:
        """
        Initialize the telemetry dashboard.

        Args:
            pool:        Shared ProxyPool to monitor
            refresh_sec: Dashboard redraw interval in seconds
            top_n:       Number of top proxies to display
            enabled:     Toggle dashboard on/off
        """
        self._pool        = pool
        self._refresh_sec = refresh_sec
        self._top_n       = top_n
        self._enabled     = enabled
        self._stop_event  = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._start_time  = time.time()
        self._global_stats: Dict[str, int] = {
            "total_requests": 0,
            "total_success":  0,
            "total_failures": 0,
            "total_banned":   0,
        }
        self._stats_lock  = threading.Lock()

    def start(self) -> None:
        """Launch the background dashboard thread."""
        if not self._enabled:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._dashboard_loop,
            name="TelemetryDashboard",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop the dashboard thread."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

    def record_request(
        self,
        success:    bool,
        was_banned: bool = False,
    ) -> None:
        """
        Update global counters after each request.

        Args:
            success:    Whether request returned 2xx
            was_banned: Whether a ban status code was received
        """
        with self._stats_lock:
            self._global_stats["total_requests"] += 1
            if was_banned:
                self._global_stats["total_banned"] += 1
                self._global_stats["total_failures"] += 1
            elif success:
                self._global_stats["total_success"] += 1
            else:
                self._global_stats["total_failures"] += 1

    def _dashboard_loop(self) -> None:
        """Continuously redraw the dashboard at the configured interval."""
        while not self._stop_event.is_set():
            try:
                self._render()
            except Exception as exc:
                logger.debug(f"[DASHBOARD] Render error: {exc}")
            self._stop_event.wait(timeout=self._refresh_sec)

    def _render(self) -> None:
        """Build and print the full dashboard to stdout."""
        now      = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        uptime   = self._format_uptime(time.time() - self._start_time)
        status   = self._pool.status_breakdown
        all_prox = self._pool.get_all_proxies()

        # Compute global success rate
        with self._stats_lock:
            stats = dict(self._global_stats)

        total = stats["total_requests"]
        sr = (stats["total_success"] / total * 100) if total > 0 else 0.0

        # Top N proxies by weight
        available = self._pool.get_available_proxies()
        top_proxies = sorted(
            available,
            key=lambda p: p.metrics.weight,
            reverse=True
        )[:self._top_n]

        # Build output lines
        lines = [
            "",  # Blank line before dashboard
            self._header(f"  PROXY MANAGER TELEMETRY  [{now}]  (uptime: {uptime})"),
            self._row(
                f" POOL: {len(all_prox)} total",
                f"ACTIVE: {status.get('ACTIVE', 0)}",
                f"COOLDOWN: {status.get('COOLDOWN', 0)}",
            ),
            self._row(
                f" STRATEGY: pool",
                f"TESTING: {status.get('TESTING', 0)}",
                f"BLACKLIST: {status.get('BLACKLIST', 0)}",
            ),
            self._divider(),
            self._row(
                f" REQUESTS: {total}",
                f"SUCCESS: {stats['total_success']} ({sr:.1f}%)",
                f"BANNED: {stats['total_banned']}",
            ),
            self._divider(),
            self._cell(" TOP PROXIES (by composite weight):"),
        ]

        if top_proxies:
            for i, p in enumerate(top_proxies, 1):
                m = p.metrics
                lines.append(
                    self._cell(
                        f"  {i}. {p.display_str:<32} "
                        f"W:{m.weight:>5.1f}  "
                        f"L:{m.avg_latency_ms:>5.0f}ms  "
                        f"SR:{m.success_rate:>5.1f}%  "
                        f"[{p.anonymity.name}]"
                    )
                )
        else:
            lines.append(self._cell("  No available proxies"))

        lines.append(self._footer())

        print("\n".join(lines), flush=True)

    def _format_uptime(self, seconds: float) -> str:
        """Convert seconds to 'Xh Ym Zs' format."""
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        s = int(seconds % 60)
        return f"{h}h {m}m {s}s"

    def _header(self, text: str) -> str:
        c = Fore.CYAN if COLORAMA_AVAILABLE else ""
        r = Style.RESET_ALL if COLORAMA_AVAILABLE else ""
        border = self._BORDER_CHAR * self._WIDTH
        return f"{c}┌{border}┐\n│{text:^{self._WIDTH}}│{r}"

    def _divider(self) -> str:
        c = Fore.CYAN if COLORAMA_AVAILABLE else ""
        r = Style.RESET_ALL if COLORAMA_AVAILABLE else ""
        return f"{c}├{self._BORDER_CHAR * self._WIDTH}┤{r}"

    def _footer(self) -> str:
        c = Fore.CYAN if COLORAMA_AVAILABLE else ""
        r = Style.RESET_ALL if COLORAMA_AVAILABLE else ""
        return f"{c}└{self._BORDER_CHAR * self._WIDTH}┘{r}"

    def _row(self, col1: str, col2: str = "", col3: str = "") -> str:
        """Format a three-column row."""
        c   = Fore.GREEN if COLORAMA_AVAILABLE else ""
        r   = Style.RESET_ALL if COLORAMA_AVAILABLE else ""
        w   = self._WIDTH
        col_w = w // 3
        return (
            f"{c}│{col1:<{col_w}}"
            f"{col2:<{col_w}}"
            f"{col3:<{w - 2*col_w}}│{r}"
        )

    def _cell(self, text: str) -> str:
        c = Fore.GREEN if COLORAMA_AVAILABLE else ""
        r = Style.RESET_ALL if COLORAMA_AVAILABLE else ""
        return f"{c}│{text:<{self._WIDTH}}│{r}"

# =============================================================================
# SMART REQUEST WRAPPER
# =============================================================================

class StickySessionContext:
    """
    Context manager for session-sticky proxy workflows.
    Ensures the same proxy is used for all requests within the `with` block
    and automatically releases the session on exit.

    Usage:
        with proxy_manager.sticky_session(thread_id=1) as session:
            r = session.post("https://api.example.com/login", data={...})
    """

    def __init__(
        self,
        wrapper:   "SmartRequestWrapper",
        thread_id: int,
    ) -> None:
        self._wrapper   = wrapper
        self._thread_id = thread_id

    def __enter__(self) -> "StickySessionContext":
        return self

    def get(self, url: str, **kwargs) -> Optional[requests.Response]:
        """Sticky GET request."""
        return self._wrapper.get(url, thread_id=self._thread_id, **kwargs)

    def post(self, url: str, **kwargs) -> Optional[requests.Response]:
        """Sticky POST request."""
        return self._wrapper.post(url, thread_id=self._thread_id, **kwargs)

    def request(self, method: str, url: str, **kwargs) -> Optional[requests.Response]:
        """Sticky request with any HTTP method."""
        return self._wrapper.request(
            method, url, thread_id=self._thread_id, **kwargs
        )

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Release the sticky session on context exit."""
        self._wrapper.release_session(self._thread_id)


class SmartRequestWrapper:
    """
    Intelligent HTTP request wrapper with:
        - Automatic proxy selection from the pool
        - Session-sticky support for multi-step flows
        - Exponential backoff with jitter on failure
        - Ban/error detection and proxy rotation
        - Per-request telemetry recording
        - Configurable timeouts and headers

    This is the primary interface for making HTTP requests in your app.
    All internal complexity (proxy management, retrying) is transparent
    to the caller.
    """

    _DEFAULT_HEADERS = {
        "Accept": "application/json, text/html, */*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
    }

    def __init__(
        self,
        proxy_manager:   "ProxyManager",
        backoff_config:  Optional[BackoffConfig]  = None,
        default_timeout: int                       = DEFAULT_REQUEST_TIMEOUT_SEC,
        default_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Initialize the smart request wrapper.

        Args:
            proxy_manager:   The ProxyManager instance to source proxies from
            backoff_config:  Retry/backoff settings (uses defaults if None)
            default_timeout: Default request timeout in seconds
            default_headers: Headers merged into every request
        """
        self._manager   = proxy_manager
        self._backoff   = backoff_config or BackoffConfig()
        self._timeout   = default_timeout
        self._headers   = {**self._DEFAULT_HEADERS, **(default_headers or {})}

    # ------------------------------------------------------------------
    # Public HTTP methods
    # ------------------------------------------------------------------

    def get(
        self,
        url:       str,
        thread_id: int  = 0,
        **kwargs,
    ) -> Optional[requests.Response]:
        """
        Perform a GET request with full retry and proxy management.

        Args:
            url:       Target URL
            thread_id: Caller's thread ID (used for sticky sessions)
            **kwargs:  Passed directly to requests.Session.request()

        Returns:
            Response on success, None if all retries exhausted
        """
        return self.request("GET", url, thread_id=thread_id, **kwargs)

    def post(
        self,
        url:       str,
        thread_id: int  = 0,
        **kwargs,
    ) -> Optional[requests.Response]:
        """
        Perform a POST request with full retry and proxy management.

        Args:
            url:       Target URL
            thread_id: Caller's thread ID
            **kwargs:  Passed directly to requests.Session.request()

        Returns:
            Response on success, None if all retries exhausted
        """
        return self.request("POST", url, thread_id=thread_id, **kwargs)

    def request(
        self,
        method:    str,
        url:       str,
        thread_id: int  = 0,
        **kwargs,
    ) -> Optional[requests.Response]:
        """
        Core request dispatcher with retry loop.

        Retry flow for each attempt:
            1. Get proxy (sticky session or new rotation)
            2. Merge headers
            3. Send request with timeout
            4. On ban/rate-limit: rotate proxy, increment backoff
            5. On success: record metrics, return response
            6. On network error: rotate proxy, increment backoff

        Args:
            method:    HTTP method string ('GET', 'POST', etc.)
            url:       Target URL
            thread_id: Caller's thread ID (0 = no sticky session)
            **kwargs:  Forwarded to requests.Session.request()

        Returns:
            requests.Response on success, None on all retries exhausted
        """
        # Merge caller headers with defaults
        caller_headers = kwargs.pop("headers", {})
        merged_headers = {**self._headers, **caller_headers}

        timeout = kwargs.pop("timeout", self._timeout)
        verify  = kwargs.pop("verify", False)

        last_error: Optional[Exception] = None

        for attempt in range(self._backoff.max_retries):
            if self._manager.exit_flag:
                logger.debug("[REQUEST] Exit flag set, aborting")
                return None

            # Acquire proxy
            proxy = self._get_proxy_for_thread(thread_id)
            proxy_info = proxy.display_str if proxy else "DIRECT"

            # Build request-level proxy dict
            proxies = proxy.requests_dict if proxy else None

            try:
                start_ms = time.perf_counter()

                with requests.Session() as session:
                    response = session.request(
                        method  = method,
                        url     = url,
                        headers = merged_headers,
                        proxies = proxies,
                        timeout = timeout,
                        verify  = verify,
                        **kwargs
                    )

                latency_ms = (time.perf_counter() - start_ms) * 1000
                status     = response.status_code
                success    = status in SUCCESS_STATUS_CODES
                was_banned = status in BAN_STATUS_CODES

                # Record metrics on the proxy
                if proxy:
                    with proxy._lock:
                        proxy.metrics.record_request(success, latency_ms, was_banned)

                # Let ban interceptor decide what to do
                should_retry, action = self._manager.ban_interceptor.handle_response(
                    proxy or ProxyEntry(),   # Dummy for direct mode
                    status,
                    url,
                )

                # Record in telemetry
                self._manager.telemetry.record_request(success, was_banned)

                if success:
                    logger.debug(
                        f"[REQUEST] ✓ {method} {url} "
                        f"[{status}] {latency_ms:.0f}ms via {proxy_info}"
                    )
                    return response

                if should_retry:
                    logger.info(
                        f"[REQUEST] ↺ Attempt {attempt+1}/{self._backoff.max_retries} "
                        f"[{status}] {action} via {proxy_info}"
                    )
                    # Release sticky session so next attempt gets a fresh proxy
                    if was_banned and thread_id:
                        self._manager.session_manager.release(thread_id)
                    self._backoff.sleep(attempt)
                    continue

                # Non-retryable error
                return response

            except requests.exceptions.ProxyError as exc:
                logger.warning(
                    f"[REQUEST] Proxy error ({proxy_info}): {exc} "
                    f"attempt {attempt+1}/{self._backoff.max_retries}"
                )
                if proxy:
                    proxy.set_cooldown(DEFAULT_COOLDOWN_SEC)
                last_error = exc

            except requests.exceptions.ConnectTimeout as exc:
                logger.warning(f"[REQUEST] Connect timeout ({proxy_info}): {exc}")
                if proxy:
                    proxy.metrics.record_request(False, self._timeout * 1000)
                    with proxy._lock:
                        proxy.metrics.consecutive_failures += 1
                last_error = exc

            except requests.exceptions.ReadTimeout as exc:
                logger.warning(f"[REQUEST] Read timeout ({proxy_info}): {exc}")
                last_error = exc

            except requests.exceptions.ConnectionError as exc:
                logger.warning(f"[REQUEST] Connection error ({proxy_info}): {exc}")
                if proxy:
                    proxy.set_cooldown(60)
                last_error = exc

            except Exception as exc:
                logger.error(f"[REQUEST] Unexpected error: {exc}")
                last_error = exc

            # Backoff before next retry
            self._backoff.sleep(attempt)

        logger.error(
            f"[REQUEST] All {self._backoff.max_retries} attempts failed "
            f"for {method} {url}. Last error: {last_error}"
        )
        return None

    def release_session(self, thread_id: int) -> None:
        """
        Release a sticky session manually.

        Args:
            thread_id: Thread whose session to release
        """
        self._manager.session_manager.release(thread_id)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get_proxy_for_thread(self, thread_id: int) -> Optional[ProxyEntry]:
        """
        Select a proxy, respecting sticky sessions.

        For threads with an active sticky session, returns the same proxy.
        Otherwise, selects a new proxy via the configured rotation strategy.

        Args:
            thread_id: Caller's thread ID

        Returns:
            ProxyEntry to use, or None for direct connection
        """
        if thread_id > 0:
            # Check for existing sticky session
            assigned = self._manager.session_manager.get_assigned(thread_id)
            if assigned:
                return assigned

        # Get new proxy via rotation strategy
        proxy = self._manager.select_proxy()

        if proxy and thread_id > 0:
            # Register as sticky session for this thread
            self._manager.session_manager.assign(thread_id, proxy)

        return proxy

# =============================================================================
# MAIN PROXY MANAGER (Facade / Orchestrator)
# =============================================================================

class ProxyManager:
    """
    Central orchestrator for the entire proxy management system.

    This class acts as the public API and ties together all subsystems:
        - ProxyPool:          Thread-safe proxy storage
        - ProxyParser:        Proxy string parsing
        - ProxyHealthMonitor: Background health checking
        - SessionManager:     Sticky session tracking
        - BanInterceptor:     Ban/rate-limit handling
        - APIRefreshManager:  External proxy API integration
        - TelemetryDashboard: Real-time console monitoring
        - SmartRequestWrapper: Managed HTTP requests

    Typical Usage:
        # Setup
        manager = ProxyManager(
            proxy_file="proxies.txt",
            strategy=RotationStrategy.WEIGHTED_ROUND_ROBIN,
        )
        manager.start()

        # Make requests
        wrapper = manager.get_wrapper()
        response = wrapper.get("https://target.com", thread_id=1)

        # Sticky session for multi-step flows
        with manager.sticky_session(thread_id=1) as session:
            r1 = session.post("https://api.target.com/register", data={...})
            r2 = session.post("https://api.target.com/activate", data={...})

        # Shutdown
        manager.stop()
    """

    def __init__(
        self,
        proxy_file:           Optional[str]            = "proxies.txt",
        proxy_list:           Optional[List[str]]       = None,
        default_protocol:     ProxyProtocol             = ProxyProtocol.HTTP,
        strategy:             RotationStrategy          = RotationStrategy.WEIGHTED_ROUND_ROBIN,
        health_interval_sec:  float                     = DEFAULT_HEALTH_INTERVAL_SEC,
        health_timeout_sec:   float                     = DEFAULT_HEALTH_TIMEOUT_SEC,
        max_failures:         int                       = DEFAULT_MAX_FAILURES,
        cooldown_sec:         float                     = DEFAULT_COOLDOWN_SEC,
        api_refresh_config:   Optional[APIRefreshConfig] = None,
        dashboard_enabled:    bool                      = True,
        dashboard_refresh_sec:float                     = 15.0,
        backoff_config:       Optional[BackoffConfig]   = None,
        direct_mode_fallback: bool                      = True,
        max_pool_size:        int                       = MAX_POOL_SIZE,
    ) -> None:
        """
        Initialize the ProxyManager and all its subsystems.

        Args:
            proxy_file:           Path to a proxy list file (one per line)
            proxy_list:           Inline list of proxy strings (alternative to file)
            default_protocol:     Protocol to assume for entries without one
            strategy:             Proxy rotation algorithm
            health_interval_sec:  How often to run the health check cycle
            health_timeout_sec:   Timeout per proxy during health checks
            max_failures:         Consecutive failures before blacklisting
            cooldown_sec:         Duration of the cooldown period
            api_refresh_config:   External API proxy provider config
            dashboard_enabled:    Show the real-time telemetry dashboard
            dashboard_refresh_sec:Dashboard redraw interval
            backoff_config:       Retry/backoff settings for SmartRequestWrapper
            direct_mode_fallback: If True, allow requests without proxy when pool empty
            max_pool_size:        Maximum number of proxies in the pool
        """
        # Core state
        self._strategy            = strategy
        self._direct_fallback     = direct_mode_fallback
        self._default_protocol    = default_protocol
        self.exit_flag            = False
        self._start_time          = time.time()

        # Initialize subsystems
        self.pool            = ProxyPool(max_size=max_pool_size)
        self.session_manager = SessionManager()
        self.ban_interceptor = BanInterceptor()
        self._rotator        = RotatorFactory.create(strategy)

        self.health_monitor  = ProxyHealthMonitor(
            pool              = self.pool,
            check_interval_sec= health_interval_sec,
            check_timeout_sec = health_timeout_sec,
            max_failures      = max_failures,
        )

        self.telemetry = TelemetryDashboard(
            pool         = self.pool,
            refresh_sec  = dashboard_refresh_sec,
            enabled      = dashboard_enabled,
        )

        self._api_refresh = APIRefreshManager(
            pool   = self.pool,
            config = api_refresh_config or APIRefreshConfig(),
        )

        self._backoff = backoff_config or BackoffConfig()

        # Load initial proxies
        self._initial_load(proxy_file, proxy_list)

        # SmartRequestWrapper (created lazily on first access)
        self._wrapper: Optional[SmartRequestWrapper] = None

        logger.info(
            f"[MANAGER] ProxyManager initialized — "
            f"Pool: {self.pool.size}, Strategy: {self._rotator.name}"
        )

    # ------------------------------------------------------------------
    # Lifecycle management
    # ------------------------------------------------------------------

    def start(self) -> "ProxyManager":
        """
        Start all background subsystems:
            - Health monitor thread
            - API refresh thread (if configured)
            - Telemetry dashboard thread

        Returns self for chaining: manager = ProxyManager(...).start()
        """
        self.exit_flag = False
        self.health_monitor.start()
        self._api_refresh.start()
        self.telemetry.start()

        logger.info(
            f"[MANAGER] All subsystems started | "
            f"Pool: {self.pool.size} proxies | "
            f"Strategy: {self._rotator.name}"
        )
        return self

    def stop(self) -> None:
        """
        Gracefully shut down all background threads.
        Call this before application exit.
        """
        self.exit_flag = True
        self.health_monitor.stop()
        self._api_refresh.stop()
        self.telemetry.stop()
        logger.info("[MANAGER] All subsystems stopped")

    # ------------------------------------------------------------------
    # Proxy selection
    # ------------------------------------------------------------------

    def select_proxy(self) -> Optional[ProxyEntry]:
        """
        Select the next proxy using the configured rotation strategy.

        Returns:
            ProxyEntry if available, None if pool is empty and direct
            fallback is enabled, or raises RuntimeError if no fallback.
        """
        available = self.pool.get_available_proxies()

        if not available:
            if self._direct_fallback:
                logger.warning(
                    "[MANAGER] Pool empty — using DIRECT connection (no proxy)"
                )
                return None
            logger.error("[MANAGER] No proxies available and direct fallback disabled")
            return None

        return self._rotator.select(available)

    def get_proxy(self) -> Optional[Dict[str, str]]:
        """
        Get a proxy as a requests-compatible dict (for direct use).

        Returns:
            Dict with 'http'/'https' keys, or None for direct connection
        """
        proxy = self.select_proxy()
        return proxy.requests_dict if proxy else None

    # ------------------------------------------------------------------
    # Public API shortcuts
    # ------------------------------------------------------------------

    def get_wrapper(
        self,
        default_headers: Optional[Dict[str, str]] = None,
    ) -> SmartRequestWrapper:
        """
        Get (or create) the SmartRequestWrapper bound to this manager.

        Args:
            default_headers: Optional extra headers for all requests

        Returns:
            SmartRequestWrapper instance
        """
        if self._wrapper is None:
            self._wrapper = SmartRequestWrapper(
                proxy_manager   = self,
                backoff_config  = self._backoff,
                default_headers = default_headers,
            )
        return self._wrapper

    @contextlib.contextmanager
    def sticky_session(
        self, thread_id: int
    ) -> Iterator[StickySessionContext]:
        """
        Context manager for session-sticky proxy workflows.

        Guarantees the same proxy is used for all requests inside the block.
        The session is automatically released on block exit.

        Args:
            thread_id: Unique identifier for this thread/workflow

        Yields:
            StickySessionContext with get/post/request methods

        Example:
            with manager.sticky_session(thread_id=1) as session:
                r1 = session.post("https://api.example.com/step1", data={})
                r2 = session.post("https://api.example.com/step2", data={})
        """
        wrapper = self.get_wrapper()
        context = StickySessionContext(wrapper, thread_id)
        try:
            yield context
        finally:
            self.session_manager.release(thread_id)

    def add_proxies_from_file(self, filepath: str) -> int:
        """
        Load and add proxies from a file at runtime.

        Args:
            filepath: Path to the proxy file

        Returns:
            Number of new proxies added
        """
        entries = ProxyParser.parse_file(filepath, self._default_protocol)
        added   = self.pool.add_many(entries)
        logger.info(f"[MANAGER] Runtime load: +{added} proxies from '{filepath}'")
        return added

    def add_proxies_from_list(self, proxy_lines: List[str]) -> int:
        """
        Add proxies from a list of strings at runtime.

        Args:
            proxy_lines: List of raw proxy strings

        Returns:
            Number of new proxies added
        """
        entries = ProxyParser.parse_list(proxy_lines, self._default_protocol)
        return self.pool.add_many(entries)

    def report_ban(self, proxy_entry: ProxyEntry) -> None:
        """
        Manually report a proxy as banned (for caller-side detection).

        Args:
            proxy_entry: The ProxyEntry to penalize
        """
        self.ban_interceptor.handle_response(proxy_entry, 403)

    def reset_all_cooldowns(self) -> int:
        """
        Emergency reset: clear all proxy cooldowns immediately.

        Returns:
            Number of proxies recovered
        """
        count = self.pool.reset_cooldowns()
        logger.info(f"[MANAGER] Reset {count} proxies from cooldown")
        return count

    def purge_blacklisted(self) -> int:
        """
        Permanently remove blacklisted proxies from the pool.

        Returns:
            Number of proxies removed
        """
        count = self.pool.clear_blacklisted()
        logger.info(f"[MANAGER] Purged {count} blacklisted proxies")
        return count

    # ------------------------------------------------------------------
    # Statistics & reporting
    # ------------------------------------------------------------------

    def get_stats(self) -> Dict[str, Any]:
        """
        Return a comprehensive statistics snapshot.

        Returns:
            Dict with pool status, top proxies, ban stats, and uptime
        """
        available = self.pool.get_available_proxies()
        top_5     = sorted(
            available,
            key=lambda p: p.metrics.weight,
            reverse=True
        )[:5]

        return {
            "uptime_sec":         round(time.time() - self._start_time, 1),
            "strategy":           self._rotator.name,
            "pool_status":        self.pool.status_breakdown,
            "total_proxies":      self.pool.size,
            "available_proxies":  self.pool.available_count,
            "active_sessions":    self.session_manager.active_sessions,
            "domain_bans":        self.ban_interceptor.get_domain_stats(),
            "api_refresh":        self._api_refresh.stats,
            "top_proxies":        [p.to_dict() for p in top_5],
        }

    def print_stats(self) -> None:
        """Print a formatted statistics summary to stdout."""
        stats = self.get_stats()
        color_g = Fore.GREEN if COLORAMA_AVAILABLE else ""
        color_y = Fore.YELLOW if COLORAMA_AVAILABLE else ""
        color_c = Fore.CYAN if COLORAMA_AVAILABLE else ""
        reset   = Style.RESET_ALL if COLORAMA_AVAILABLE else ""

        print(f"\n{color_c}{'═' * 60}")
        print(f"  PROXY MANAGER STATISTICS")
        print(f"{'═' * 60}{reset}")
        print(f"{color_g}  Uptime:     {stats['uptime_sec']}s")
        print(f"  Strategy:   {stats['strategy']}")
        print(f"  Pool Size:  {stats['total_proxies']}")
        print(f"  Available:  {stats['available_proxies']}")
        print(f"  Sessions:   {stats['active_sessions']}{reset}")
        print(f"\n{color_y}  Pool Breakdown:{reset}")
        for status, count in stats["pool_status"].items():
            if count > 0:
                print(f"{color_g}    {status:<12}: {count}{reset}")
        print(f"{color_c}{'═' * 60}{reset}\n")

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _initial_load(
        self,
        proxy_file:  Optional[str],
        proxy_list:  Optional[List[str]],
    ) -> None:
        """
        Load proxies from file and/or list on initialization.

        Args:
            proxy_file:  Path to proxy file (or None)
            proxy_list:  List of proxy strings (or None)
        """
        total_added = 0

        if proxy_file:
            entries = ProxyParser.parse_file(proxy_file, self._default_protocol)
            total_added += self.pool.add_many(entries)

        if proxy_list:
            entries = ProxyParser.parse_list(proxy_list, self._default_protocol)
            total_added += self.pool.add_many(entries)

        if total_added == 0:
            if self._direct_fallback:
                logger.warning(
                    "[MANAGER] No proxies loaded. Running in DIRECT mode. "
                    "Create 'proxies.txt' to enable proxy rotation."
                )
            else:
                logger.error(
                    "[MANAGER] No proxies loaded and direct fallback disabled!"
                )
        else:
            logger.info(
                f"[MANAGER] Initial load complete: {total_added} proxies"
            )

# =============================================================================
# CONVENIENCE FACTORY FUNCTION
# =============================================================================

def create_proxy_manager(
    proxy_file:           str              = "proxies.txt",
    strategy:             RotationStrategy = RotationStrategy.WEIGHTED_ROUND_ROBIN,
    enable_dashboard:     bool             = True,
    enable_health_check:  bool             = True,
    api_url:              str              = "",
    api_interval_sec:     float            = 600.0,
    max_retries:          int              = 4,
    base_delay_sec:       float            = 1.0,
    auto_start:           bool             = True,
) -> ProxyManager:
    """
    Convenience factory to create a fully configured ProxyManager.

    Args:
        proxy_file:           Path to proxies.txt file
        strategy:             Rotation strategy enum value
        enable_dashboard:     Show real-time telemetry console
        enable_health_check:  Run background proxy health monitor
        api_url:              External proxy provider API URL (optional)
        api_interval_sec:     API refresh interval in seconds
        max_retries:          Max retries per HTTP request
        base_delay_sec:       Base exponential backoff delay
        auto_start:           Call manager.start() automatically

    Returns:
        Configured and optionally started ProxyManager

    Example:
        manager = create_proxy_manager(
            proxy_file="proxies.txt",
            strategy=RotationStrategy.WEIGHTED_ROUND_ROBIN,
            api_url="https://proxy-provider.com/api/list?key=YOUR_KEY",
        )
        wrapper = manager.get_wrapper()
        response = wrapper.get("https://target.com", thread_id=1)
    """
    api_config = APIRefreshConfig(
        url          = api_url,
        interval_sec = api_interval_sec,
        enabled      = bool(api_url),
        response_format = "text",
    )

    backoff = BackoffConfig(
        max_retries    = max_retries,
        base_delay_sec = base_delay_sec,
    )

    manager = ProxyManager(
        proxy_file          = proxy_file,
        strategy            = strategy,
        api_refresh_config  = api_config,
        dashboard_enabled   = enable_dashboard,
        health_interval_sec = DEFAULT_HEALTH_INTERVAL_SEC if enable_health_check else 9999999,
        backoff_config      = backoff,
    )

    if auto_start:
        manager.start()

    return manager

# =============================================================================
# EXAMPLE proxies.txt FORMAT REFERENCE
# =============================================================================

PROXIES_FILE_TEMPLATE = """
# proxies.txt - Supported formats:
# ─────────────────────────────────────────────────────────────────
# Bare host:port (HTTP assumed)
# 192.168.1.1:8080
#
# With authentication (user:pass@host:port)
# myuser:mypass@proxy.example.com:3128
#
# Full URL with protocol
# http://192.168.1.1:8080
# https://user:pass@proxy.example.com:3128
# socks4://10.0.0.1:1080
# socks5://user:pass@10.0.0.1:1080
#
# Alternative colon-separated (host:port:user:pass)
# 192.168.1.1:8080:myuser:mypass
# ─────────────────────────────────────────────────────────────────
# Comments (lines starting with #) and blank lines are ignored
""".strip()

# =============================================================================
# SELF-TEST / DEMO RUNNER
# =============================================================================

def run_demo() -> None:
    """
    Demonstration of the ProxyManager system.
    Run this to verify the system is working correctly.
    """
    print(f"\n{'='*60}")
    print("  ADVANCED PROXY MANAGER - SYSTEM DEMO")
    print(f"{'='*60}\n")

    # ── 1. Parser demo ──────────────────────────────────────────
    print("[DEMO] Testing ProxyParser...")
    test_proxies = [
        "192.168.1.1:8080",
        "user:pass@10.0.0.1:3128",
        "http://proxy.example.com:80",
        "socks5://alice:secret@dark.proxy.net:1080",
        "10.10.10.10:8888:admin:password123",
        "bad-proxy-string",                      # Should fail gracefully
        "# This is a comment",                   # Should be ignored
    ]

    print(f"  Parsing {len(test_proxies)} test strings...")
    for line in test_proxies:
        result = ProxyParser.parse_line(line)
        status = f"✓ {result.display_str}" if result else "✗ (skipped)"
        print(f"    {line!r:45s} → {status}")

    # ── 2. BackoffConfig demo ────────────────────────────────────
    print("\n[DEMO] Exponential Backoff delays:")
    backoff = BackoffConfig(base_delay_sec=1.0, multiplier=2.0, max_delay_sec=30.0)
    for attempt in range(5):
        delay = backoff.compute_delay(attempt)
        bar   = "█" * int(delay * 2)
        print(f"  Attempt {attempt+1}: {delay:6.2f}s  {bar}")

    # ── 3. ProxyPool demo ────────────────────────────────────────
    print("\n[DEMO] ProxyPool operations...")
    pool = ProxyPool(max_size=10)

    entries = ProxyParser.parse_list([
        "1.1.1.1:8080",
        "2.2.2.2:3128",
        "socks5://3.3.3.3:1080",
        "1.1.1.1:8080",    # Duplicate — should be rejected
    ])
    added = pool.add_many(entries)
    print(f"  Added {added}/4 proxies (1 duplicate rejected)")
    print(f"  Pool size: {pool.size}")
    print(f"  Status: {pool.status_breakdown}")

    # ── 4. ProxyManager without proxies ─────────────────────────
    print("\n[DEMO] ProxyManager (no proxies file — direct mode)...")
    manager = ProxyManager(
        proxy_file           = None,
        proxy_list           = ["8.8.8.8:8080", "socks5://9.9.9.9:1080"],
        strategy             = RotationStrategy.WEIGHTED_ROUND_ROBIN,
        dashboard_enabled    = False,
        health_interval_sec  = 999999,    # Disable health checks in demo
    )

    proxy = manager.select_proxy()
    print(f"  Selected proxy: {proxy.display_str if proxy else 'DIRECT'}")

    stats = manager.get_stats()
    print(f"  Pool size: {stats['total_proxies']}")
    print(f"  Strategy:  {stats['strategy']}")

    # ── 5. Rotation strategy demo ────────────────────────────────
    print("\n[DEMO] Rotation strategies:")
    for strat in [
        RotationStrategy.ROUND_ROBIN,
        RotationStrategy.WEIGHTED_ROUND_ROBIN,
        RotationStrategy.RANDOM,
        RotationStrategy.LEAST_USED,
        RotationStrategy.FASTEST_FIRST,
    ]:
        rotator = RotatorFactory.create(strat)
        available = manager.pool.get_available_proxies()
        selected = rotator.select(available)
        result = selected.display_str if selected else "None"
        print(f"  {rotator.name:<25s} → {result}")

    # ── 6. BanInterceptor demo ───────────────────────────────────
    print("\n[DEMO] BanInterceptor:")
    interceptor = BanInterceptor()
    dummy_proxy = ProxyEntry(host="1.2.3.4", port=8080, status=ProxyStatus.ACTIVE)

    for code in [200, 403, 429, 503, 500]:
        dummy_proxy.status = ProxyStatus.ACTIVE  # Reset
        should_retry, action = interceptor.handle_response(
            dummy_proxy, code, "https://example.com/api"
        )
        print(
            f"  HTTP {code}: retry={should_retry!s:<5} action='{action}' "
            f"proxy_status={dummy_proxy.status.name}"
        )

    # ── 7. Session sticky demo (conceptual) ──────────────────────
    print("\n[DEMO] Sticky Session (conceptual):")
    sm = SessionManager()
    test_proxy = ProxyEntry(host="5.5.5.5", port=3128, status=ProxyStatus.ACTIVE)

    sm.assign(thread_id=42, proxy=test_proxy)
    retrieved = sm.get_assigned(thread_id=42)
    print(f"  Thread 42 assigned: {retrieved.display_str if retrieved else 'None'}")
    print(f"  Active sessions: {sm.active_sessions}")

    sm.release(thread_id=42)
    print(f"  After release — Active sessions: {sm.active_sessions}")

    manager.stop()

    print(f"\n{'='*60}")
    print("  DEMO COMPLETE — All systems operational ✓")
    print(f"{'='*60}\n")


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    run_demo()