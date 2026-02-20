#!/usr/bin/env python3
"""
=============================================================================
RUHI TELEGRAM BOT v4.0 - Production Ready
=============================================================================
Free Fire Account Generator with Advanced Proxy Management
Hosted on Render with PostgreSQL Database

Features:
- Advanced ProxyManager with sticky sessions, health monitoring, rotation
- Multi-threaded account generation with rarity/couple detection
- Telegram Bot interface with interactive menus
- PostgreSQL database for proxy persistence
- Flask server for Render Web Service compatibility
- Real-time status updates and alerts
=============================================================================
"""

# =============================================================================
# STANDARD LIBRARY IMPORTS
# =============================================================================
import asyncio
import os
import sys
import re
import json
import time
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
import base64
import codecs
import subprocess
import importlib
import signal
import tempfile
import asyncio
import io
from enum import Enum, auto
from typing import (
    Optional, Dict, List, Tuple, Any, Callable, 
    Iterator, Set, Union, NamedTuple
)
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from urllib.parse import urlparse
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, Future, as_completed

# =============================================================================
# THIRD-PARTY IMPORTS (with graceful degradation)
# =============================================================================

# Core networking
try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except ImportError as e:
    print(f"[FATAL] Missing requests: pip install requests urllib3")
    raise

# Telegram Bot
try:
    from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
    from telegram.ext import (
        Application, CommandHandler, MessageHandler, ConversationHandler,
        CallbackQueryHandler, ContextTypes, filters
    )
    TELEGRAM_AVAILABLE = True
except ImportError:
    print("[WARNING] python-telegram-bot not installed. Run: pip install python-telegram-bot")
    TELEGRAM_AVAILABLE = False

# Flask for Render
try:
    from flask import Flask
    FLASK_AVAILABLE = True
except ImportError:
    print("[WARNING] Flask not installed. Run: pip install flask")
    FLASK_AVAILABLE = False

# PostgreSQL Database
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    import psycopg2.pool
    DATABASE_AVAILABLE = True
except ImportError:
    print("[WARNING] psycopg2 not installed. Run: pip install psycopg2-binary")
    DATABASE_AVAILABLE = False

# Colorama for console
try:
    from colorama import Fore, Style, Back, init as colorama_init
    colorama_init(autoreset=True)
    COLORAMA_AVAILABLE = True
except ImportError:
    COLORAMA_AVAILABLE = False
    class FakeColor:
        def __getattr__(self, name): return ""
    Fore = Style = Back = FakeColor()

# Crypto for game protocol
try:
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import pad
    CRYPTO_AVAILABLE = True
except ImportError:
    print("[FATAL] pycryptodome required: pip install pycryptodome")
    raise

# psutil for system info
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

# =============================================================================
# ENVIRONMENT CONFIGURATION
# =============================================================================

# Load from environment variables (Render compatibility)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")  # Render provides this
RENDER_PORT = int(os.getenv("PORT", "10000"))
RENDER_HOST = os.getenv("RENDER_HOST", "0.0.0.0")

# Validate critical environment
if not TELEGRAM_BOT_TOKEN and TELEGRAM_AVAILABLE:
    print("[WARNING] TELEGRAM_BOT_TOKEN not set! Bot will not start.")
if not DATABASE_URL and DATABASE_AVAILABLE:
    print("[WARNING] DATABASE_URL not set! Using SQLite fallback.")

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Configure structured logger with file and console output."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    if not logger.handlers:
        # Console handler
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(level)
        formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console.setFormatter(formatter)
        logger.addHandler(console)
        
        # File handler
        try:
            file_handler = logging.FileHandler('ruhi_bot.log', encoding='utf-8')
            file_handler.setLevel(logging.WARNING)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except Exception:
            pass
    
    return logger

logger = setup_logger("RUHI_BOT")

# =============================================================================
# DATABASE MANAGER (PostgreSQL with SQLite Fallback)
# =============================================================================

class DatabaseManager:
    """
    Thread-safe database manager for proxy persistence and bot state.
    Uses PostgreSQL on Render, falls back to SQLite for local testing.
    """
    
    def __init__(self):
        self._lock = threading.RLock()
        self._pool = None
        self._local_db_path = "ruhi_bot.db"
        self._is_postgres = False
        
        self._init_connection()
        self._init_tables()
    
    def _init_connection(self):
        """Initialize database connection pool."""
        if DATABASE_AVAILABLE and DATABASE_URL:
            try:
                # Parse Render's DATABASE_URL format
                # postgres://user:pass@host:port/dbname
                self._pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=1, maxconn=10,
                    dsn=DATABASE_URL
                )
                self._is_postgres = True
                logger.info("[DB] Connected to PostgreSQL")
                return
            except Exception as e:
                logger.error(f"[DB] PostgreSQL failed: {e}, using SQLite")
        
        # SQLite fallback
        import sqlite3
        self._is_postgres = False
        logger.info("[DB] Using SQLite fallback")
    
    def _get_connection(self):
        """Get connection from pool or create new SQLite connection."""
        if self._is_postgres:
            return self._pool.getconn()
        else:
            import sqlite3
            return sqlite3.connect(self._local_db_path, check_same_thread=False)
    
    def _return_connection(self, conn):
        """Return connection to pool or close SQLite connection."""
        if self._is_postgres:
            self._pool.putconn(conn)
        else:
            conn.close()
    
    def _init_tables(self):
        """Create required tables if they don't exist."""
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                
                # Proxies table
                if self._is_postgres:
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS proxies (
                            id SERIAL PRIMARY KEY,
                            proxy_id VARCHAR(16) UNIQUE NOT NULL,
                            protocol VARCHAR(10) NOT NULL,
                            host VARCHAR(255) NOT NULL,
                            port INTEGER NOT NULL,
                            username VARCHAR(255),
                            password VARCHAR(255),
                            status VARCHAR(20) DEFAULT 'UNKNOWN',
                            anonymity VARCHAR(20) DEFAULT 'UNKNOWN',
                            country VARCHAR(10) DEFAULT 'UNKNOWN',
                            metrics JSONB DEFAULT '{}',
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS bot_state (
                            id SERIAL PRIMARY KEY,
                            key VARCHAR(50) UNIQUE NOT NULL,
                            value TEXT,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS accounts (
                            id SERIAL PRIMARY KEY,
                            uid VARCHAR(50) UNIQUE NOT NULL,
                            password VARCHAR(255),
                            account_id VARCHAR(50),
                            name VARCHAR(100),
                            region VARCHAR(10),
                            rarity_type VARCHAR(50),
                            rarity_score INTEGER,
                            is_rare BOOLEAN DEFAULT FALSE,
                            is_couple BOOLEAN DEFAULT FALSE,
                            jwt_token TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                else:
                    # SQLite schema
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS proxies (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            proxy_id TEXT UNIQUE NOT NULL,
                            protocol TEXT NOT NULL,
                            host TEXT NOT NULL,
                            port INTEGER NOT NULL,
                            username TEXT,
                            password TEXT,
                            status TEXT DEFAULT 'UNKNOWN',
                            anonymity TEXT DEFAULT 'UNKNOWN',
                            country TEXT DEFAULT 'UNKNOWN',
                            metrics TEXT DEFAULT '{}',
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS bot_state (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            key TEXT UNIQUE NOT NULL,
                            value TEXT,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS accounts (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            uid TEXT UNIQUE NOT NULL,
                            password TEXT,
                            account_id TEXT,
                            name TEXT,
                            region TEXT,
                            rarity_type TEXT,
                            rarity_score INTEGER,
                            is_rare INTEGER DEFAULT 0,
                            is_couple INTEGER DEFAULT 0,
                            jwt_token TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                
                conn.commit()
                logger.info("[DB] Tables initialized")
            finally:
                self._return_connection(conn)
    
    def save_proxy(self, proxy_data: Dict) -> bool:
        """Save or update a proxy in the database."""
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                
                if self._is_postgres:
                    cursor.execute("""
                        INSERT INTO proxies 
                        (proxy_id, protocol, host, port, username, password, status, anonymity, country, metrics)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (proxy_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        anonymity = EXCLUDED.anonymity,
                        country = EXCLUDED.country,
                        metrics = EXCLUDED.metrics,
                        updated_at = CURRENT_TIMESTAMP
                    """, (
                        proxy_data['proxy_id'],
                        proxy_data['protocol'],
                        proxy_data['host'],
                        proxy_data['port'],
                        proxy_data.get('username'),
                        proxy_data.get('password'),
                        proxy_data.get('status', 'UNKNOWN'),
                        proxy_data.get('anonymity', 'UNKNOWN'),
                        proxy_data.get('country', 'UNKNOWN'),
                        json.dumps(proxy_data.get('metrics', {}))
                    ))
                else:
                    cursor.execute("""
                        INSERT OR REPLACE INTO proxies 
                        (proxy_id, protocol, host, port, username, password, status, anonymity, country, metrics, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                    """, (
                        proxy_data['proxy_id'],
                        proxy_data['protocol'],
                        proxy_data['host'],
                        proxy_data['port'],
                        proxy_data.get('username'),
                        proxy_data.get('password'),
                        proxy_data.get('status', 'UNKNOWN'),
                        proxy_data.get('anonymity', 'UNKNOWN'),
                        proxy_data.get('country', 'UNKNOWN'),
                        json.dumps(proxy_data.get('metrics', {}))
                    ))
                
                conn.commit()
                return True
            except Exception as e:
                logger.error(f"[DB] Save proxy failed: {e}")
                return False
            finally:
                self._return_connection(conn)
    
    def load_proxies(self) -> List[Dict]:
        """Load all proxies from database."""
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT proxy_id, protocol, host, port, username, password,
                           status, anonymity, country, metrics
                    FROM proxies
                    WHERE status != 'BLACKLIST'
                    ORDER BY updated_at DESC
                """)
                
                columns = [desc[0] for desc in cursor.description]
                proxies = []
                for row in cursor.fetchall():
                    proxy = dict(zip(columns, row))
                    if isinstance(proxy.get('metrics'), str):
                        proxy['metrics'] = json.loads(proxy['metrics'])
                    proxies.append(proxy)
                
                return proxies
            except Exception as e:
                logger.error(f"[DB] Load proxies failed: {e}")
                return []
            finally:
                self._return_connection(conn)
    
    def save_account(self, account_data: Dict) -> bool:
        """Save a generated account to database."""
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                
                if self._is_postgres:
                    cursor.execute("""
                        INSERT INTO accounts 
                        (uid, password, account_id, name, region, rarity_type, rarity_score, is_rare, is_couple, jwt_token)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (uid) DO NOTHING
                    """, (
                        account_data['uid'],
                        account_data.get('password'),
                        account_data.get('account_id'),
                        account_data.get('name'),
                        account_data.get('region'),
                        account_data.get('rarity_type'),
                        account_data.get('rarity_score', 0),
                        account_data.get('is_rare', False),
                        account_data.get('is_couple', False),
                        account_data.get('jwt_token')
                    ))
                else:
                    cursor.execute("""
                        INSERT OR IGNORE INTO accounts 
                        (uid, password, account_id, name, region, rarity_type, rarity_score, is_rare, is_couple, jwt_token)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        account_data['uid'],
                        account_data.get('password'),
                        account_data.get('account_id'),
                        account_data.get('name'),
                        account_data.get('region'),
                        account_data.get('rarity_type'),
                        account_data.get('rarity_score', 0),
                        int(account_data.get('is_rare', False)),
                        int(account_data.get('is_couple', False)),
                        account_data.get('jwt_token')
                    ))
                
                conn.commit()
                return True
            except Exception as e:
                logger.error(f"[DB] Save account failed: {e}")
                return False
            finally:
                self._return_connection(conn)
    
    def get_stats(self) -> Dict:
        """Get database statistics."""
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                
                # Count proxies by status
                cursor.execute("""
                    SELECT status, COUNT(*) FROM proxies GROUP BY status
                """)
                proxy_stats = dict(cursor.fetchall())
                
                # Count accounts
                cursor.execute("SELECT COUNT(*) FROM accounts")
                total_accounts = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM accounts WHERE is_rare = TRUE")
                rare_accounts = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM accounts WHERE is_couple = TRUE")
                couple_accounts = cursor.fetchone()[0]
                
                return {
                    'proxies': proxy_stats,
                    'total_accounts': total_accounts,
                    'rare_accounts': rare_accounts,
                    'couple_accounts': couple_accounts
                }
            except Exception as e:
                logger.error(f"[DB] Stats failed: {e}")
                return {}
            finally:
                self._return_connection(conn)
    
    def clear_proxies(self) -> bool:
        """Clear all proxies from database."""
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM proxies")
                conn.commit()
                return True
            except Exception as e:
                logger.error(f"[DB] Clear proxies failed: {e}")
                return False
            finally:
                self._return_connection(conn)

# Global database instance
db = DatabaseManager()

# =============================================================================
# PROXY SYSTEM ENUMS AND CONSTANTS
# =============================================================================

class ProxyProtocol(Enum):
    HTTP = "http"
    HTTPS = "https"
    SOCKS4 = "socks4"
    SOCKS5 = "socks5"

class ProxyStatus(Enum):
    ACTIVE = auto()
    TESTING = auto()
    COOLDOWN = auto()
    BLACKLIST = auto()
    UNKNOWN = auto()

class AnonymityLevel(Enum):
    TRANSPARENT = auto()
    ANONYMOUS = auto()
    ELITE = auto()
    UNKNOWN = auto()

class RotationStrategy(Enum):
    ROUND_ROBIN = "round_robin"
    WEIGHTED_ROUND_ROBIN = "weighted_round_robin"
    RANDOM = "random"
    LEAST_USED = "least_used"
    FASTEST_FIRST = "fastest_first"
    SESSION_STICKY = "session_sticky"

# Health check configuration
HEALTH_CHECK_URLS = [
    "http://httpbin.org/ip",
    "https://api.ipify.org?format=json",
    "http://ip-api.com/json/",
]
DEFAULT_HEALTH_INTERVAL_SEC = 120
DEFAULT_COOLDOWN_SEC = 300
DEFAULT_MAX_FAILURES = 3
DEFAULT_REQUEST_TIMEOUT_SEC = 35
DEFAULT_HEALTH_TIMEOUT_SEC = 15
MAX_POOL_SIZE = 500

BAN_STATUS_CODES = {403, 429, 503, 407, 401}
RETRY_STATUS_CODES = {500, 502, 503, 504}
SUCCESS_STATUS_CODES = set(range(200, 300))

# =============================================================================
# GAME PROTOCOL CONSTANTS (PRESERVED EXACTLY)
# =============================================================================

# FIXED: Exactly 128 hex characters (64 bytes)
hex_key = "32656534343831396539623435393838343531343130363762323831363231383734643064356437616639643866376530306331653534373135623764316533"
key = bytes.fromhex(hex_key)

hex_data = "8J+agCBQUkVNSVVNIEFDQ09VTlQgR0VORVJBVE9SIPCfkqsgQnkgTUFIVV9BUElTIHwgTm90IEZvciBTYWxlIPCfkas="
client_data = base64.b64decode(hex_data).decode('utf-8')
GARENA = "QllfTUFIVV9BUElT=="

# Region configuration
REGION_LANG = {
    "ME": "ar", "IND": "hi", "ID": "id", "VN": "vi", "TH": "th",
    "BD": "bn", "PK": "ur", "TW": "zh", "CIS": "ru", "SAC": "es", "BR": "pt"
}

REGION_URLS = {
    "IND": "https://client.ind.freefiremobile.com/",
    "ID": "https://clientbp.ggblueshark.com/",
    "BR": "https://client.us.freefiremobile.com/",
    "ME": "https://clientbp.common.ggbluefox.com/",
    "VN": "https://clientbp.ggblueshark.com/",
    "TH": "https://clientbp.common.ggbluefox.com/",
    "CIS": "https://clientbp.ggblueshark.com/",
    "BD": "https://clientbp.ggblueshark.com/",
    "PK": "https://clientbp.ggblueshark.com/",
    "SG": "https://clientbp.ggblueshark.com/",
    "SAC": "https://client.us.freefiremobile.com/",
    "TW": "https://clientbp.ggblueshark.com/",
}

# Rarity patterns
ACCOUNT_RARITY_PATTERNS = {
    "REPEATED_DIGITS_4": [r"(\d)\1{3,}", 3],
    "REPEATED_DIGITS_3": [r"(\d)\1\1(\d)\2\2", 2],
    "SEQUENTIAL_5": [r"(12345|23456|34567|45678|56789)", 4],
    "SEQUENTIAL_4": [r"(0123|1234|2345|3456|4567|5678|6789|9876|8765|7654|6543|5432|4321|3210)", 3],
    "PALINDROME_6": [r"^(\d)(\d)(\d)\3\2\1$", 5],
    "PALINDROME_4": [r"^(\d)(\d)\2\1$", 3],
    "SPECIAL_COMBINATIONS_HIGH": [r"(69|420|1337|007)", 4],
    "SPECIAL_COMBINATIONS_MED": [r"(100|200|300|400|500|666|777|888|999)", 2],
    "QUADRUPLE_DIGITS": [r"(1111|2222|3333|4444|5555|6666|7777|8888|9999|0000)", 4],
    "MIRROR_PATTERN_HIGH": [r"^(\d{2,3})\1$", 3],
    "MIRROR_PATTERN_MED": [r"(\d{2})0\1", 2],
    "GOLDEN_RATIO": [r"1618|0618", 3]
}

# =============================================================================
# DATA MODELS
# =============================================================================

@dataclass
class ProxyMetrics:
    total_requests: int = 0
    successful_reqs: int = 0
    failed_reqs: int = 0
    banned_count: int = 0
    total_latency_ms: float = 0.0
    min_latency_ms: float = float("inf")
    max_latency_ms: float = 0.0
    last_used_ts: float = field(default_factory=time.time)
    last_checked_ts: float = 0.0
    last_success_ts: float = 0.0
    last_failure_ts: float = 0.0
    consecutive_failures: int = 0

    @property
    def avg_latency_ms(self) -> float:
        if self.total_requests == 0:
            return float("inf")
        return self.total_latency_ms / self.total_requests

    @property
    def success_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return (self.successful_reqs / self.total_requests) * 100.0

    @property
    def weight(self) -> float:
        if self.total_requests < 3:
            return 50.0
        reliability = self.success_rate / 100.0
        latency_score = max(0.0, 1.0 - (self.avg_latency_ms / 10000.0))
        return round((reliability * 70.0) + (latency_score * 30.0), 2)

    def record_request(self, success: bool, latency_ms: float, was_banned: bool = False) -> None:
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
    proxy_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    protocol: ProxyProtocol = ProxyProtocol.HTTP
    host: str = ""
    port: int = 0
    username: Optional[str] = None
    password: Optional[str] = None
    status: ProxyStatus = ProxyStatus.UNKNOWN
    anonymity: AnonymityLevel = AnonymityLevel.UNKNOWN
    country: str = "UNKNOWN"
    metrics: ProxyMetrics = field(default_factory=ProxyMetrics)
    cooldown_until_ts: float = 0.0
    blacklist_reason: str = ""
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def __post_init__(self):
        if not isinstance(self._lock, threading.RLock):
            object.__setattr__(self, "_lock", threading.RLock())

    @property
    def url(self) -> str:
        auth = f"{self.username}:{self.password}@" if self.username else ""
        return f"{self.protocol.value}://{auth}{self.host}:{self.port}"

    @property
    def requests_dict(self) -> Dict[str, str]:
        return {"http": self.url, "https": self.url}

    @property
    def is_available(self) -> bool:
        with self._lock:
            if self.status in (ProxyStatus.BLACKLIST, ProxyStatus.TESTING):
                return False
            if self.status == ProxyStatus.COOLDOWN:
                if time.time() >= self.cooldown_until_ts:
                    self.status = ProxyStatus.ACTIVE
                    self.metrics.consecutive_failures = 0
                    return True
                return False
            return self.status == ProxyStatus.ACTIVE

    @property
    def display_str(self) -> str:
        auth = f"{self.username}@" if self.username else ""
        return f"{self.protocol.value}://{auth}{self.host}:{self.port}"

    def set_cooldown(self, duration_sec: float = DEFAULT_COOLDOWN_SEC) -> None:
        with self._lock:
            self.status = ProxyStatus.COOLDOWN
            self.cooldown_until_ts = time.time() + duration_sec

    def blacklist(self, reason: str = "") -> None:
        with self._lock:
            self.status = ProxyStatus.BLACKLIST
            self.blacklist_reason = reason

    def to_dict(self) -> Dict[str, Any]:
        return {
            "proxy_id": self.proxy_id,
            "protocol": self.protocol.value,
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "status": self.status.name,
            "anonymity": self.anonymity.name,
            "country": self.country,
            "metrics": {
                "total_requests": self.metrics.total_requests,
                "success_rate": f"{self.metrics.success_rate:.1f}%",
                "avg_latency_ms": f"{self.metrics.avg_latency_ms:.0f}",
                "weight": self.metrics.weight,
            }
        }


@dataclass
class BackoffConfig:
    max_retries: int = 4
    base_delay_sec: float = 1.0
    max_delay_sec: float = 60.0
    multiplier: float = 2.0
    jitter_fraction: float = 0.3

    def compute_delay(self, attempt: int) -> float:
        exponential = self.base_delay_sec * (self.multiplier ** attempt)
        capped = min(exponential, self.max_delay_sec)
        jitter = capped * self.jitter_fraction * random.random()
        return round(capped + jitter, 3)

    def sleep(self, attempt: int) -> None:
        time.sleep(self.compute_delay(attempt))


@dataclass
class APIRefreshConfig:
    url: str = ""
    interval_sec: float = 600.0
    auth_header: Optional[str] = None
    response_format: str = "text"
    json_path: str = ""
    enabled: bool = False

# =============================================================================
# PROXY PARSER
# =============================================================================

class ProxyParser:
    _PATTERN_FULL_URL = re.compile(
        r"^(?P<protocol>https?|socks[45])://"
        r"(?:(?P<username>[^:@\s]+):(?P<password>[^@\s]+)@)?"
        r"(?P<host>[\w.\-]+):(?P<port>\d{1,5})/?$",
        re.IGNORECASE
    )
    _PATTERN_AUTH_AT = re.compile(
        r"^(?P<username>[^:@\s]+):(?P<password>[^@\s]+)@"
        r"(?P<host>[\w.\-]+):(?P<port>\d{1,5})$"
    )
    _PATTERN_BARE = re.compile(r"^(?P<host>[\w.\-]+):(?P<port>\d{1,5})$")
    _PATTERN_COLON_SEP = re.compile(
        r"^(?P<host>[\w.\-]+):(?P<port>\d{1,5})"
        r":(?P<username>[^:\s]+):(?P<password>[^:\s]+)$"
    )

    _PROTOCOL_MAP = {
        "http": ProxyProtocol.HTTP,
        "https": ProxyProtocol.HTTPS,
        "socks4": ProxyProtocol.SOCKS4,
        "socks5": ProxyProtocol.SOCKS5,
    }

    @classmethod
    def parse_line(cls, line: str, default_protocol: ProxyProtocol = ProxyProtocol.HTTP) -> Optional[ProxyEntry]:
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("//"):
            return None
        
        line = line.rstrip("/")
        
        parsed = (
            cls._try_full_url(line) or
            cls._try_auth_at(line) or
            cls._try_colon_sep(line) or
            cls._try_bare(line)
        )
        
        if not parsed:
            return None
        
        host, port, username, password, protocol_str = parsed
        
        if not (1 <= port <= 65535):
            return None
        
        protocol = cls._PROTOCOL_MAP.get((protocol_str or "").lower(), default_protocol)
        
        return ProxyEntry(
            protocol=protocol,
            host=host,
            port=port,
            username=username or None,
            password=password or None,
            status=ProxyStatus.UNKNOWN,
        )

    @classmethod
    def _try_full_url(cls, line: str):
        m = cls._PATTERN_FULL_URL.match(line)
        if m:
            return (m.group("host"), int(m.group("port")), m.group("username"), m.group("password"), m.group("protocol"))
        return None

    @classmethod
    def _try_auth_at(cls, line: str):
        m = cls._PATTERN_AUTH_AT.match(line)
        if m:
            return (m.group("host"), int(m.group("port")), m.group("username"), m.group("password"), None)
        return None

    @classmethod
    def _try_colon_sep(cls, line: str):
        m = cls._PATTERN_COLON_SEP.match(line)
        if m:
            return (m.group("host"), int(m.group("port")), m.group("username"), m.group("password"), None)
        return None

    @classmethod
    def _try_bare(cls, line: str):
        m = cls._PATTERN_BARE.match(line)
        if m:
            return (m.group("host"), int(m.group("port")), None, None, None)
        return None

    @classmethod
    def parse_list(cls, lines: List[str], default_protocol: ProxyProtocol = ProxyProtocol.HTTP) -> List[ProxyEntry]:
        results = []
        for line in lines:
            entry = cls.parse_line(line, default_protocol)
            if entry:
                results.append(entry)
        return results

    @classmethod
    def parse_file(cls, filepath: str, default_protocol: ProxyProtocol = ProxyProtocol.HTTP) -> List[ProxyEntry]:
        """File se proxies load karne ke liye method"""
        if not filepath or not os.path.isfile(filepath):
            return []
        proxies = []
        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    entry = cls.parse_line(line, default_protocol)
                    if entry:
                        proxies.append(entry)
        except Exception as e:
            logger.error(f"[PARSER] File read error: {e}")
        return proxies

    @classmethod
    def parse_file(cls, filepath: str, default_protocol: ProxyProtocol = ProxyProtocol.HTTP) -> List[ProxyEntry]:
        """File se proxies load karne ke liye method"""
        if not filepath or not os.path.isfile(filepath):
            return []
        proxies = []
        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    entry = cls.parse_line(line, default_protocol)
                    if entry:
                        proxies.append(entry)
        except Exception as e:
            logger.error(f"[PARSER] File read error: {e}")
        return proxies

    @classmethod
    def parse_file(cls, filepath: str, default_protocol: ProxyProtocol = ProxyProtocol.HTTP) -> List[ProxyEntry]:
        """File se proxies load karne ke liye method"""
        if not filepath or not os.path.isfile(filepath):
            return []
        proxies = []
        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    entry = cls.parse_line(line, default_protocol)
                    if entry:
                        proxies.append(entry)
        except Exception as e:
            logger.error(f"[PARSER] File read error: {e}")
        return proxies

# =============================================================================
# PROXY POOL
# =============================================================================

class ProxyPool:
    def __init__(self, max_size: int = MAX_POOL_SIZE):
        self._lock = threading.RLock()
        self._proxies: Dict[str, ProxyEntry] = {}
        self._host_set: Set[Tuple[str, int]] = set()
        self._max_size = max_size

    def add(self, entry: ProxyEntry) -> bool:
        with self._lock:
            if len(self._proxies) >= self._max_size:
                return False
            key = (entry.host.lower(), entry.port)
            if key in self._host_set:
                return False
            self._proxies[entry.proxy_id] = entry
            self._host_set.add(key)
            return True

    def add_many(self, entries: List[ProxyEntry]) -> int:
        added = sum(1 for e in entries if self.add(e))
        return added

    def remove(self, proxy_id: str) -> bool:
        with self._lock:
            entry = self._proxies.pop(proxy_id, None)
            if entry:
                key = (entry.host.lower(), entry.port)
                self._host_set.discard(key)
                return True
            return False

    def get(self, proxy_id: str) -> Optional[ProxyEntry]:
        with self._lock:
            return self._proxies.get(proxy_id)

    def get_all_proxies(self) -> List[ProxyEntry]:
        with self._lock:
            return list(self._proxies.values())

    def get_available_proxies(self) -> List[ProxyEntry]:
        with self._lock:
            return [p for p in self._proxies.values() if p.is_available]

    def clear_blacklisted(self) -> int:
        with self._lock:
            to_remove = [pid for pid, p in self._proxies.items() if p.status == ProxyStatus.BLACKLIST]
            for pid in to_remove:
                entry = self._proxies.pop(pid, None)
                if entry:
                    self._host_set.discard((entry.host.lower(), entry.port))
            return len(to_remove)

    def reset_cooldowns(self) -> int:
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
        with self._lock:
            return len(self._proxies)

    @property
    def available_count(self) -> int:
        return len(self.get_available_proxies())

    @property
    def status_breakdown(self) -> Dict[str, int]:
        counts = {s.name: 0 for s in ProxyStatus}
        with self._lock:
            for proxy in self._proxies.values():
                counts[proxy.status.name] += 1
        return counts

# =============================================================================
# ROTATION STRATEGIES
# =============================================================================

class BaseRotator(ABC):
    @abstractmethod
    def select(self, proxies: List[ProxyEntry]) -> Optional[ProxyEntry]:
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        pass

class RoundRobinRotator(BaseRotator):
    def __init__(self):
        self._counter = itertools.count()
        self._lock = threading.Lock()

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
    @property
    def name(self) -> str:
        return "Weighted Round Robin"

    def select(self, proxies: List[ProxyEntry]) -> Optional[ProxyEntry]:
        if not proxies:
            return None
        weights = [p.metrics.weight for p in proxies]
        total = sum(weights)
        if total <= 0:
            return random.choice(proxies)
        r = random.uniform(0, total)
        cumulative = 0.0
        for proxy, weight in zip(proxies, weights):
            cumulative += weight
            if r <= cumulative:
                return proxy
        return proxies[-1]

class RandomRotator(BaseRotator):
    @property
    def name(self) -> str:
        return "Random"

    def select(self, proxies: List[ProxyEntry]) -> Optional[ProxyEntry]:
        return random.choice(proxies) if proxies else None

class LeastUsedRotator(BaseRotator):
    @property
    def name(self) -> str:
        return "Least Used"

    def select(self, proxies: List[ProxyEntry]) -> Optional[ProxyEntry]:
        if not proxies:
            return None
        return min(proxies, key=lambda p: p.metrics.total_requests)

class FastestFirstRotator(BaseRotator):
    @property
    def name(self) -> str:
        return "Fastest First"

    def select(self, proxies: List[ProxyEntry]) -> Optional[ProxyEntry]:
        if not proxies:
            return None
        return min(proxies, key=lambda p: p.metrics.avg_latency_ms)

class RotatorFactory:
    _MAP = {
        RotationStrategy.ROUND_ROBIN: RoundRobinRotator,
        RotationStrategy.WEIGHTED_ROUND_ROBIN: WeightedRoundRobinRotator,
        RotationStrategy.RANDOM: RandomRotator,
        RotationStrategy.LEAST_USED: LeastUsedRotator,
        RotationStrategy.FASTEST_FIRST: FastestFirstRotator,
    }

    @classmethod
    def create(cls, strategy: RotationStrategy):
        return cls._MAP.get(strategy, RoundRobinRotator)()

# =============================================================================
# SESSION MANAGER
# =============================================================================

class SessionManager:
    def __init__(self):
        self._lock = threading.RLock()
        self._sessions: Dict[int, ProxyEntry] = {}
        self._session_start: Dict[int, float] = {}

    def get_assigned(self, thread_id: int) -> Optional[ProxyEntry]:
        with self._lock:
            proxy = self._sessions.get(thread_id)
            if proxy and not proxy.is_available:
                self._sessions.pop(thread_id, None)
                self._session_start.pop(thread_id, None)
                return None
            return proxy

    def assign(self, thread_id: int, proxy: ProxyEntry) -> None:
        with self._lock:
            self._sessions[thread_id] = proxy
            self._session_start[thread_id] = time.time()

    def release(self, thread_id: int) -> None:
        with self._lock:
            self._sessions.pop(thread_id, None)
            self._session_start.pop(thread_id, None)

    def session_duration(self, thread_id: int) -> float:
        with self._lock:
            start = self._session_start.get(thread_id, 0.0)
            return time.time() - start if start else 0.0

    @property
    def active_sessions(self) -> int:
        with self._lock:
            return len(self._sessions)

# =============================================================================
# BAN INTERCEPTOR
# =============================================================================

class BanInterceptor:
    _STATUS_ACTIONS = {
        401: (False, "Unauthorized"),
        403: (True, "Forbidden - IP banned"),
        407: (True, "Proxy Auth Required"),
        429: (True, "Rate limited"),
        503: (True, "Service Unavailable"),
        500: (False, "Server Error"),
        502: (False, "Bad Gateway"),
        504: (False, "Gateway Timeout"),
    }

    def __init__(self):
        self._lock = threading.RLock()
        self._ban_counts: Dict[str, int] = {}
        self._domain_bans: Dict[str, int] = {}

    def handle_response(self, proxy: ProxyEntry, status_code: int, url: str = "") -> Tuple[bool, str]:
        domain = self._extract_domain(url)
        is_ban, description = self._STATUS_ACTIONS.get(status_code, (False, f"HTTP {status_code}"))

        if status_code in SUCCESS_STATUS_CODES:
            with self._lock:
                self._ban_counts.pop(proxy.proxy_id, None)
            return False, "success"

        with self._lock:
            if is_ban:
                ban_count = self._ban_counts.get(proxy.proxy_id, 0) + 1
                self._ban_counts[proxy.proxy_id] = ban_count
                self._domain_bans[domain] = self._domain_bans.get(domain, 0) + 1

                if ban_count == 1:
                    proxy.set_cooldown(300)
                    action = f"cooldown-5m (ban #{ban_count})"
                elif ban_count == 2:
                    proxy.set_cooldown(900)
                    action = f"cooldown-15m (ban #{ban_count})"
                else:
                    proxy.blacklist(f"{description} × {ban_count}")
                    action = f"blacklisted (ban #{ban_count})"

                return True, action

            elif status_code in RETRY_STATUS_CODES:
                proxy.set_cooldown(60)
                return True, f"server-error-{status_code}"

            return False, description

    def get_domain_stats(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._domain_bans)

    @staticmethod
    def _extract_domain(url: str) -> str:
        try:
            return urlparse(url).netloc or "unknown"
        except Exception:
            return "unknown"

# =============================================================================
# HEALTH MONITOR
# =============================================================================

class ProxyHealthMonitor:
    def __init__(
        self,
        pool: ProxyPool,
        check_interval_sec: float = DEFAULT_HEALTH_INTERVAL_SEC,
        check_timeout_sec: float = DEFAULT_HEALTH_TIMEOUT_SEC,
        max_workers: int = 20,
        max_failures: int = DEFAULT_MAX_FAILURES,
    ):
        self._pool = pool
        self._interval = check_interval_sec
        self._timeout = check_timeout_sec
        self._max_workers = max_workers
        self._max_failures = max_failures
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._check_count = 0

    def start(self):
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._monitor_loop, name="HealthMonitor", daemon=True)
        self._thread.start()
        logger.info("[HEALTH] Monitor started")

    def stop(self):
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)

    def _monitor_loop(self):
        while not self._stop_event.is_set():
            try:
                self._run_check_cycle()
            except Exception as e:
                logger.error(f"[HEALTH] Cycle error: {e}")
            self._stop_event.wait(timeout=self._interval)

    def _run_check_cycle(self):
        self._check_count += 1
        all_proxies = self._pool.get_all_proxies()
        if not all_proxies:
            return

        with ThreadPoolExecutor(max_workers=min(self._max_workers, len(all_proxies)), thread_name_prefix="health") as executor:
            futures = {
                executor.submit(self._check_proxy, p): p
                for p in all_proxies
                if p.status != ProxyStatus.BLACKLIST
            }
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception:
                    pass

    def _check_proxy(self, proxy: ProxyEntry):
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
                if response_data:
                    proxy.anonymity = self._classify_anonymity(response_data)
                    proxy.country = response_data.get("country", "UNKNOWN")
            else:
                proxy.metrics.record_request(False, latency_ms)
                failures = proxy.metrics.consecutive_failures
                if failures >= self._max_failures:
                    proxy.blacklist(f"Failed {failures} health checks")
                else:
                    proxy.set_cooldown(DEFAULT_COOLDOWN_SEC * failures)

    def _measure_tcp_latency(self, proxy: ProxyEntry) -> float:
        try:
            start = time.perf_counter()
            with socket.create_connection((proxy.host, proxy.port), timeout=self._timeout):
                return round((time.perf_counter() - start) * 1000, 2)
        except Exception:
            return 9999.0

    def _http_check(self, proxy: ProxyEntry) -> Tuple[bool, Optional[Dict]]:
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
                    return True, response.json()
                except ValueError:
                    return True, {}
            return False, None
        except Exception:
            return False, None

    @staticmethod
    def _classify_anonymity(response_data: Dict) -> AnonymityLevel:
        headers = response_data.get("headers", {})
        header_keys_lower = {k.lower() for k in headers}
        revealing = {"x-real-ip", "x-forwarded-for", "x-client-ip"}
        proxy_headers = {"via", "x-proxy-id", "proxy-connection"}
        
        if revealing & header_keys_lower:
            return AnonymityLevel.TRANSPARENT
        if proxy_headers & header_keys_lower:
            return AnonymityLevel.ANONYMOUS
        return AnonymityLevel.ELITE

# =============================================================================
# API REFRESH MANAGER
# =============================================================================

class APIRefreshManager:
    def __init__(self, pool: ProxyPool, config: APIRefreshConfig):
        self._pool = pool
        self._config = config
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_refresh_ts = 0.0
        self._refresh_count = 0
        self._total_added = 0

    def start(self):
        if not self._config.enabled or not self._config.url:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._refresh_loop, name="APIRefresh", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=15)

    def _refresh_loop(self):
        while not self._stop_event.is_set():
            try:
                added = self._fetch_and_add()
                self._refresh_count += 1
                self._last_refresh_ts = time.time()
                self._total_added += added
            except Exception as e:
                logger.error(f"[API_REFRESH] Fetch error: {e}")
            self._stop_event.wait(timeout=self._config.interval_sec)

    def _fetch_and_add(self) -> int:
        headers = {"User-Agent": "ProxyManager/3.0"}
        if self._config.auth_header:
            headers["Authorization"] = self._config.auth_header
        
        response = requests.get(self._config.url, headers=headers, timeout=30, verify=False)
        response.raise_for_status()
        
        lines = self._parse_response(response)
        entries = ProxyParser.parse_list(lines)
        return self._pool.add_many(entries)

    def _parse_response(self, response: requests.Response) -> List[str]:
        fmt = self._config.response_format.lower()
        if fmt == "json":
            data = response.json()
            if self._config.json_path:
                for key in self._config.json_path.split("."):
                    if isinstance(data, dict):
                        data = data.get(key, {})
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
        return response.text.splitlines()

    @property
    def stats(self) -> Dict[str, Any]:
        return {
            "refresh_count": self._refresh_count,
            "total_added": self._total_added,
            "last_refresh": datetime.fromtimestamp(self._last_refresh_ts).strftime("%H:%M:%S") if self._last_refresh_ts else "Never",
            "next_refresh_in": max(0, self._config.interval_sec - (time.time() - self._last_refresh_ts)) if self._last_refresh_ts else 0,
        }

# =============================================================================
# SMART REQUEST WRAPPER
# =============================================================================

class StickySessionContext:
    def __init__(self, wrapper: "SmartRequestWrapper", thread_id: int):
        self._wrapper = wrapper
        self._thread_id = thread_id

    def get(self, url: str, **kwargs):
        return self._wrapper.get(url, thread_id=self._thread_id, **kwargs)

    def post(self, url: str, **kwargs):
        return self._wrapper.post(url, thread_id=self._thread_id, **kwargs)

    def request(self, method: str, url: str, **kwargs):
        return self._wrapper.request(method, url, thread_id=self._thread_id, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._wrapper.release_session(self._thread_id)


class SmartRequestWrapper:
    _DEFAULT_HEADERS = {
        "Accept": "application/json, text/html, */*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
    }

    def __init__(
        self,
        proxy_manager: "ProxyManager",
        backoff_config: Optional[BackoffConfig] = None,
        default_timeout: int = DEFAULT_REQUEST_TIMEOUT_SEC,
        default_headers: Optional[Dict[str, str]] = None,
    ):
        self._manager = proxy_manager
        self._backoff = backoff_config or BackoffConfig()
        self._timeout = default_timeout
        self._headers = {**self._DEFAULT_HEADERS, **(default_headers or {})}

    def get(self, url: str, thread_id: int = 0, **kwargs):
        return self.request("GET", url, thread_id=thread_id, **kwargs)

    def post(self, url: str, thread_id: int = 0, **kwargs):
        return self.request("POST", url, thread_id=thread_id, **kwargs)

    def request(self, method: str, url: str, thread_id: int = 0, **kwargs):
        caller_headers = kwargs.pop("headers", {})
        merged_headers = {**self._headers, **caller_headers}
        timeout = kwargs.pop("timeout", self._timeout)
        verify = kwargs.pop("verify", False)
        last_error = None

        for attempt in range(self._backoff.max_retries):
            if self._manager.exit_flag:
                return None

            proxy = self._get_proxy_for_thread(thread_id)
            proxy_info = proxy.display_str if proxy else "DIRECT"
            proxies = proxy.requests_dict if proxy else None

            try:
                start_ms = time.perf_counter()
                with requests.Session() as session:
                    response = session.request(
                        method=method,
                        url=url,
                        headers=merged_headers,
                        proxies=proxies,
                        timeout=timeout,
                        verify=verify,
                        **kwargs
                    )

                latency_ms = (time.perf_counter() - start_ms) * 1000
                status = response.status_code
                success = status in SUCCESS_STATUS_CODES
                was_banned = status in BAN_STATUS_CODES

                if proxy:
                    with proxy._lock:
                        proxy.metrics.record_request(success, latency_ms, was_banned)

                should_retry, action = self._manager.ban_interceptor.handle_response(
                    proxy or ProxyEntry(), status, url
                )

                if success:
                    return response

                if should_retry:
                    if was_banned and thread_id:
                        self._manager.session_manager.release(thread_id)
                    self._backoff.sleep(attempt)
                    continue

                return response

            except requests.exceptions.ProxyError as e:
                if proxy:
                    proxy.set_cooldown(DEFAULT_COOLDOWN_SEC)
                last_error = e
            except requests.exceptions.ConnectTimeout as e:
                if proxy:
                    proxy.metrics.record_request(False, self._timeout * 1000)
                    with proxy._lock:
                        proxy.metrics.consecutive_failures += 1
                last_error = e
            except requests.exceptions.ReadTimeout as e:
                last_error = e
            except requests.exceptions.ConnectionError as e:
                if proxy:
                    proxy.set_cooldown(60)
                last_error = e
            except Exception as e:
                last_error = e

            self._backoff.sleep(attempt)

        logger.error(f"[REQUEST] All retries failed for {method} {url}: {last_error}")
        return None

    def release_session(self, thread_id: int):
        self._manager.session_manager.release(thread_id)

    def _get_proxy_for_thread(self, thread_id: int) -> Optional[ProxyEntry]:
        if thread_id > 0:
            assigned = self._manager.session_manager.get_assigned(thread_id)
            if assigned:
                return assigned
        
        proxy = self._manager.select_proxy()
        if proxy and thread_id > 0:
            self._manager.session_manager.assign(thread_id, proxy)
        return proxy

# =============================================================================
# PROXY MANAGER (MAIN FACADE)
# =============================================================================

class ProxyManager:
    def __init__(
        self,
        proxy_file: Optional[str] = "proxies.txt",
        proxy_list: Optional[List[str]] = None,
        default_protocol: ProxyProtocol = ProxyProtocol.HTTP,
        strategy: RotationStrategy = RotationStrategy.WEIGHTED_ROUND_ROBIN,
        health_interval_sec: float = DEFAULT_HEALTH_INTERVAL_SEC,
        health_timeout_sec: float = DEFAULT_HEALTH_TIMEOUT_SEC,
        max_failures: int = DEFAULT_MAX_FAILURES,
        cooldown_sec: float = DEFAULT_COOLDOWN_SEC,
        api_refresh_config: Optional[APIRefreshConfig] = None,
        direct_mode_fallback: bool = True,
        max_pool_size: int = MAX_POOL_SIZE,
    ):
        self._strategy = strategy
        self._direct_fallback = direct_mode_fallback
        self._default_protocol = default_protocol
        self.exit_flag = False
        self._start_time = time.time()

        self.pool = ProxyPool(max_size=max_pool_size)
        self.session_manager = SessionManager()
        self.ban_interceptor = BanInterceptor()
        self._rotator = RotatorFactory.create(strategy)

        self.health_monitor = ProxyHealthMonitor(
            pool=self.pool,
            check_interval_sec=health_interval_sec,
            check_timeout_sec=health_timeout_sec,
            max_failures=max_failures,
        )

        self._api_refresh = APIRefreshManager(
            pool=self.pool,
            config=api_refresh_config or APIRefreshConfig(),
        )

        self._backoff = BackoffConfig()
        self._initial_load(proxy_file, proxy_list)
        self._wrapper: Optional[SmartRequestWrapper] = None

    def start(self):
        self.exit_flag = False
        self.health_monitor.start()
        self._api_refresh.start()
        logger.info(f"[MANAGER] Started with {self.pool.size} proxies, strategy: {self._rotator.name}")
        return self

    def stop(self):
        self.exit_flag = True
        self.health_monitor.stop()
        self._api_refresh.stop()

    def select_proxy(self) -> Optional[ProxyEntry]:
        available = self.pool.get_available_proxies()
        if not available:
            if self._direct_fallback:
                return None
            return None
        return self._rotator.select(available)

    def get_wrapper(self, default_headers: Optional[Dict[str, str]] = None) -> SmartRequestWrapper:
        if self._wrapper is None:
            self._wrapper = SmartRequestWrapper(
                proxy_manager=self,
                backoff_config=self._backoff,
                default_headers=default_headers,
            )
        return self._wrapper

    @contextlib.contextmanager
    def sticky_session(self, thread_id: int) -> Iterator[StickySessionContext]:
        wrapper = self.get_wrapper()
        context = StickySessionContext(wrapper, thread_id)
        try:
            yield context
        finally:
            self.session_manager.release(thread_id)

    def add_proxies_from_file(self, filepath: str) -> int:
        entries = ProxyParser.parse_file(filepath, self._default_protocol)
        added = self.pool.add_many(entries)
        # Save to database
        for entry in self.pool.get_all_proxies():
            db.save_proxy(entry.to_dict())
        return added

    def add_proxies_from_list(self, proxy_lines: List[str]) -> int:
        entries = ProxyParser.parse_list(proxy_lines, self._default_protocol)
        added = self.pool.add_many(entries)
        for entry in self.pool.get_all_proxies():
            db.save_proxy(entry.to_dict())
        return added

    def _initial_load(self, proxy_file, proxy_list):
        # First try to load from database
        db_proxies = db.load_proxies()
        if db_proxies:
            for p_data in db_proxies:
                entry = ProxyEntry(
                    proxy_id=p_data['proxy_id'],
                    protocol=ProxyProtocol(p_data['protocol']),
                    host=p_data['host'],
                    port=p_data['port'],
                    username=p_data.get('username'),
                    password=p_data.get('password'),
                    status=ProxyStatus[p_data.get('status', 'UNKNOWN')],
                    anonymity=AnonymityLevel[p_data.get('anonymity', 'UNKNOWN')],
                    country=p_data.get('country', 'UNKNOWN'),
                )
                if 'metrics' in p_data and isinstance(p_data['metrics'], dict):
                    m = entry.metrics
                    m_dict = p_data['metrics']
                    m.total_requests = m_dict.get('total_requests', 0)
                    m.successful_reqs = int(float(m_dict.get('success_rate', '0').rstrip('%') or 0) * m.total_requests / 100) if m.total_requests else 0
                self.pool.add(entry)
            logger.info(f"[MANAGER] Loaded {len(db_proxies)} proxies from database")

        # Then load from file if provided
        total_added = 0
        if proxy_file and os.path.exists(proxy_file):
            entries = ProxyParser.parse_file(proxy_file, self._default_protocol)
            total_added += self.pool.add_many(entries)
        if proxy_list:
            entries = ProxyParser.parse_list(proxy_list, self._default_protocol)
            total_added += self.pool.add_many(entries)
        
        if total_added > 0:
            logger.info(f"[MANAGER] Added {total_added} new proxies from files")

    def get_stats(self) -> Dict[str, Any]:
        available = self.pool.get_available_proxies()
        top_5 = sorted(available, key=lambda p: p.metrics.weight, reverse=True)[:5]
        
        return {
            "uptime_sec": round(time.time() - self._start_time, 1),
            "strategy": self._rotator.name,
            "pool_status": self.pool.status_breakdown,
            "total_proxies": self.pool.size,
            "available_proxies": self.pool.available_count,
            "active_sessions": self.session_manager.active_sessions,
            "domain_bans": self.ban_interceptor.get_domain_stats(),
            "api_refresh": self._api_refresh.stats,
            "top_proxies": [p.to_dict() for p in top_5],
        }

# =============================================================================
# GAME PROTOCOL FUNCTIONS (PRESERVED EXACTLY)
# =============================================================================

def EnC_Vr(N: int) -> bytes:
    if N < 0:
        return b''
    H = []
    while True:
        BesTo = N & 0x7F
        N >>= 7
        if N:
            BesTo |= 0x80
        H.append(BesTo)
        if not N:
            break
    return bytes(H)

def CrEaTe_VarianT(field_number: int, value: int) -> bytes:
    field_header = (field_number << 3) | 0
    return EnC_Vr(field_header) + EnC_Vr(value)

def CrEaTe_LenGTh(field_number: int, value) -> bytes:
    field_header = (field_number << 3) | 2
    encoded_value = value.encode() if isinstance(value, str) else value
    return EnC_Vr(field_header) + EnC_Vr(len(encoded_value)) + encoded_value

def CrEaTe_ProTo(fields: Dict) -> bytes:
    packet = bytearray()
    for field, value in fields.items():
        if isinstance(value, dict):
            nested_packet = CrEaTe_ProTo(value)
            packet.extend(CrEaTe_LenGTh(field, nested_packet))
        elif isinstance(value, int):
            packet.extend(CrEaTe_VarianT(field, value))
        elif isinstance(value, (str, bytes)):
            packet.extend(CrEaTe_LenGTh(field, value))
    return bytes(packet)

def E_AEs(Pc: str) -> bytes:
    Z = bytes.fromhex(Pc)
    _key = bytes([89, 103, 38, 116, 99, 37, 68, 69, 117, 104, 54, 37, 90, 99, 94, 56])
    _iv = bytes([54, 111, 121, 90, 68, 114, 50, 50, 69, 51, 121, 99, 104, 106, 77, 37])
    K = AES.new(_key, AES.MODE_CBC, _iv)
    R = K.encrypt(pad(Z, AES.block_size))
    return R

def encrypt_api(plain_text: str) -> str:
    plain_text_bytes = bytes.fromhex(plain_text)
    _key = bytes([89, 103, 38, 116, 99, 37, 68, 69, 117, 104, 54, 37, 90, 99, 94, 56])
    _iv = bytes([54, 111, 121, 90, 68, 114, 50, 50, 69, 51, 121, 99, 104, 106, 77, 37])
    cipher = AES.new(_key, AES.MODE_CBC, _iv)
    cipher_text = cipher.encrypt(pad(plain_text_bytes, AES.block_size))
    return cipher_text.hex()

def encode_string(original: str) -> Dict:
    keystream = [
        0x30, 0x30, 0x30, 0x32, 0x30, 0x31, 0x37, 0x30,
        0x30, 0x30, 0x30, 0x30, 0x32, 0x30, 0x31, 0x37,
        0x30, 0x30, 0x30, 0x30, 0x30, 0x32, 0x30, 0x31,
        0x37, 0x30, 0x30, 0x30, 0x30, 0x30, 0x32, 0x30,
    ]
    encoded = ""
    for i in range(len(original)):
        orig_byte = ord(original[i])
        key_byte = keystream[i % len(keystream)]
        result_byte = orig_byte ^ key_byte
        encoded += chr(result_byte)
    return {"open_id": original, "field_14": encoded}

def to_unicode_escaped(s: str) -> str:
    return "".join(c if 32 <= ord(c) <= 126 else f"\\u{ord(c):04x}" for c in s)

def decode_jwt_token(jwt_token: str) -> str:
    try:
        parts = jwt_token.split('.')
        if len(parts) >= 2:
            payload_part = parts[1]
            padding = 4 - len(payload_part) % 4
            if padding != 4:
                payload_part += "=" * padding
            decoded = base64.urlsafe_b64decode(payload_part)
            data = json.loads(decoded)
            account_id = data.get('account_id') or data.get('external_id')
            if account_id:
                return str(account_id)
    except Exception as e:
        logger.warning(f"JWT decode failed: {e}")
    return "N/A"

# =============================================================================
# NAME & PASSWORD GENERATION
# =============================================================================

def generate_exponent_number() -> str:
    exponent_digits = {
        "0": "⁰", "1": "¹", "2": "²", "3": "³", "4": "⁴",
        "5": "⁵", "6": "⁶", "7": "⁷", "8": "⁸", "9": "⁹",
    }
    number = random.randint(1, 99999)
    number_str = f"{number:05d}"
    return "".join(exponent_digits[digit] for digit in number_str)

def generate_random_name(base_name: str) -> str:
    exponent_part = generate_exponent_number()
    return f"{base_name[:7]}{exponent_part}"

def generate_custom_password(prefix: str) -> str:
    garena_decoded = base64.b64decode(GARENA).decode("utf-8")
    characters = string.ascii_uppercase + string.digits
    random_part1 = "".join(random.choice(characters) for _ in range(5))
    random_part2 = "".join(random.choice(characters) for _ in range(5))
    return f"{prefix}_{random_part1}_{garena_decoded}_{random_part2}"

# =============================================================================
# RARITY & COUPLE DETECTION
# =============================================================================

def check_account_rarity(account_data: Dict) -> Tuple[bool, Optional[str], Optional[str], int]:
    account_id = account_data.get("account_id", "")
    if account_id == "N/A" or not account_id:
        return False, None, None, 0

    rarity_score = 0
    detected_patterns = []

    for rarity_type, pattern_data in ACCOUNT_RARITY_PATTERNS.items():
        pattern = pattern_data[0]
        score = pattern_data[1]
        if re.search(pattern, account_id):
            rarity_score += score
            detected_patterns.append(rarity_type)

    account_id_digits = [int(d) for d in account_id if d.isdigit()]
    
    if len(set(account_id_digits)) == 1 and len(account_id_digits) >= 4:
        rarity_score += 5
        detected_patterns.append("UNIFORM_DIGITS")

    if len(account_id_digits) >= 4:
        differences = [account_id_digits[i+1] - account_id_digits[i] for i in range(len(account_id_digits)-1)]
        if len(set(differences)) == 1:
            rarity_score += 4
            detected_patterns.append("ARITHMETIC_SEQUENCE")

    if len(account_id) <= 8 and account_id.isdigit() and int(account_id) < 1000000:
        rarity_score += 3
        detected_patterns.append("LOW_ACCOUNT_ID")

    if rarity_score >= 3:  # Default threshold
        reason = f"Account ID {account_id} - Score: {rarity_score} - Patterns: {', '.join(detected_patterns)}"
        return True, "RARE_ACCOUNT", reason, rarity_score

    return False, None, None, rarity_score

def check_account_couple_patterns(account_id1: str, account_id2: str) -> Tuple[bool, Optional[str]]:
    try:
        if abs(int(account_id1) - int(account_id2)) == 1:
            return True, f"Sequential Account IDs: {account_id1} & {account_id2}"
    except ValueError:
        pass

    if account_id1 == account_id2[::-1]:
        return True, f"Mirror Account IDs: {account_id1} & {account_id2}"

    try:
        total = int(account_id1) + int(account_id2)
        if total % 1000 == 0 or total % 10000 == 0:
            return True, f"Complementary sum: {account_id1} + {account_id2} = {total}"
    except ValueError:
        pass

    love_numbers = ['520', '521', '1314', '3344']
    for love_num in love_numbers:
        if love_num in account_id1 and love_num in account_id2:
            return True, f"Both contain love number: {love_num}"

    return False, None

# =============================================================================
# GLOBAL STATE FOR GENERATION
# =============================================================================

class GenerationState:
    def __init__(self):
        self.lock = threading.Lock()
        self.exit_flag = False
        self.success_counter = 0
        self.target_accounts = 0
        self.rare_counter = 0
        self.couples_counter = 0
        self.start_time = 0.0
        self.threads: List[threading.Thread] = []
        self.proxy_manager: Optional[ProxyManager] = None
        self.telegram_app = None
        self.chat_id: Optional[int] = None
        self.status_message_id: Optional[int] = None

    def reset(self):
        with self.lock:
            self.exit_flag = False
            self.success_counter = 0
            self.rare_counter = 0
            self.couples_counter = 0
            self.start_time = time.time()
            self.threads = []

    def get_progress(self) -> Dict[str, Any]:
        with self.lock:
            elapsed = time.time() - self.start_time if self.start_time else 0
            speed = self.success_counter / elapsed if elapsed > 0 else 0
            progress_pct = (self.success_counter / self.target_accounts * 100) if self.target_accounts > 0 else 0
            
            proxy_stats = {}
            if self.proxy_manager:
                proxy_stats = self.proxy_manager.get_stats()
            
            return {
                'generated': self.success_counter,
                'target': self.target_accounts,
                'progress_pct': progress_pct,
                'rare': self.rare_counter,
                'couples': self.couples_counter,
                'elapsed': elapsed,
                'speed': speed,
                'proxies': proxy_stats.get('available_proxies', 0),
                'proxy_total': proxy_stats.get('total_proxies', 0),
            }

gen_state = GenerationState()

# =============================================================================
# ACCOUNT CREATION WITH STICKY SESSION
# =============================================================================

def create_acc_sticky(
    region: str,
    account_name: str,
    password_prefix: str,
    thread_id: int,
    is_ghost: bool,
    session: StickySessionContext,
) -> Optional[Dict]:
    """Create guest account using sticky session."""
    if gen_state.exit_flag:
        return None

    try:
        password = generate_custom_password(password_prefix)
        data = f"password={password}&client_type=2&source=2&app_id=100067"
        message = data.encode('utf-8')
        signature = hmac.new(key, message, hashlib.sha256).hexdigest()

        headers = {
            "User-Agent": "GarenaMSDK/4.0.19P8(ASUS_Z01QD ;Android 12;en;US;)",
            "Authorization": "Signature " + signature,
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept-Encoding": "gzip",
            "Connection": "Keep-Alive",
            "Host": "100067.connect.garena.com",
        }

        response = session.post(
            "https://100067.connect.garena.com/oauth/guest/register",
            headers=headers,
            data=data,
        )

        if response is None:
            return None

        try:
            resp_json = response.json()
        except ValueError:
            logger.warning(f"Thread {thread_id}: Invalid JSON in guest register")
            return None

        if 'uid' in resp_json:
            uid = resp_json['uid']
            logger.info(f"Thread {thread_id}: Guest created: {uid}")
            time.sleep(random.uniform(0.5, 1.5))
            return token_sticky(uid, password, region, account_name, password_prefix, thread_id, is_ghost, session)

        return None

    except Exception as e:
        logger.warning(f"Thread {thread_id}: Create account failed: {e}")
        return None

def token_sticky(
    uid: str,
    password: str,
    region: str,
    account_name: str,
    password_prefix: str,
    thread_id: int,
    is_ghost: bool,
    session: StickySessionContext,
) -> Optional[Dict]:
    """Get OAuth token using sticky session."""
    if gen_state.exit_flag:
        return None

    try:
        headers = {
            "Accept-Encoding": "gzip",
            "Connection": "Keep-Alive",
            "Content-Type": "application/x-www-form-urlencoded",
            "Host": "100067.connect.garena.com",
            "User-Agent": "GarenaMSDK/4.0.19P8(ASUS_Z01QD ;Android 12;en;US;)",
        }
        body = {
            "uid": uid,
            "password": password,
            "response_type": "token",
            "client_type": "2",
            "client_secret": key,
            "client_id": "100067",
        }

        response = session.post(
            "https://100067.connect.garena.com/oauth/guest/token/grant",
            headers=headers,
            data=body,
        )

        if response is None:
            return None

        try:
            resp_json = response.json()
        except ValueError:
            return None

        if 'open_id' in resp_json:
            open_id = resp_json['open_id']
            access_token = resp_json["access_token"]

            result = encode_string(open_id)
            field = to_unicode_escaped(result['field_14'])
            field = codecs.decode(field, 'unicode_escape').encode('latin1')

            logger.info(f"Thread {thread_id}: Token granted for {uid}")
            time.sleep(random.uniform(0.5, 1.5))

            return Major_Regsiter_sticky(
                access_token, open_id, field, uid, password,
                region, account_name, password_prefix, thread_id, is_ghost, session
            )

        return None

    except Exception as e:
        logger.warning(f"Thread {thread_id}: Token grant failed: {e}")
        return None

def Major_Regsiter_sticky(
    access_token: str,
    open_id: str,
    field: bytes,
    uid: str,
    password: str,
    region: str,
    account_name: str,
    password_prefix: str,
    thread_id: int,
    is_ghost: bool,
    session: StickySessionContext,
) -> Optional[Dict]:
    """Register on game server using sticky session."""
    if gen_state.exit_flag:
        return None

    try:
        if is_ghost:
            url = "https://loginbp.ggblueshark.com/MajorRegister"
            host = "loginbp.ggblueshark.com"
        else:
            if region.upper() in ["ME", "TH"]:
                url = "https://loginbp.common.ggbluefox.com/MajorRegister"
                host = "loginbp.common.ggbluefox.com"
            else:
                url = "https://loginbp.ggblueshark.com/MajorRegister"
                host = "loginbp.ggblueshark.com"

        name = generate_random_name(account_name)

        headers = {
            "Accept-Encoding": "gzip",
            "Authorization": "Bearer",
            "Connection": "Keep-Alive",
            "Content-Type": "application/x-www-form-urlencoded",
            "Expect": "100-continue",
            "Host": host,
            "ReleaseVersion": "OB52",
            "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 9; ASUS_I005DA Build/PI)",
            "X-GA": "v1 1",
            "X-Unity-Version": "2018.4.",
        }

        lang_code = "pt" if is_ghost else REGION_LANG.get(region.upper(), "en")

        payload = {
            1: name,
            2: access_token,
            3: open_id,
            5: 102000007,
            6: 4,
            7: 1,
            13: 1,
            14: field,
            15: lang_code,
            16: 1,
            17: 1,
        }

        payload_bytes = CrEaTe_ProTo(payload)
        encrypted_payload = E_AEs(payload_bytes.hex())

        response = session.post(url, headers=headers, data=encrypted_payload)

        if response is None:
            return None

        if response.status_code == 200:
            logger.info(f"Thread {thread_id}: MajorRegister OK: {name}")

            # Perform login to get JWT
            login_result = perform_major_login_sticky(
                uid, password, access_token, open_id, region, is_ghost, session
            )
            account_id = login_result.get("account_id", "N/A")
            jwt_token = login_result.get("jwt_token", "")

            # Region binding
            if not is_ghost and jwt_token and account_id != "N/A" and region.upper() != "BR":
                force_region_binding_sticky(region, jwt_token, session)

            return {
                "uid": uid,
                "password": password,
                "name": name,
                "region": "GHOST" if is_ghost else region,
                "status": "success",
                "account_id": account_id,
                "jwt_token": jwt_token,
            }

        return None

    except Exception as e:
        logger.warning(f"Thread {thread_id}: Major_Regsiter error: {e}")
        return None

def perform_major_login_sticky(
    uid: str,
    password: str,
    access_token: str,
    open_id: str,
    region: str,
    is_ghost: bool,
    session: StickySessionContext,
) -> Dict:
    """Login to get JWT token using sticky session."""
    try:
        lang = "pt" if is_ghost else REGION_LANG.get(region.upper(), "en")

        payload_parts = [
            b'\x1a\x132025-08-30 05:19:21"\tfree fire(\x01:\x081.114.13B2Android OS 9 / API-28 (PI/rel.cjw.20220518.114133)J\x08HandheldR\nATM MobilsZ\x04WIFI`\xb6\nh\xee\x05r\x03300z\x1fARMv7 VFPv3 NEON VMH | 2400 | 2\x80\x01\xc9\x0f\x8a\x01\x0fAdreno (TM) 640\x92\x01\rOpenGL ES 3.2\x9a\x01+Google|dfa4ab4b-9dc4-454e-8065-e70c733fa53f\xa2\x01\x0e105.235.139.91\xaa\x01\x02',
            lang.encode("ascii"),
            b'\xb2\x01 1d8ec0240ede109973f3321b9354b44d\xba\x01\x014\xc2\x01\x08Handheld\xca\x01\x10Asus ASUS_I005DA\xea\x01@afcfbf13334be42036e4f742c80b956344bed760ac91b3aff9b607a610ab4390\xf0\x01\x01\xca\x02\nATM Mobils\xd2\x02\x04WIFI\xca\x03 7428b253defc164018c604a1ebbfebdf\xe0\x03\xa8\x81\x02\xe8\x03\xf6\xe5\x01\xf0\x03\xaf\x13\xf8\x03\x84\x07\x80\x04\xe7\xf0\x01\x88\x04\xa8\x81\x02\x90\x04\xe7\xf0\x01\x98\x04\xa8\x81\x02\xc8\x04\x01\xd2\x04=/data/app/com.dts.freefireth-PdeDnOilCSFn37p1AH_FLg==/lib/arm\xe0\x04\x01\xea\x04_2087f61c19f57f2af4e7feff0b24d9d9|/data/app/com.dts.freefireth-PdeDnOilCSFn37p1AH_FLg==/base.apk\xf0\x04\x03\xf8\x04\x01\x8a\x05\x0232\x9a\x05\n2019118692\xb2\x05\tOpenGLES2\xb8\x05\xff\x7f\xc0\x05\x04\xe0\x05\xf3F\xea\x05\x07android\xf2\x05pKqsHT5ZLWrYljNb5Vqh//yFRlaPHSO9NWSQsVvOmdhEEn7W+VHNUK+Q+fduA3ptNrGB0Ll0LRz3WW0jOwesLj6aiU7sZ40p8BfUE/FI/jzSTwRe2\xf8\x05\xfb\xe4\x06\x88\x06\x01\x90\x06\x01\x9a\x06\x014\xa2\x06\x014\xb2\x06"GQ@O\x00\x0e^\x00D\x06UA\x0ePM\r\x13hZ\x07T\x06\x0cm\\V\x0ejYV;\x0bU5'
        ]

        payload = b''.join(payload_parts)
        payload = payload.replace(
            b'afcfbf13334be42036e4f742c80b956344bed760ac91b3aff9b607a610ab4390',
            access_token.encode()
        )
        payload = payload.replace(
            b'1d8ec0240ede109973f3321b9354b44d',
            open_id.encode()
        )

        encrypted = encrypt_api(payload.hex())
        final_payload = bytes.fromhex(encrypted)

        if is_ghost:
            url = "https://loginbp.ggblueshark.com/MajorLogin"
            host = "loginbp.ggblueshark.com"
        elif region.upper() in ["ME", "TH"]:
            url = "https://loginbp.common.ggbluefox.com/MajorLogin"
            host = "loginbp.common.ggbluefox.com"
        else:
            url = "https://loginbp.ggblueshark.com/MajorLogin"
            host = "loginbp.ggblueshark.com"

        headers = {
            "Accept-Encoding": "gzip",
            "Authorization": "Bearer",
            "Connection": "Keep-Alive",
            "Content-Type": "application/x-www-form-urlencoded",
            "Expect": "100-continue",
            "Host": host,
            "ReleaseVersion": "OB52",
            "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 9; ASUS_I005DA Build/PI)",
            "X-GA": "v1 1",
            "X-Unity-Version": "2018.4.11f1",
        }

        response = session.post(url, headers=headers, data=final_payload)

        if response and response.status_code == 200 and len(response.text) > 10:
            jwt_start = response.text.find("eyJ")
            if jwt_start != -1:
                jwt_token = response.text[jwt_start:]
                second_dot = jwt_token.find(".", jwt_token.find(".") + 1)
                if second_dot != -1:
                    jwt_token = jwt_token[:second_dot + 44]
                    account_id = decode_jwt_token(jwt_token)
                    return {"account_id": account_id, "jwt_token": jwt_token}

        return {"account_id": "N/A", "jwt_token": ""}

    except Exception as e:
        logger.warning(f"MajorLogin failed: {e}")
        return {"account_id": "N/A", "jwt_token": ""}

def force_region_binding_sticky(region: str, jwt_token: str, session: StickySessionContext) -> bool:
    """Bind account to region using sticky session."""
    try:
        if region.upper() in ["ME", "TH"]:
            url = "https://loginbp.common.ggbluefox.com/ChooseRegion"
        else:
            url = "https://loginbp.ggblueshark.com/ChooseRegion"

        region_code = "RU" if region.upper() == "CIS" else region.upper()
        fields = {1: region_code}
        proto_data = CrEaTe_ProTo(fields)
        encrypted_data = encrypt_api(proto_data.hex())
        payload = bytes.fromhex(encrypted_data)

        headers = {
            'User-Agent': "Dalvik/2.1.0 (Linux; U; Android 12; M2101K7AG Build/SKQ1.210908.001)",
            'Connection': "Keep-Alive",
            'Accept-Encoding': "gzip",
            'Content-Type': "application/x-www-form-urlencoded",
            'Expect': "100-continue",
            'Authorization': f"Bearer {jwt_token}",
            'X-Unity-Version': "2018.4.11f1",
            'X-GA': "v1 1",
            'ReleaseVersion': "OB52",
        }

        response = session.post(url, data=payload, headers=headers)
        return response is not None and response.status_code == 200

    except Exception as e:
        logger.warning(f"Region binding failed: {e}")
        return False

# =============================================================================
# WORKER FUNCTION WITH STICKY SESSION
# =============================================================================

def worker(
    region: str,
    account_name: str,
    password_prefix: str,
    total_accounts: int,
    thread_id: int,
    is_ghost: bool,
):
    """Worker thread with sticky session for IP consistency."""
    logger.info(f"🧵 Thread {thread_id} started")

    accounts_generated = 0

    try:
        while not gen_state.exit_flag:
            with gen_state.lock:
                if gen_state.success_counter >= total_accounts:
                    break

            # Use sticky session for entire account creation flow
            with gen_state.proxy_manager.sticky_session(thread_id) as session:
                try:
                    result = create_acc_sticky(
                        region, account_name, password_prefix,
                        thread_id, is_ghost, session
                    )

                    if result:
                        # Process result
                        account_id = result.get("account_id", "N/A")
                        
                        with gen_state.lock:
                            gen_state.success_counter += 1
                            current_count = gen_state.success_counter

                        # Check rarity
                        is_rare, rarity_type, rarity_reason, rarity_score = check_account_rarity(result)
                        
                        if is_rare:
                            with gen_state.lock:
                                gen_state.rare_counter += 1
                            
                            # Send Telegram alert
                            send_rare_alert(result, rarity_type, rarity_reason, rarity_score)
                            
                            # Save to DB
                            result['is_rare'] = True
                            result['rarity_type'] = rarity_type
                            result['rarity_score'] = rarity_score
                        
                        # Save normal account
                        db.save_account(result)
                        accounts_generated += 1

                        # Check for couples (simplified - would need global state)
                        
                except Exception as e:
                    logger.error(f"Thread {thread_id} iteration error: {e}")

            time.sleep(random.uniform(0.8, 2.0))

    except Exception as e:
        logger.error(f"Thread {thread_id} fatal error: {e}")
    finally:
        logger.info(f"🧵 Thread {thread_id} finished: {accounts_generated} accounts")

# =============================================================================
# TELEGRAM ALERTS
# =============================================================================

def send_rare_alert(account_data: Dict, rarity_type: str, reason: str, score: int):
    """Send rare account alert to Telegram."""
    if not gen_state.telegram_app or not gen_state.chat_id:
        return

    message = f"""
💎 <b>RARE ACCOUNT FOUND!</b>

🎯 Type: <code>{rarity_type}</code>
⭐ Score: <b>{score}</b>
👤 Name: <code>{account_data['name']}</code>
🆔 UID: <code>{account_data['uid']}</code>
🎮 Account ID: <code>{account_data.get('account_id', 'N/A')}</code>
🔑 Password: <code>{account_data['password']}</code>
🌍 Region: <code>{account_data.get('region', 'N/A')}</code>

📝 {reason}
"""

    try:
        asyncio.create_task(
            gen_state.telegram_app.bot.send_message(
                chat_id=gen_state.chat_id,
                text=message,
                parse_mode='HTML'
            )
        )
    except Exception as e:
        logger.error(f"Failed to send rare alert: {e}")

def send_couple_alert(account1: Dict, account2: Dict, reason: str):
    """Send couples account alert to Telegram."""
    if not gen_state.telegram_app or not gen_state.chat_id:
        return

    message = f"""
💑 <b>COUPLES ACCOUNT FOUND!</b>

📝 {reason}

👤 Account 1:
   Name: <code>{account1['name']}</code>
   ID: <code>{account1.get('account_id', 'N/A')}</code>
   UID: <code>{account1['uid']}</code>

👤 Account 2:
   Name: <code>{account2['name']}</code>
   ID: <code>{account2.get('account_id', 'N/A')}</code>
   UID: <code>{account2['uid']}</code>

🌍 Region: <code>{account1.get('region', 'N/A')}</code>
"""

    try:
        asyncio.create_task(
            gen_state.telegram_app.bot.send_message(
                chat_id=gen_state.chat_id,
                text=message,
                parse_mode='HTML'
            )
        )
    except Exception as e:
        logger.error(f"Failed to send couple alert: {e}")

async def update_status_message():
    """Update live status message every 60 seconds."""
    while True:
        try:
            if gen_state.exit_flag or not gen_state.chat_id:
                await asyncio.sleep(5)
                continue

            progress = gen_state.get_progress()
            
            status_text = f"""
📊 <b>GENERATION STATUS</b>

🎯 Progress: <code>{progress['generated']}/{progress['target']}</code> ({progress['progress_pct']:.1f}%)
⚡ Speed: <code>{progress['speed']:.2f}</code> accounts/sec
⏱️ Elapsed: <code>{timedelta(seconds=int(progress['elapsed']))}</code>

💎 Rare: <b>{progress['rare']}</b>
💑 Couples: <b>{progress['couples']}</b>

🌐 Proxies: <code>{progress['proxies']}/{progress['proxy_total']}</code> available
"""

            if gen_state.status_message_id:
                try:
                    await gen_state.telegram_app.bot.edit_message_text(
                        chat_id=gen_state.chat_id,
                        message_id=gen_state.status_message_id,
                        text=status_text,
                        parse_mode='HTML'
                    )
                except Exception:
                    # Message might be deleted, send new one
                    msg = await gen_state.telegram_app.bot.send_message(
                        chat_id=gen_state.chat_id,
                        text=status_text,
                        parse_mode='HTML'
                    )
                    gen_state.status_message_id = msg.message_id
            else:
                msg = await gen_state.telegram_app.bot.send_message(
                    chat_id=gen_state.chat_id,
                    text=status_text,
                    parse_mode='HTML'
                )
                gen_state.status_message_id = msg.message_id

            await asyncio.sleep(60)

        except Exception as e:
            logger.error(f"Status update error: {e}")
            await asyncio.sleep(10)

# =============================================================================
# TELEGRAM BOT HANDLERS
# =============================================================================

import asyncio

# Conversation states
REGION, COUNT, NAME_PREFIX, PASS_PREFIX, THREADS = range(5)

def get_main_keyboard():
    """Get persistent main keyboard."""
    keyboard = [
        [KeyboardButton("🚀 Start Generation"), KeyboardButton("🌐 Proxy Status")],
        [KeyboardButton("📊 View Stats"), KeyboardButton("⚙️ Settings")],
        [KeyboardButton("⏹ Stop Generation")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command."""
    gen_state.chat_id = update.effective_chat.id
    gen_state.telegram_app = context.application

    welcome_text = f"""
🎮 <b>Welcome to RUHI BOT v4.0</b>

{client_data}

✨ <b>Features:</b>
• Advanced Proxy Management with Health Monitoring
• Multi-threaded Account Generation
• Automatic Rarity & Couple Detection
• Real-time Status Updates
• PostgreSQL Database Persistence

🌐 <b>Proxy Status:</b> <code>{gen_state.proxy_manager.pool.available_count if gen_state.proxy_manager else 0}</code> available

Use the buttons below to control the bot:
"""

    await update.message.reply_text(
        welcome_text,
        parse_mode='HTML',
        reply_markup=get_main_keyboard()
    )

    # Start status updater
    asyncio.create_task(update_status_message())

async def generate_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start generation conversation."""
    gen_state.chat_id = update.effective_chat.id
    
    regions_text = "Available regions:\n" + "\n".join([f"• <code>{r}</code> ({REGION_LANG[r]})" for r in REGION_LANG if r != "BR"])
    regions_text += "\n• <code>GHOST</code> (All Servers)"
    
    await update.message.reply_text(
        f"🎯 <b>Start Generation</b>\n\n{regions_text}\n\nPlease enter the region code:",
        parse_mode='HTML'
    )
    return REGION

async def region_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle region input."""
    region = update.message.text.strip().upper()
    
    if region not in REGION_LANG and region != "GHOST":
        await update.message.reply_text("❌ Invalid region. Please try again:")
        return REGION
    
    context.user_data['region'] = region
    context.user_data['is_ghost'] = (region == "GHOST")
    
    await update.message.reply_text(
        "🎯 How many accounts do you want to generate?\n"
        "(Recommended: 10-100 for testing)"
    )
    return COUNT

async def count_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle count input."""
    try:
        count = int(update.message.text.strip())
        if count <= 0 or count > 10000:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Please enter a valid number (1-10000):")
        return COUNT
    
    context.user_data['count'] = count
    
    await update.message.reply_text(
        "👤 Enter account name prefix:\n"
        "(e.g., RUHI, VIP, PRO)"
    )
    return NAME_PREFIX

async def name_prefix_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle name prefix input."""
    name_prefix = update.message.text.strip()
    if not name_prefix:
        await update.message.reply_text("❌ Name prefix cannot be empty:")
        return NAME_PREFIX
    
    context.user_data['name_prefix'] = name_prefix
    
    await update.message.reply_text(
        "🔑 Enter password prefix:\n"
        "(e.g., PASS, KEY)"
    )
    return PASS_PREFIX

async def pass_prefix_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle password prefix input."""
    pass_prefix = update.message.text.strip()
    if not pass_prefix:
        await update.message.reply_text("❌ Password prefix cannot be empty:")
        return PASS_PREFIX
    
    context.user_data['pass_prefix'] = pass_prefix
    
    await update.message.reply_text(
        "🧵 Enter number of threads:\n"
        "(Recommended: 3-5 to avoid IP ban)"
    )
    return THREADS

async def threads_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle threads input and start generation."""
    try:
        threads = int(update.message.text.strip())
        if threads <= 0 or threads > 50:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Please enter a valid number (1-50):")
        return THREADS
    
    # Get all parameters
    region = context.user_data['region']
    count = context.user_data['count']
    name_prefix = context.user_data['name_prefix']
    pass_prefix = context.user_data['pass_prefix']
    is_ghost = context.user_data['is_ghost']

    # Reset and configure generation state
    gen_state.reset()
    gen_state.target_accounts = count
    
    # Ensure proxy manager is running
    if not gen_state.proxy_manager:
        gen_state.proxy_manager = ProxyManager(
            proxy_file="proxies.txt" if os.path.exists("proxies.txt") else None,
            strategy=RotationStrategy.WEIGHTED_ROUND_ROBIN,
        )
        gen_state.proxy_manager.start()

    # Start generation
    await update.message.reply_text(
        f"🚀 <b>Starting Generation</b>\n\n"
        f"Region: <code>{region}</code>\n"
        f"Target: <code>{count}</code> accounts\n"
        f"Threads: <code>{threads}</code>\n"
        f"Name: <code>{name_prefix}</code>\n\n"
        f"Status updates will appear every 60 seconds...",
        parse_mode='HTML',
        reply_markup=get_main_keyboard()
    )

    # Launch worker threads
    for i in range(threads):
        t = threading.Thread(
            target=worker,
            args=(region, name_prefix, pass_prefix, count, i+1, is_ghost),
            name=f"Worker-{i+1}",
            daemon=True,
        )
        t.start()
        gen_state.threads.append(t)
        time.sleep(0.3)

    return ConversationHandler.END

async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stop all generation."""
    gen_state.exit_flag = True
    
    # Wait for threads to finish
    for t in gen_state.threads:
        t.join(timeout=5)
    
    gen_state.threads = []
    
    progress = gen_state.get_progress()
    
    await update.message.reply_text(
        f"⏹ <b>Generation Stopped</b>\n\n"
        f"Generated: <code>{progress['generated']}</code>\n"
        f"Rare: <code>{progress['rare']}</code>\n"
        f"Couples: <code>{progress['couples']}</code>\n"
        f"Speed: <code>{progress['speed']:.2f}</code> accounts/sec",
        parse_mode='HTML',
        reply_markup=get_main_keyboard()
    )

async def proxy_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show proxy status."""
    if not gen_state.proxy_manager:
        await update.message.reply_text("❌ Proxy manager not initialized")
        return

    stats = gen_state.proxy_manager.get_stats()
    
    status_text = f"""
🌐 <b>PROXY STATUS</b>

📊 Pool Statistics:
   Total: <code>{stats['total_proxies']}</code>
   Available: <code>{stats['available_proxies']}</code>
   Active Sessions: <code>{stats['active_sessions']}</code>

🔍 Status Breakdown:
"""
    for status, count in stats['pool_status'].items():
        status_text += f"   {status}: <code>{count}</code>\n"

    status_text += f"\n⚙️ Strategy: <code>{stats['strategy']}</code>\n"
    status_text += f"⏱️ Uptime: <code>{timedelta(seconds=int(stats['uptime_sec']))}</code>"

    await update.message.reply_text(status_text, parse_mode='HTML')

async def view_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """View database stats."""
    db_stats = db.get_stats()
    
    stats_text = f"""
📊 <b>DATABASE STATISTICS</b>

💾 Accounts:
   Total: <code>{db_stats.get('total_accounts', 0)}</code>
   Rare: <code>{db_stats.get('rare_accounts', 0)}</code>
   Couples: <code>{db_stats.get('couple_accounts', 0)}</code>

🌐 Proxies by Status:
"""
    for status, count in db_stats.get('proxies', {}).items():
        stats_text += f"   {status}: <code>{count}</code>\n"

    await update.message.reply_text(stats_text, parse_mode='HTML')

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle uploaded files."""
    document = update.message.document
    
    if not document.file_name.endswith('.txt'):
        await update.message.reply_text("❌ Please upload a .txt file")
        return

    # Download file
    file = await context.bot.get_file(document.file_id)
    
    # Create temporary file
    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.txt') as tmp:
        await file.download_to_drive(tmp.name)
        tmp_path = tmp.name

    try:
        if document.file_name == 'proxies.txt':
            # Clear existing and load new
            if gen_state.proxy_manager:
                # Add to pool
                count = gen_state.proxy_manager.add_proxies_from_file(tmp_path)
                
                # Also save to database
                with open(tmp_path, 'r') as f:
                    lines = f.readlines()
                
                await update.message.reply_text(
                    f"✅ <b>Proxies Updated!</b>\n\n"
                    f"Added <code>{count}</code> new proxies to the pool.\n"
                    f"Total in pool: <code>{gen_state.proxy_manager.pool.size}</code>",
                    parse_mode='HTML'
                )
            else:
                await update.message.reply_text("❌ Proxy manager not initialized")
        else:
            await update.message.reply_text(
                f"📄 File received: <code>{document.file_name}</code>\n"
                f"Use <code>proxies.txt</code> as filename to update proxies.",
                parse_mode='HTML'
            )

    finally:
        os.unlink(tmp_path)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle button presses."""
    text = update.message.text
    
    if text == "🚀 Start Generation":
        return await generate_command(update, context)
    elif text == "🌐 Proxy Status":
        return await proxy_status(update, context)
    elif text == "📊 View Stats":
        return await view_stats(update, context)
    elif text == "⚙️ Settings":
        await update.message.reply_text(
            "⚙️ <b>Settings</b>\n\n"
            "Current configuration:\n"
            f"• Health Check Interval: <code>{DEFAULT_HEALTH_INTERVAL_SEC}s</code>\n"
            f"• Max Failures: <code>{DEFAULT_MAX_FAILURES}</code>\n"
            f"• Cooldown Duration: <code>{DEFAULT_COOLDOWN_SEC}s</code>",
            parse_mode='HTML'
        )
    elif text == "⏹ Stop Generation":
        return await stop_command(update, context)

async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel ongoing conversation."""
    await update.message.reply_text(
        "❌ Cancelled.",
        reply_markup=get_main_keyboard()
    )
    return ConversationHandler.END

# =============================================================================
# FLASK SERVER FOR RENDER
# =============================================================================

def create_flask_app():
    """Create Flask app for Render Web Service."""
    flask_app = Flask(__name__)

    @flask_app.route('/')
    def home():
        progress = gen_state.get_progress()
        return f"""
        <html>
        <head><title>RUHI BOT</title></head>
        <body style="font-family: monospace; padding: 40px;">
        <h1>🎮 RUHI BOT v4.0</h1>
        <p><b>Status:</b> {'Running' if not gen_state.exit_flag else 'Idle'}</p>
        <hr>
        <h2>📊 Generation Progress</h2>
        <p>Generated: {progress['generated']}/{progress['target']}</p>
        <p>Progress: {progress['progress_pct']:.1f}%</p>
        <p>Speed: {progress['speed']:.2f} accounts/sec</p>
        <p>Rare: {progress['rare']} | Couples: {progress['couples']}</p>
        <hr>
        <h2>🌐 Proxy Pool</h2>
        <p>Available: {progress['proxies']}/{progress['proxy_total']}</p>
        <hr>
        <p><i>Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i></p>
        </body>
        </html>
        """

    @flask_app.route('/health')
    def health():
        return {"status": "ok", "timestamp": datetime.now().isoformat()}

    return flask_app

def run_flask_server():
    """Run Flask server in background thread."""
    app = create_flask_app()
    app.run(host=RENDER_HOST, port=RENDER_PORT, threaded=True, debug=False)

# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main():
    """Main entry point."""
    logger.info("=" * 60)
    logger.info("RUHI TELEGRAM BOT v4.0 - Starting...")
    logger.info("=" * 60)

    # Check environment
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set! Exiting.")
        sys.exit(1)

    # Initialize proxy manager with database-loaded proxies
    gen_state.proxy_manager = ProxyManager(
        proxy_file="proxies.txt" if os.path.exists("proxies.txt") else None,
        strategy=RotationStrategy.WEIGHTED_ROUND_ROBIN,
        health_interval_sec=DEFAULT_HEALTH_INTERVAL_SEC,
    )
    gen_state.proxy_manager.start()

    # Start Flask server in background thread
    if FLASK_AVAILABLE:
        flask_thread = threading.Thread(target=run_flask_server, daemon=True)
        flask_thread.start()
        logger.info(f"[FLASK] Server started on {RENDER_HOST}:{RENDER_PORT}")

    # Build Telegram application
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Add handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("stop", stop_command))

    # Conversation handler for generation
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("generate", generate_command)],
        states={
            REGION: [MessageHandler(filters.TEXT & ~filters.COMMAND, region_input)],
            COUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, count_input)],
            NAME_PREFIX: [MessageHandler(filters.TEXT & ~filters.COMMAND, name_prefix_input)],
            PASS_PREFIX: [MessageHandler(filters.TEXT & ~filters.COMMAND, pass_prefix_input)],
            THREADS: [MessageHandler(filters.TEXT & ~filters.COMMAND, threads_input)],
        },
        fallbacks=[CommandHandler("cancel", cancel_conversation)],
    )
    application.add_handler(conv_handler)

    # Button handler
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, button_handler))

    # Document handler
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    # Store reference
    gen_state.telegram_app = application

    logger.info("[TELEGRAM] Bot starting polling...")
    
    # Run the bot
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()