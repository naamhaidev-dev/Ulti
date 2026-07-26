#!/usr/bin/env python3
"""
⚡ GITHUB CODESPACE BOT v5.0 – DARK UI EDITION
├─ 🌛 Modern dark theme with emoji-rich interface
├─ 🗓 Smooth animations & mobile-friendly layout
├─ 💎 Easy navigation with categorized commands
├─ 💼 Professional look with clean formatting
└─ 🔒 Thread-safe for multiple users
"""

import os
import sys
import time
import re
import sqlite3
import logging
import threading
import base64
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any
from urllib.parse import urlparse, quote
from io import BytesIO

import requests
from telebot import TeleBot
from telebot.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from dotenv import load_dotenv

try:
    from pymongo import MongoClient
    MONGO_AVAILABLE = True
except ImportError:
    MONGO_AVAILABLE = False

load_dotenv()

# ======================== CONFIG ========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN not set!")

GITHUB_API_BASE = os.getenv("GITHUB_API_BASE", "https://api.github.com")
DB_TYPE = os.getenv("DB_TYPE", "sqlite").lower()
DB_PATH = os.getenv("DB_PATH", "tokens.db")
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "codespace_bot")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()]
MAX_RETRIES = 3
RETRY_BACKOFF = 2
MAX_FILE_SIZE = 50 * 1024 * 1024
PAGE_SIZE = 15
CACHE_TTL = 300
DOWNLOAD_CHUNK_SIZE = 1024 * 1024

# ======================== UI CONSTANTS ========================
UI = {
    "header": "╔═══════════════════════════════════╗\n║  💥 𝙋𝘼𝙄𝘿 𝘽𝙊𝙏 24/7 💥  ║\n╚═══════════════════════════════════╝",
    "separator": "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    "bullet": "▸",
    "arrow": "➜",
    "success": "✅",
    "error": "❌",
    "warning": "⚠️",
    "info": "ℹ️",
    "loading": "⏳",
    "done": "🎯",
    "rocket": "🚀",
    "crown": "👑",
    "star": "⭐",
    "folder": "📂",
    "file": "📄",
    "github": "🐙",
    "cloud": "☁️",
    "database": "🗄️",
    "lock": "🔒",
    "unlock": "🔓",
    "power": "⚡",
    "gear": "⚙️",
    "wrench": "🔧",
}

# ======================== THREAD-SAFE CACHE ========================
class ThreadSafeCache:
    def __init__(self, ttl: int = 300, max_size: int = 50):
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._lock = threading.RLock()
        self.ttl = ttl
        self.max_size = max_size
    
    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key in self._cache:
                data, timestamp = self._cache[key]
                if time.time() - timestamp < self.ttl:
                    return data
                del self._cache[key]
            return None
    
    def set(self, key: str, data: Any):
        with self._lock:
            if len(self._cache) >= self.max_size:
                oldest = min(self._cache.keys(), key=lambda k: self._cache[k][1])
                del self._cache[oldest]
            self._cache[key] = (data, time.time())
    
    def clear(self):
        with self._lock:
            self._cache.clear()
    
    def size(self) -> int:
        with self._lock:
            return len(self._cache)

# ======================== THREAD-SAFE DATABASE ========================
class TokenDB:
    def __init__(self, db_type: str = "sqlite", **kwargs):
        self.db_type = db_type.lower()
        self._lock = threading.RLock()
        if self.db_type == "mongo":
            if not MONGO_AVAILABLE:
                raise ImportError("pymongo not installed")
            self._init_mongo(kwargs.get("mongo_uri"), kwargs.get("mongo_db_name"))
        else:
            self._init_sqlite(kwargs.get("db_path", "tokens.db"))
    
    def _init_sqlite(self, db_path: str):
        self.db_path = db_path
        with self._lock:
            conn = sqlite3.connect(db_path, check_same_thread=False)
            c = conn.cursor()
            c.execute("""CREATE TABLE IF NOT EXISTS tokens (
                user_id INTEGER PRIMARY KEY, 
                token TEXT NOT NULL, 
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP, 
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )""")
            c.execute("""CREATE TABLE IF NOT EXISTS actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                user_id INTEGER, 
                action TEXT, 
                details TEXT, 
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )""")
            conn.commit()
            conn.close()
    
    def _init_mongo(self, uri, db_name):
        self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        self.client.admin.command('ping')
        self.db = self.client[db_name]
        self.tokens_col = self.db["tokens"]
        self.actions_col = self.db["actions"]
        self.tokens_col.create_index("user_id", unique=True)
    
    def save_token(self, user_id: int, token: str):
        with self._lock:
            if self.db_type == "mongo":
                self.tokens_col.update_one(
                    {"user_id": user_id}, 
                    {"$set": {"token": token, "updated_at": datetime.utcnow()}}, 
                    upsert=True
                )
            else:
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                c = conn.cursor()
                c.execute(
                    "INSERT OR REPLACE INTO tokens (user_id, token, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                    (user_id, token)
                )
                conn.commit()
                conn.close()
    
    def get_token(self, user_id: int) -> Optional[str]:
        with self._lock:
            if self.db_type == "mongo":
                doc = self.tokens_col.find_one({"user_id": user_id})
                return doc["token"] if doc else None
            else:
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                c = conn.cursor()
                c.execute("SELECT token FROM tokens WHERE user_id = ?", (user_id,))
                row = c.fetchone()
                conn.close()
                return row[0] if row else None
    
    def delete_token(self, user_id: int):
        with self._lock:
            if self.db_type == "mongo":
                self.tokens_col.delete_one({"user_id": user_id})
            else:
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                c = conn.cursor()
                c.execute("DELETE FROM tokens WHERE user_id = ?", (user_id,))
                conn.commit()
                conn.close()
    
    def log_action(self, user_id: int, action: str, details: str = ""):
        with self._lock:
            if self.db_type == "mongo":
                self.db["actions"].insert_one({
                    "user_id": user_id, 
                    "action": action, 
                    "details": details, 
                    "timestamp": datetime.utcnow()
                })
            else:
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                c = conn.cursor()
                c.execute(
                    "INSERT INTO actions (user_id, action, details) VALUES (?, ?, ?)",
                    (user_id, action, details)
                )
                conn.commit()
                conn.close()

try:
    if DB_TYPE == "mongo":
        db = TokenDB(db_type="mongo", mongo_uri=MONGO_URI, mongo_db_name=MONGO_DB_NAME)
    else:
        db = TokenDB(db_type="sqlite", db_path=DB_PATH)
except Exception as e:
    logging.error(f"DB init fail: {e}")
    sys.exit(1)

# ======================== BOT INIT ========================
bot = TeleBot(BOT_TOKEN, threaded=True)

# Thread-safe globals
code_locks: Dict[str, threading.Lock] = {}
code_lock_lock = threading.Lock()
user_context: Dict[int, Dict] = {}
user_context_lock = threading.RLock()
tree_cache = ThreadSafeCache(ttl=CACHE_TTL, max_size=50)
rate_limit_warnings: Dict[int, bool] = {}
rate_limit_lock = threading.Lock()

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ======================== UI HELPERS ========================
def format_ui_header(title: str) -> str:
    """Generate beautiful header with box drawing"""
    line = "═" * min(len(title) + 4, 40)
    return f"╔{line}╗\n║  {title}  ║\n╚{line}╝"

def create_main_menu(user_id: int) -> InlineKeyboardMarkup:
    """Main menu with dark theme buttons"""
    markup = InlineKeyboardMarkup(row_width=2)
    
    # First row - Main features
    markup.row(
        InlineKeyboardButton("🐙 Codespaces", callback_data="menu_codespaces"),
        InlineKeyboardButton("📂 Files", callback_data="menu_files")
    )
    
    # Second row - Utilities
    markup.row(
        InlineKeyboardButton("🔑 Token", callback_data="menu_token"),
        InlineKeyboardButton("⚙️ Settings", callback_data="menu_settings")
    )
    
    # Third row - Help & Info
    markup.row(
        InlineKeyboardButton("❓ Help", callback_data="menu_help"),
        InlineKeyboardButton("📊 Stats", callback_data="menu_stats")
    )
    
    # Admin only
    if is_admin(user_id):
        markup.row(
            InlineKeyboardButton("👑 Admin Panel", callback_data="menu_admin")
        )
    
    return markup

def create_codespace_menu(codespace_name: str, state: str) -> InlineKeyboardMarkup:
    """Dynamic codespace action menu"""
    markup = InlineKeyboardMarkup(row_width=2)
    buttons = []
    
    # State-based actions
    if state in ("Stopped", "Shutdown"):
        buttons.append(InlineKeyboardButton("▶️ Start", callback_data=f"start_{codespace_name}"))
    elif state in ("Available", "Running", "Starting", "Queued"):
        buttons.append(InlineKeyboardButton("⏹ Stop", callback_data=f"stop_{codespace_name}"))
    
    buttons.append(InlineKeyboardButton("🔄 Refresh", callback_data=f"refresh_{codespace_name}"))
    buttons.append(InlineKeyboardButton("📋 Details", callback_data=f"details_{codespace_name}"))
    
    # Row 1: Action buttons
    if len(buttons) >= 2:
        markup.row(buttons[0], buttons[1] if len(buttons) > 1 else None)
    if len(buttons) > 2:
        markup.row(buttons[2])
    
    # Bottom navigation
    markup.row(
        InlineKeyboardButton("🔙 Back to Menu", callback_data="menu_codespaces")
    )
    
    return markup

def create_file_browser_menu(ctx: Dict, page: int, total_pages: int) -> InlineKeyboardMarkup:
    """File browser with dark theme buttons"""
    markup = InlineKeyboardMarkup(row_width=2)
    owner = ctx.get("owner", "")
    repo = ctx.get("repo", "")
    path = ctx.get("path", "")
    tree = ctx.get("tree", [])
    
    if not tree:
        return markup
    
    files, dirs = list_directory_from_tree(tree, path)
    total_files = len(files)
    page = min(max(page, 0), total_pages - 1) if total_files > 0 else 0
    
    # Navigation row
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️", callback_data=f"pg|{page-1}"))
    nav_buttons.append(InlineKeyboardButton(f"{page+1}/{total_pages if total_files > 0 else 1}", callback_data="noop"))
    if page < total_pages - 1 and total_files > 0:
        nav_buttons.append(InlineKeyboardButton("➡️", callback_data=f"pg|{page+1}"))
    
    if nav_buttons:
        markup.row(*nav_buttons)
    
    # Path navigation
    if path:
        parent = "/".join(path.split("/")[:-1])
        markup.row(
            InlineKeyboardButton("📂 ..", callback_data=f"dir|{owner}|{repo}|{parent}")
        )
    
    # Directories
    dir_buttons = []
    for d in dirs[:10]:
        new_path = f"{path}/{d}" if path else d
        dir_buttons.append(InlineKeyboardButton(f"📁 {d}", callback_data=f"dir|{owner}|{repo}|{new_path}"))
    
    for i in range(0, len(dir_buttons), 2):
        if i + 1 < len(dir_buttons):
            markup.row(dir_buttons[i], dir_buttons[i+1])
        else:
            markup.row(dir_buttons[i])
    
    # Files (paginated)
    if total_files > 0:
        start = page * PAGE_SIZE
        end = min(start + PAGE_SIZE, total_files)
        page_files = files[start:end]
        
        for f in page_files[:10]:
            fpath = f.get("path", "")
            fname = fpath.split("/")[-1]
            markup.row(
                InlineKeyboardButton(f"📄 {fname[:20]}", callback_data=f"file|{owner}|{repo}|{fpath}")
            )
    
    # Bottom actions
    markup.row(
        InlineKeyboardButton("🔄 Refresh", callback_data=f"refresh_tree|{owner}|{repo}"),
        InlineKeyboardButton("📦 Download ZIP", callback_data=f"zip_repo|{owner}|{repo}")
    )
    markup.row(
        InlineKeyboardButton("🔙 Back", callback_data="menu_files")
    )
    
    return markup

# ======================== THREAD-SAFE HELPERS ========================
def get_codespace_lock(name: str) -> threading.Lock:
    with code_lock_lock:
        if name not in code_locks:
            code_locks[name] = threading.Lock()
        return code_locks[name]

def get_user_context(user_id: int) -> Optional[Dict]:
    with user_context_lock:
        return user_context.get(user_id)

def set_user_context(user_id: int, data: Dict):
    with user_context_lock:
        user_context[user_id] = data

def clear_user_context(user_id: int):
    with user_context_lock:
        user_context.pop(user_id, None)

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def safe_edit(msg, text, parse_mode="Markdown", reply_markup=None):
    try:
        bot.edit_message_text(text, msg.chat.id, msg.message_id, parse_mode=parse_mode, reply_markup=reply_markup)
    except Exception as e:
        logger.warning(f"Edit failed: {e}")

def safe_send(chat_id, text, parse_mode="Markdown", reply_markup=None):
    try:
        return bot.send_message(chat_id, text, parse_mode=parse_mode, reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Send failed: {e}")
        return None

# ======================== GITHUB API HELPERS ========================
def github_headers(token: Optional[str] = None) -> Dict[str, str]:
    h = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "CodespaceBot/5.0"
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h

def api_call(method, url, token=None, retries=MAX_RETRIES, user_id=None):
    headers = github_headers(token)
    for attempt in range(retries + 1):
        try:
            resp = method(url, headers=headers, timeout=30)
            
            if user_id and resp:
                with rate_limit_lock:
                    remaining = resp.headers.get("X-RateLimit-Remaining")
                    if remaining and remaining.isdigit():
                        rem = int(remaining)
                        if rem < 10 and not rate_limit_warnings.get(user_id, False):
                            rate_limit_warnings[user_id] = True
                            bot.send_message(user_id, f"⚠️ Rate limit low! Only {rem} requests remaining.")
                        elif rem > 20:
                            rate_limit_warnings[user_id] = False
            
            if resp.status_code == 429:
                reset = int(resp.headers.get("X-RateLimit-Reset", 0))
                wait = max(1, reset - int(time.time()))
                if user_id:
                    bot.send_message(user_id, f"⏳ Rate limited, waiting {wait}s...")
                time.sleep(wait + 2)
                continue
            
            if resp.status_code >= 500 and attempt < retries:
                time.sleep(RETRY_BACKOFF ** (attempt + 1))
                continue
            
            return resp, None
            
        except requests.Timeout:
            if attempt < retries:
                time.sleep(RETRY_BACKOFF ** (attempt + 1))
                continue
            return None, "GitHub API timeout"
        except Exception as e:
            if attempt < retries:
                time.sleep(RETRY_BACKOFF ** (attempt + 1))
                continue
            return None, f"Error: {e}"
    
    return None, "Max retries reached"

def get_default_branch(owner: str, repo: str, token: Optional[str] = None) -> Optional[str]:
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}"
    resp, err = api_call(requests.get, url, token, retries=1)
    if resp and resp.status_code == 200:
        return resp.json().get("default_branch", "main")
    return None

def get_repo_tree(owner: str, repo: str, branch: str, token: Optional[str] = None) -> Tuple[Optional[List[Dict]], Optional[str]]:
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/git/refs/heads/{branch}"
    resp, err = api_call(requests.get, url, token)
    if err or not resp or resp.status_code != 200:
        return None, f"Branch error: {err or resp.status_code}"
    
    commit_sha = resp.json().get("object", {}).get("sha")
    if not commit_sha:
        return None, "Could not get commit SHA"
    
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/git/commits/{commit_sha}"
    resp, err = api_call(requests.get, url, token)
    if err or not resp or resp.status_code != 200:
        return None, "Could not get commit tree"
    
    tree_sha = resp.json().get("tree", {}).get("sha")
    if not tree_sha:
        return None, "Could not get tree SHA"
    
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/git/trees/{tree_sha}?recursive=1"
    resp, err = api_call(requests.get, url, token)
    if err or not resp or resp.status_code != 200:
        return None, f"Tree fetch error: {err or resp.status_code}"
    
    data = resp.json()
    return data.get("tree", []), None

def get_cached_tree(owner: str, repo: str, token: Optional[str] = None) -> Tuple[Optional[List[Dict]], Optional[str], Optional[str]]:
    cache_key = f"{owner}/{repo}"
    cached = tree_cache.get(cache_key)
    if cached:
        tree, branch = cached
        return tree, branch, None
    
    branch = get_default_branch(owner, repo, token)
    if not branch:
        return None, None, "Could not determine default branch"
    
    tree, err = get_repo_tree(owner, repo, branch, token)
    if err or tree is None:
        return None, branch, err or "Failed to fetch tree"
    
    tree_cache.set(cache_key, (tree, branch))
    return tree, branch, None

def list_directory_from_tree(tree: List[Dict], path: str = "") -> Tuple[List[Dict], List[str]]:
    path = path.strip("/")
    files = []
    dirs_set = set()
    path_prefix = f"{path}/" if path else ""
    
    for item in tree:
        item_path = item.get("path", "")
        if not item_path.startswith(path_prefix) and (path and item_path != path):
            continue
        remaining = item_path[len(path_prefix):]
        if "/" in remaining:
            dir_name = remaining.split("/")[0]
            dirs_set.add(dir_name)
        elif item.get("type") == "blob":
            files.append(item)
    
    return files, sorted(dirs_set)

def parse_github_url(text: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not text.startswith("http"):
        text = "https://" + text
    parsed = urlparse(text)
    if parsed.netloc not in ("github.com", "www.github.com"):
        return None, None, None
    parts = parsed.path.strip("/").split("/")
    if len(parts) < 2:
        return None, None, None
    owner = parts[0]
    repo = parts[1].replace(".git", "")
    subpath = ""
    if len(parts) > 4 and parts[2] in ("tree", "blob", "raw"):
        subpath = "/".join(parts[4:])
    elif len(parts) > 2:
        subpath = "/".join(parts[2:])
    return owner, repo, subpath if subpath else None

def get_file_content(owner: str, repo: str, path: str, branch: str, token: Optional[str] = None) -> Tuple[Optional[bytes], Optional[str], Optional[str]]:
    encoded_path = quote(path, safe="")
    raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{encoded_path}"
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    
    try:
        resp = requests.get(raw_url, headers=headers, timeout=60, stream=True)
        if resp.status_code == 200:
            total_size = int(resp.headers.get("Content-Length", 0))
            if total_size > MAX_FILE_SIZE:
                return None, None, f"File too large ({total_size//1024//1024}MB)"
            
            data = bytearray()
            for chunk in resp.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                if chunk:
                    data.extend(chunk)
            return bytes(data), resp.headers.get("Content-Type", "text/plain"), None
        else:
            return None, None, f"HTTP {resp.status_code}"
    except Exception as e:
        return None, None, f"Download error: {e}"

# ======================== CODESPACE HELPERS ========================
def get_codespaces(token, user_id=None):
    url = f"{GITHUB_API_BASE}/user/codespaces"
    resp, err = api_call(requests.get, url, token, user_id=user_id)
    if err or not resp or resp.status_code != 200:
        return None
    return resp.json().get("codespaces", [])

def start_codespace(token, name, user_id=None):
    resp, err = api_call(requests.post, f"{GITHUB_API_BASE}/user/codespaces/{name}/start", token, user_id=user_id)
    if err or not resp:
        return False, err or "Request failed"
    if resp.status_code in (200, 202):
        return True, "Start request accepted"
    return False, f"Failed (status {resp.status_code})"

def stop_codespace(token, name, user_id=None):
    resp, err = api_call(requests.post, f"{GITHUB_API_BASE}/user/codespaces/{name}/stop", token, user_id=user_id)
    if err or not resp:
        return False, err or "Request failed"
    if resp.status_code in (200, 202):
        return True, "Stop request accepted"
    return False, f"Failed (status {resp.status_code})"

def wait_for_state(token, name, target, user_id=None, timeout=45, interval=3):
    start = time.time()
    while time.time() - start < timeout:
        codespaces = get_codespaces(token, user_id)
        if codespaces is None:
            return False, "Status fetch failed"
        found = next((c for c in codespaces if c.get("name") == name), None)
        if not found:
            return False, f"Codespace {name} not found"
        state = found.get("state", "Unknown")
        if target == "Running" and state in ("Available", "Running", "Starting"):
            return True, f"State: {state}"
        if target == "Stopped" and state in ("Stopped", "Shutdown"):
            return True, f"State: {state}"
        time.sleep(interval)
    return False, f"Timeout waiting for {target}"

def format_codespace(cs: Dict) -> str:
    name = cs.get("name", "N/A")
    state = cs.get("state", "Unknown")
    repo = cs.get("repository", {}).get("full_name", "N/A")
    machine = cs.get("machine", {})
    machine_name = machine.get("display_name") or machine.get("name", "N/A")
    location = cs.get("location", "N/A")
    created = (cs.get("created_at") or "N/A")[:10]
    
    emoji = {
        "Available": "🟢", "Starting": "🟡", "Running": "🟢",
        "Stopping": "🟠", "Stopped": "🔴", "Shutdown": "🔴",
        "Deleted": "💀", "Queued": "⏳", "Unknown": "❓"
    }.get(state, "❓")
    
    return f"""╔═══════════════════════════════════╗
║  {emoji} {name}  ║
╠═══════════════════════════════════╣
║ 📦 Repo: {repo[:30]}  ║
║ 💻 Machine: {machine_name[:25]}  ║
║ 📍 Location: {location[:20]}  ║
║ 📅 Created: {created}  ║
║ 📊 Status: {state}  ║
╚═══════════════════════════════════╝"""

# ======================== COMMAND HANDLERS ========================
@bot.message_handler(commands=["start"])
def cmd_start(message):
    user_id = message.from_user.id
    welcome = f"""🌟 {UI['header']}
{UI['separator']}
{UI['rocket']} *Welcome to PaID Bot 24/7*
{UI['github']} *GitHub Codespace Controller*
{UI['crown']} *OG SAGAR* 😈
{UI['separator']}

{UI['info']} Use /help for all commands
{UI['power']} Bot is fully threaded & stable

💡 *Quick Commands:*
{UI['bullet']} `/settoken` - Set GitHub token
{UI['bullet']} `/list` - View codespaces
{UI['bullet']} `/files` - Browse repositories
{UI['bullet']} `/grep` - Search files
{UI['bullet']} `/zip` - Download repo

{UI['separator']}
📌 *Click menu below to navigate* 👇"""

    safe_send(message.chat.id, welcome, reply_markup=create_main_menu(user_id))

@bot.message_handler(commands=["help"])
def cmd_help(message):
    user_id = message.from_user.id
    help_text = f"""📚 *PAID BOT HELP MENU*
{UI['separator']}

🔑 *Token Management*
{UI['bullet']} `/settoken <token>` - Save GitHub token
{UI['bullet']} `/tokens` - View stored token
{UI['bullet']} `/rmtoken` - Delete token

🐙 *Codespace Controls*
{UI['bullet']} `/list` - List all codespaces
{UI['bullet']} `/start <name>` - Start a codespace
{UI['bullet']} `/stop <name>` - Stop a codespace

📂 *File Browser*
{UI['bullet']} `/files <repo_url> [path]` - Browse repository
{UI['bullet']} `/findfile <url> <filename>` - Search files
{UI['bullet']} `/getfile <url> <path>` - Download file
{UI['bullet']} `/preview <url> <path>` - Preview file
{UI['bullet']} `/grep <url> <pattern>` - Search file content
{UI['bullet']} `/zip <url> [branch]` - Download as ZIP

⚙️ *Utilities*
{UI['bullet']} `/ratelimit` - Check GitHub API limit
{UI['bullet']} `/clear` - Clear session cache

👑 *Admin Commands*
{UI['bullet']} `/sessions` - Active sessions
{UI['bullet']} `/users` - All users
{UI['bullet']} `/broadcast` - Send announcement
{UI['bullet']} `/stats` - Bot statistics
{UI['bullet']} `/clearcache` - Clear cache

{UI['separator']}
💡 *Click menu below to navigate* 👇"""

    safe_send(message.chat.id, help_text, reply_markup=create_main_menu(user_id))

@bot.message_handler(commands=["settoken"])
def cmd_settoken(message):
    user_id = message.from_user.id
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        safe_send(message.chat.id, f"{UI['error']} Usage: `/settoken <github_token>`", parse_mode="Markdown")
        return
    
    token = parts[1].strip()
    if not token.startswith(("ghp_", "gho_", "ghu_", "ghs_", "ghr_")) or len(token) < 20:
        safe_send(message.chat.id, f"{UI['error']} Invalid token format!", parse_mode="Markdown")
        return
    
    codespaces = get_codespaces(token, user_id)
    if codespaces is None:
        safe_send(message.chat.id, f"{UI['error']} Invalid token. Need `codespace` scope.", parse_mode="Markdown")
        return
    
    db.save_token(user_id, token)
    db.log_action(user_id, "settoken", "saved")
    safe_send(message.chat.id, f"{UI['success']} Token saved! Found {len(codespaces)} codespace(s).")

@bot.message_handler(commands=["list"])
def cmd_list(message):
    user_id = message.from_user.id
    token = db.get_token(user_id)
    if not token:
        safe_send(message.chat.id, f"{UI['error']} Please set token first with /settoken")
        return
    
    msg = safe_send(message.chat.id, f"{UI['loading']} Fetching codespaces...")
    codespaces = get_codespaces(token, user_id)
    
    if codespaces is None:
        safe_edit(msg, f"{UI['error']} Failed to fetch codespaces. Check token.")
        return
    
    if not codespaces:
        safe_edit(msg, f"{UI['info']} No active codespaces found.")
        return
    
    safe_edit(msg, f"{UI['success']} Found {len(codespaces)} codespace(s):")
    
    for cs in codespaces:
        name = cs.get("name", "unknown")
        state = cs.get("state", "Unknown")
        repo = cs.get("repository", {}).get("full_name", "N/A")
        
        card = f"""╔═══════════════════════════════════╗
║  {UI['folder']} {name[:20]}  ║
╠═══════════════════════════════════╣
║ 📦 {repo[:35]}  ║
║ 📊 Status: {state}  ║
╚═══════════════════════════════════╝"""
        
        safe_send(message.chat.id, card, reply_markup=create_codespace_menu(name, state))

@bot.message_handler(commands=["files"])
def cmd_files(message):
    user_id = message.from_user.id
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        safe_send(message.chat.id, f"{UI['error']} Usage: `/files <repo_url> [path]`\nExample: `/files https://github.com/EchoMusicApp/Echo-Music`", parse_mode="Markdown")
        return
    
    token = db.get_token(user_id)
    if not token:
        safe_send(message.chat.id, f"{UI['error']} Please set token first with /settoken")
        return
    
    text = parts[1].strip()
    if " " in text:
        url_part, path_part = text.split(" ", 1)
    else:
        url_part, path_part = text, ""
    
    parsed = parse_github_url(url_part)
    if not parsed:
        safe_send(message.chat.id, f"{UI['error']} Invalid GitHub URL.")
        return
    
    owner, repo, initial_path = parsed
    final_path = path_part if path_part else (initial_path if initial_path else "")
    
    msg = safe_send(message.chat.id, f"{UI['loading']} Fetching repository tree... This may take a moment.")
    
    tree, branch, err = get_cached_tree(owner, repo, token)
    if err or tree is None:
        safe_edit(msg, f"{UI['error']} {err or 'Failed to fetch repository'}")
        return
    
    ctx = {
        "owner": owner,
        "repo": repo,
        "path": final_path,
        "page": 0,
        "token": token,
        "tree": tree,
        "branch": branch
    }
    set_user_context(user_id, ctx)
    
    safe_edit(msg, f"{UI['success']} Loaded {len(tree)} files. Browsing...", 
              reply_markup=create_file_browser_menu(ctx, 0, 1))

# ======================== CALLBACK HANDLER ========================
@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call: CallbackQuery):
    data = call.data
    chat_id = call.message.chat.id
    user_id = call.from_user.id
    
    # ===== MAIN MENU NAVIGATION =====
    if data == "menu_codespaces":
        token = db.get_token(user_id)
        if not token:
            safe_send(chat_id, f"{UI['error']} Please set token first with /settoken")
            bot.answer_callback_query(call.id)
            return
        
        safe_edit(call.message, f"{UI['loading']} Fetching codespaces...")
        codespaces = get_codespaces(token, user_id)
        if codespaces is None:
            safe_edit(call.message, f"{UI['error']} Failed to fetch codespaces.")
            bot.answer_callback_query(call.id)
            return
        
        if not codespaces:
            safe_edit(call.message, f"{UI['info']} No active codespaces found.")
            bot.answer_callback_query(call.id)
            return
        
        text = f"""🐙 *YOUR CODESPACES*
{UI['separator']}
Found {len(codespaces)} codespace(s)
Click on any to manage 👇
{UI['separator']}"""
        
        markup = InlineKeyboardMarkup(row_width=1)
        for cs in codespaces:
            name = cs.get("name", "unknown")
            state = cs.get("state", "Unknown")
            emoji = {"Available":"🟢","Running":"🟢","Stopped":"🔴","Shutdown":"🔴"}.get(state, "🟡")
            markup.add(InlineKeyboardButton(f"{emoji} {name}", callback_data=f"cs_{name}"))
        
        markup.add(InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main"))
        safe_edit(call.message, text, parse_mode="Markdown", reply_markup=markup)
        bot.answer_callback_query(call.id)
        return
    
    if data == "menu_files":
        safe_edit(call.message, f"""📂 *FILE BROWSER*
{UI['separator']}
Enter a repository URL to browse:
`/files https://github.com/user/repo`

Or use the file commands:
{UI['bullet']} `/getfile <url> <path>`
{UI['bullet']} `/grep <url> <pattern>`
{UI['bullet']} `/zip <url> [branch]`

{UI['separator']}
Click below to go back 👇""", 
        parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")]]))
        bot.answer_callback_query(call.id)
        return
    
    if data == "menu_token":
        token = db.get_token(user_id)
        if token:
            masked = token[:8] + "..." + token[-8:] if len(token) > 16 else "***"
            text = f"""🔑 *TOKEN MANAGEMENT*
{UI['separator']}
{UI['success']} Token stored: `{masked}`

{UI['info']} Token starts with: `{token[:4]}...`

{UI['warning']} Never share your token!
{UI['separator']}
Actions:"""
            markup = InlineKeyboardMarkup(row_width=2)
            markup.add(
                InlineKeyboardButton("🗑️ Delete Token", callback_data="token_delete"),
                InlineKeyboardButton("🔄 Update Token", callback_data="token_update")
            )
            markup.add(InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main"))
            safe_edit(call.message, text, parse_mode="Markdown", reply_markup=markup)
        else:
            text = f"""🔑 *TOKEN MANAGEMENT*
{UI['separator']}
{UI['error']} No token stored.

{UI['info']} Use `/settoken <token>` to save your GitHub token.

Your token needs:
{UI['bullet']} `codespace` scope
{UI['bullet']} `repo` scope (for private repos)

{UI['separator']}
Click below to go back 👇"""
            safe_edit(call.message, text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")]]))
        bot.answer_callback_query(call.id)
        return
    
    if data == "menu_settings":
        text = f"""⚙️ *SETTINGS*
{UI['separator']}
{UI['info']} Current Configuration:

{UI['bullet']} DB Type: `{DB_TYPE.upper()}`
{UI['bullet']} Admin: `{len(ADMIN_IDS)}` users
{UI['bullet']} Cache TTL: `{CACHE_TTL}s`
{UI['bullet']} Page Size: `{PAGE_SIZE}` items

{UI['separator']}
{UI['warning']} Settings can only be changed
via environment variables.

Click below to go back 👇"""
        safe_edit(call.message, text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")]]))
        bot.answer_callback_query(call.id)
        return
    
    if data == "menu_help":
        help_text = f"""📚 *QUICK HELP*
{UI['separator']}

🚀 *Getting Started:*
1️⃣ `/settoken <token>` - Set your token
2️⃣ `/list` - View codespaces
3️⃣ `/files <repo>` - Browse files

📂 *File Commands:*
{UI['bullet']} `/files <url> [path]` - Browse
{UI['bullet']} `/getfile <url> <path>` - Download
{UI['bullet']} `/grep <url> <pattern>` - Search content
{UI['bullet']} `/zip <url> [branch]` - Download ZIP

🔑 *Token:*
{UI['bullet']} `/tokens` - View stored token
{UI['bullet']} `/rmtoken` - Delete token

{UI['separator']}
💡 Click below for full help 👇"""
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(
            InlineKeyboardButton("📖 Full Help", callback_data="help_full"),
            InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")
        )
        safe_edit(call.message, help_text, parse_mode="Markdown", reply_markup=markup)
        bot.answer_callback_query(call.id)
        return
    
    if data == "help_full":
        cmd_help(call.message)
        bot.answer_callback_query(call.id)
        return
    
    if data == "menu_stats":
        ctx_count = 0
        with user_context_lock:
            ctx_count = len(user_context)
        
        text = f"""📊 *BOT STATISTICS*
{UI['separator']}

{UI['info']} *System Stats:*
{UI['bullet']} Active Sessions: `{ctx_count}`
{UI['bullet']} Cache Size: `{tree_cache.size()}`
{UI['bullet']} Python Version: `{sys.version[:10]}`
{UI['bullet']} DB Type: `{DB_TYPE.upper()}`

{UI['info']} *Rate Limits:*
{UI['bullet']} Max Retries: `{MAX_RETRIES}`
{UI['bullet']} Backoff: `{RETRY_BACKOFF}s`

{UI['separator']}
Click below to go back 👇"""
        safe_edit(call.message, text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")]]))
        bot.answer_callback_query(call.id)
        return
    
    if data == "menu_admin" and is_admin(user_id):
        text = f"""👑 *ADMIN PANEL*
{UI['separator']}

{UI['gear']} *Admin Commands:*
{UI['bullet']} `/users` - List all users
{UI['bullet']} `/sessions` - Active sessions
{UI['bullet']} `/broadcast` - Send announcement
{UI['bullet']} `/stats` - Bot statistics
{UI['bullet']} `/clearcache` - Clear cache

{UI['separator']}
Click below to go back 👇"""
        safe_edit(call.message, text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")]]))
        bot.answer_callback_query(call.id)
        return
    
    if data == "menu_main":
        safe_edit(call.message, f"🌟 *Main Menu*\n{UI['separator']}\nSelect an option below 👇", parse_mode="Markdown", reply_markup=create_main_menu(user_id))
        bot.answer_callback_query(call.id)
        return
    
    # ===== TOKEN MANAGEMENT =====
    if data == "token_delete":
        db.delete_token(user_id)
        safe_edit(call.message, f"{UI['success']} Token deleted successfully!", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")]]))
        bot.answer_callback_query(call.id)
        return
    
    if data == "token_update":
        safe_edit(call.message, f"{UI['info']} Use `/settoken <new_token>` to update your token.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")]]))
        bot.answer_callback_query(call.id)
        return
    
    # ===== CODESPACE INDIVIDUAL =====
    if data.startswith("cs_"):
        name = data[3:]
        token = db.get_token(user_id)
        if not token:
            safe_send(chat_id, f"{UI['error']} Token not set.")
            bot.answer_callback_query(call.id)
            return
        
        codespaces = get_codespaces(token, user_id)
        if codespaces is None:
            safe_edit(call.message, f"{UI['error']} Failed to fetch codespaces.")
            bot.answer_callback_query(call.id)
            return
        
        cs = next((c for c in codespaces if c.get("name") == name), None)
        if not cs:
            safe_edit(call.message, f"{UI['error']} Codespace `{name}` not found.")
            bot.answer_callback_query(call.id)
            return
        
        state = cs.get("state", "Unknown")
        safe_edit(call.message, format_codespace(cs), parse_mode="Markdown", reply_markup=create_codespace_menu(name, state))
        bot.answer_callback_query(call.id)
        return
    
    # ===== CODESPACE ACTIONS =====
    if data.startswith("start_"):
        name = data[6:]
        token = db.get_token(user_id)
        if not token:
            safe_send(chat_id, f"{UI['error']} Token not set.")
            bot.answer_callback_query(call.id)
            return
        
        safe_edit(call.message, f"{UI['loading']} Starting `{name}`...")
        ok, reply = start_codespace(token, name, user_id)
        if not ok:
            safe_edit(call.message, f"{UI['error']} {reply}")
            bot.answer_callback_query(call.id)
            return
        
        db.log_action(user_id, "start", name)
        ok2, status = wait_for_state(token, name, "Running", user_id)
        safe_edit(call.message, f"{UI['success']} `{name}` is now Running!\n{status}")
        bot.answer_callback_query(call.id, f"Started {name}!", show_alert=True)
        return
    
    if data.startswith("stop_"):
        name = data[5:]
        token = db.get_token(user_id)
        if not token:
            safe_send(chat_id, f"{UI['error']} Token not set.")
            bot.answer_callback_query(call.id)
            return
        
        safe_edit(call.message, f"{UI['loading']} Stopping `{name}`...")
        ok, reply = stop_codespace(token, name, user_id)
        if not ok:
            safe_edit(call.message, f"{UI['error']} {reply}")
            bot.answer_callback_query(call.id)
            return
        
        db.log_action(user_id, "stop", name)
        ok2, status = wait_for_state(token, name, "Stopped", user_id)
        safe_edit(call.message, f"{UI['success']} `{name}` is now Stopped!\n{status}")
        bot.answer_callback_query(call.id, f"Stopped {name}!", show_alert=True)
        return
    
    if data.startswith("refresh_"):
        name = data[8:]
        token = db.get_token(user_id)
        if not token:
            safe_send(chat_id, f"{UI['error']} Token not set.")
            bot.answer_callback_query(call.id)
            return
        
        codespaces = get_codespaces(token, user_id)
        if codespaces is None:
            safe_edit(call.message, f"{UI['error']} Failed to fetch codespaces.")
            bot.answer_callback_query(call.id)
            return
        
        cs = next((c for c in codespaces if c.get("name") == name), None)
        if not cs:
            safe_edit(call.message, f"{UI['error']} Codespace `{name}` not found.")
            bot.answer_callback_query(call.id)
            return
        
        state = cs.get("state", "Unknown")
        safe_edit(call.message, format_codespace(cs), parse_mode="Markdown", reply_markup=create_codespace_menu(name, state))
        bot.answer_callback_query(call.id, "Refreshed!")
        return
    
    # ===== FILE BROWSER =====
    if data.startswith("file|"):
        parts = data.split("|")
        if len(parts) != 4:
            bot.answer_callback_query(call.id, "Invalid")
            return
        
        _, owner, repo, file_path = parts
        token = db.get_token(user_id)
        if not token:
            safe_edit(call.message, f"{UI['error']} Token not set.")
            bot.answer_callback_query(call.id)
            return
        
        branch = get_default_branch(owner, repo, token)
        if not branch:
            safe_edit(call.message, f"{UI['error']} Could not determine branch.")
            bot.answer_callback_query(call.id)
            return
        
        safe_edit(call.message, f"{UI['loading']} Downloading `{file_path}`...")
        raw_data, _, err = get_file_content(owner, repo, file_path, branch, token)
        
        if err or raw_data is None:
            safe_edit(call.message, f"{UI['error']} {err or 'Download failed'}")
            bot.answer_callback_query(call.id)
            return
        
        try:
            file_obj = BytesIO(raw_data)
            file_obj.name = os.path.basename(file_path)
            bot.send_document(chat_id, file_obj, caption=f"📄 `{file_path}` ({len(raw_data)//1024}KB)")
            safe_edit(call.message, f"{UI['success']} File downloaded successfully!")
        except Exception as e:
            safe_edit(call.message, f"{UI['error']} Send error: {e}")
        
        bot.answer_callback_query(call.id)
        return
    
    if data.startswith("zip_repo|"):
        parts = data.split("|")
        if len(parts) != 3:
            bot.answer_callback_query(call.id, "Invalid")
            return
        
        _, owner, repo = parts
        token = db.get_token(user_id)
        if not token:
            safe_edit(call.message, f"{UI['error']} Token not set.")
            bot.answer_callback_query(call.id)
            return
        
        branch = get_default_branch(owner, repo, token)
        if not branch:
            safe_edit(call.message, f"{UI['error']} Could not determine branch.")
            bot.answer_callback_query(call.id)
            return
        
        safe_edit(call.message, f"{UI['loading']} Preparing ZIP for {owner}/{repo}...")
        zip_url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/zipball/{branch}"
        headers = github_headers(token)
        
        try:
            resp = requests.get(zip_url, headers=headers, stream=True, timeout=120)
            if resp.status_code == 200:
                zip_data = resp.content
                zip_obj = BytesIO(zip_data)
                zip_obj.name = f"{repo}-{branch}.zip"
                bot.send_document(chat_id, zip_obj, caption=f"📦 `{repo}` ({branch}) ZIP")
                safe_edit(call.message, f"{UI['success']} ZIP downloaded successfully!")
            else:
                safe_edit(call.message, f"{UI['error']} HTTP {resp.status_code}")
        except Exception as e:
            safe_edit(call.message, f"{UI['error']} {str(e)}")
        
        bot.answer_callback_query(call.id)
        return
    
    if data.startswith("pg|"):
        page = int(data.split("|")[1])
        ctx = get_user_context(user_id)
        if ctx:
            ctx["page"] = page
            safe_edit(call.message, f"📂 *{ctx.get('owner')}/{ctx.get('repo')}*", 
                      parse_mode="Markdown", reply_markup=create_file_browser_menu(ctx, page, 1))
        bot.answer_callback_query(call.id)
        return
    
    if data.startswith("dir|"):
        parts = data.split("|")
        if len(parts) != 4:
            bot.answer_callback_query(call.id, "Invalid")
            return
        
        _, owner, repo, path = parts
        ctx = get_user_context(user_id)
        
        if ctx:
            ctx["owner"] = owner
            ctx["repo"] = repo
            ctx["path"] = path
            ctx["page"] = 0
            safe_edit(call.message, f"📂 *{owner}/{repo}* / `{path or '/'}`", 
                      parse_mode="Markdown", reply_markup=create_file_browser_menu(ctx, 0, 1))
        bot.answer_callback_query(call.id)
        return
    
    if data.startswith("refresh_tree|"):
        parts = data.split("|")
        if len(parts) != 3:
            bot.answer_callback_query(call.id, "Invalid")
            return
        
        _, owner, repo = parts
        token = db.get_token(user_id)
        if not token:
            safe_edit(call.message, f"{UI['error']} Token not set.")
            bot.answer_callback_query(call.id)
            return
        
        tree_cache.clear()
        safe_edit(call.message, f"{UI['loading']} Refreshing tree...")
        tree, branch, err = get_cached_tree(owner, repo, token)
        
        if err or tree is None:
            safe_edit(call.message, f"{UI['error']} {err or 'Failed to fetch'}")
            bot.answer_callback_query(call.id)
            return
        
        ctx = get_user_context(user_id)
        if ctx:
            ctx["tree"] = tree
            ctx["branch"] = branch
            safe_edit(call.message, f"📂 *{owner}/{repo}* (Refreshed)", 
                      parse_mode="Markdown", reply_markup=create_file_browser_menu(ctx, 0, 1))
        
        bot.answer_callback_query(call.id, "Tree refreshed!")
        return
    
    # Default response
    bot.answer_callback_query(call.id, "❓ Unknown action")

# ======================== MAIN ========================
if __name__ == "__main__":
    logger.info("🚀 Ultimate GitHub Bot v5.0 started polling...")
    logger.info("🌛 Dark UI Edition loaded successfully!")
    
    try:
        bot.infinity_polling()
    except KeyboardInterrupt:
        logger.info("Bot stopped.")
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
        sys.exit(1)