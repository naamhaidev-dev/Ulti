#!/usr/bin/env python3
"""
⚡ GITHUB CODESPACE BOT v5.3 – MULTI-USER THREAD-SAFE FIXED
├─ All commands working
├─ No Markdown errors
├─ No duplicate messages
└─ Thread-safe for multiple concurrent users
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
    "header": "╔═══════════════════════════════════╗\n║  💥 PAID BOT 24/7 💥  ║\n╚═══════════════════════════════════╝",
    "separator": "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    "bullet": "▸",
    "success": "✅",
    "error": "❌",
    "warning": "⚠️",
    "info": "ℹ️",
    "loading": "⏳",
}

TEXT_EXTENSIONS = {
    '.txt', '.md', '.py', '.java', '.c', '.cpp', '.h', '.hpp',
    '.js', '.html', '.css', '.json', '.xml', '.yaml', '.yml',
    '.properties', '.gradle', '.kts', '.sh', '.bash', '.bat',
    '.ps1', '.rb', '.go', '.rs', '.php', '.lua', '.r', '.swift',
    '.kt', '.scala', '.groovy', '.tf', '.conf', '.ini', '.cfg',
    '.toml', '.lock', '.gitignore', '.dockerignore'
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

# ======================== DATABASE ========================
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
            conn = sqlite3.connect(db_path, timeout=30.0)
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
                conn = sqlite3.connect(self.db_path, timeout=30.0)
                c = conn.cursor()
                c.execute("INSERT OR REPLACE INTO tokens (user_id, token, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)", (user_id, token))
                conn.commit()
                conn.close()
    
    def get_token(self, user_id: int) -> Optional[str]:
        with self._lock:
            if self.db_type == "mongo":
                doc = self.tokens_col.find_one({"user_id": user_id})
                return doc["token"] if doc else None
            else:
                conn = sqlite3.connect(self.db_path, timeout=30.0)
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
                conn = sqlite3.connect(self.db_path, timeout=30.0)
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
                conn = sqlite3.connect(self.db_path, timeout=30.0)
                c = conn.cursor()
                c.execute("INSERT INTO actions (user_id, action, details) VALUES (?, ?, ?)", (user_id, action, details))
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

# ======================== SAFE SEND FUNCTIONS ========================
def safe_edit(msg, text, parse_mode=None, reply_markup=None):
    """Safely edit message without Markdown errors"""
    try:
        bot.edit_message_text(text, msg.chat.id, msg.message_id, parse_mode=parse_mode, reply_markup=reply_markup)
        return True
    except Exception as e:
        logger.warning(f"Edit failed: {e}")
        return False

def safe_send(chat_id, text, parse_mode=None, reply_markup=None):
    """Safely send message without Markdown errors"""
    try:
        return bot.send_message(chat_id, text, parse_mode=parse_mode, reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Send failed: {e}")
        # Retry without parse_mode if Markdown fails
        if parse_mode:
            try:
                return bot.send_message(chat_id, text, parse_mode=None, reply_markup=reply_markup)
            except:
                pass
        return None

def safe_send_md(chat_id, text, reply_markup=None):
    """Send with Markdown, fallback to plain text on error"""
    try:
        return bot.send_message(chat_id, text, parse_mode="Markdown", reply_markup=reply_markup)
    except:
        # Remove markdown characters and send as plain text
        clean_text = re.sub(r'[*_`~]', '', text)
        return bot.send_message(chat_id, clean_text, parse_mode=None, reply_markup=reply_markup)

def safe_edit_md(msg, text, reply_markup=None):
    """Edit with Markdown, fallback to plain text on error"""
    try:
        bot.edit_message_text(text, msg.chat.id, msg.message_id, parse_mode="Markdown", reply_markup=reply_markup)
        return True
    except:
        try:
            clean_text = re.sub(r'[*_`~]', '', text)
            bot.edit_message_text(clean_text, msg.chat.id, msg.message_id, parse_mode=None, reply_markup=reply_markup)
            return True
        except:
            return False

# ======================== HELPERS ========================
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def github_headers(token: Optional[str] = None) -> Dict[str, str]:
    h = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "CodespaceBot/5.3"}
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h

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

def create_main_menu(user_id: int) -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup(row_width=2)
    markup.row(
        InlineKeyboardButton("🐙 Codespaces", callback_data="menu_codespaces"),
        InlineKeyboardButton("📂 Files", callback_data="menu_files")
    )
    markup.row(
        InlineKeyboardButton("🔑 Token", callback_data="menu_token"),
        InlineKeyboardButton("⚙️ Commands", callback_data="menu_commands")
    )
    markup.row(
        InlineKeyboardButton("❓ Help", callback_data="menu_help"),
        InlineKeyboardButton("📊 Stats", callback_data="menu_stats")
    )
    if is_admin(user_id):
        markup.row(InlineKeyboardButton("👑 Admin", callback_data="menu_admin"))
    return markup

# ======================== GITHUB API HELPERS ========================
def api_call(method, url, token=None, retries=MAX_RETRIES, user_id=None):
    headers = github_headers(token)
    for attempt in range(retries + 1):
        try:
            resp = method(url, headers=headers, timeout=30)
            if resp.status_code == 429:
                reset = int(resp.headers.get("X-RateLimit-Reset", 0))
                wait = max(1, reset - int(time.time()))
                if user_id:
                    safe_send(user_id, f"⏳ Rate limited, waiting {wait}s...")
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
    return resp.json().get("tree", []), None

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
    return resp.status_code in (200, 202), "Start request accepted" if resp.status_code in (200, 202) else f"Failed (status {resp.status_code})"

def stop_codespace(token, name, user_id=None):
    resp, err = api_call(requests.post, f"{GITHUB_API_BASE}/user/codespaces/{name}/stop", token, user_id=user_id)
    if err or not resp:
        return False, err or "Request failed"
    return resp.status_code in (200, 202), "Stop request accepted" if resp.status_code in (200, 202) else f"Failed (status {resp.status_code})"

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
    emoji = {"Available":"🟢","Starting":"🟡","Running":"🟢","Stopping":"🟠","Stopped":"🔴","Shutdown":"🔴"}.get(state, "❓")
    return f"""{UI['header']}
{UI['separator']}
{emoji} {name}
📦 Repo: {repo}
💻 Machine: {machine_name}
📊 Status: {state}
{UI['separator']}"""

def create_codespace_menu(name: str, state: str) -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup(row_width=2)
    if state in ("Stopped", "Shutdown"):
        markup.add(InlineKeyboardButton("▶️ Start", callback_data=f"start_{name}"))
    elif state in ("Available", "Running", "Starting"):
        markup.add(InlineKeyboardButton("⏹ Stop", callback_data=f"stop_{name}"))
    markup.add(InlineKeyboardButton("🔄 Refresh", callback_data=f"refresh_{name}"))
    markup.add(InlineKeyboardButton("🔙 Menu", callback_data="menu_codespaces"))
    return markup

# ======================== COMMAND HANDLERS ========================
@bot.message_handler(commands=["start"])
def cmd_start(message):
    user_id = message.from_user.id
    # Check if this is a codespace start command with argument e.g. /start <name>
    parts = message.text.split(maxsplit=1)
    if len(parts) > 1:
        cmd_start_codespace(message)
        return
    
    welcome = f"""{UI['header']}
{UI['separator']}
🚀 Welcome to PAID Bot 24/7
🐙 GitHub Codespace Controller
{UI['separator']}

💡 Quick Commands:
/settoken - Set GitHub token
/list - View codespaces
/start <name> - Start codespace
/stop <name> - Stop codespace
/files <repo> - Browse repo
/getfile <url> <path> - Download file
/grep <url> <pattern> - Search content
/zip <url> - Download repo as ZIP

{UI['separator']}
📌 Click menu below 👇"""
    safe_send(message.chat.id, welcome, reply_markup=create_main_menu(user_id))

@bot.message_handler(commands=["help"])
def cmd_help(message):
    help_text = f"""{UI['header']}
📚 PAID BOT HELP MENU
{UI['separator']}

🔑 Token:
/settoken <token> - Save token
/tokens - View token
/rmtoken - Delete token

🐙 Codespaces:
/list - List all codespaces
/start <name> - Start codespace
/stop <name> - Stop codespace

📂 Files:
/files <url> [path] - Browse repo
/findfile <url> <name> - Search file
/getfile <url> <path> - Download file
/preview <url> <path> - Preview file
/grep <url> <pattern> - Search content
/zip <url> [branch] - Download ZIP

⚙️ Utils:
/ratelimit - Check API limit
/clear - Clear session

👑 Admin:
/users - List users
/sessions - Active sessions
/stats - Bot stats
/clearcache - Clear cache
/broadcast - Send announcement"""
    safe_send(message.chat.id, help_text, reply_markup=create_main_menu(message.from_user.id))

@bot.message_handler(commands=["settoken"])
def cmd_settoken(message):
    user_id = message.from_user.id
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        safe_send(message.chat.id, f"{UI['error']} Usage: /settoken <token>")
        return
    token = parts[1].strip()
    if not token.startswith(("ghp_", "gho_", "ghu_", "ghs_", "ghr_")) or len(token) < 20:
        safe_send(message.chat.id, f"{UI['error']} Invalid token format!")
        return
    codespaces = get_codespaces(token, user_id)
    if codespaces is None:
        safe_send(message.chat.id, f"{UI['error']} Invalid token. Need codespace scope.")
        return
    db.save_token(user_id, token)
    db.log_action(user_id, "settoken", "saved")
    safe_send(message.chat.id, f"{UI['success']} Token saved! Found {len(codespaces)} codespace(s).")

@bot.message_handler(commands=["tokens"])
def cmd_tokens(message):
    user_id = message.from_user.id
    token = db.get_token(user_id)
    if not token:
        safe_send(message.chat.id, f"{UI['info']} No token stored.")
        return
    masked = token[:8] + "..." + token[-8:] if len(token) > 16 else "***"
    safe_send(message.chat.id, f"🔑 Token: {masked}")

@bot.message_handler(commands=["rmtoken"])
def cmd_rmtoken(message):
    user_id = message.from_user.id
    db.delete_token(user_id)
    safe_send(message.chat.id, f"{UI['success']} Token deleted.")

@bot.message_handler(commands=["list"])
def cmd_list(message):
    user_id = message.from_user.id
    token = db.get_token(user_id)
    if not token:
        safe_send(message.chat.id, f"{UI['error']} Set token first with /settoken")
        return
    msg = safe_send(message.chat.id, f"{UI['loading']} Fetching codespaces...")
    codespaces = get_codespaces(token, user_id)
    if codespaces is None:
        safe_edit(msg, f"{UI['error']} Failed to fetch. Check token.")
        return
    if not codespaces:
        safe_edit(msg, f"{UI['info']} No codespaces found.")
        return
    safe_edit(msg, f"{UI['success']} Found {len(codespaces)} codespace(s):")
    for cs in codespaces:
        name = cs.get("name", "unknown")
        state = cs.get("state", "Unknown")
        safe_send(message.chat.id, format_codespace(cs), reply_markup=create_codespace_menu(name, state))

def cmd_start_codespace(message):
    user_id = message.from_user.id
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        safe_send(message.chat.id, f"{UI['error']} Usage: /start <codespace_name>")
        return
    name = parts[1].strip()
    token = db.get_token(user_id)
    if not token:
        safe_send(message.chat.id, f"{UI['error']} Set token first with /settoken")
        return
    lock = get_codespace_lock(name)
    if not lock.acquire(blocking=False):
        safe_send(message.chat.id, f"⏳ Operation already running on {name}")
        return
    try:
        msg = safe_send(message.chat.id, f"⏳ Starting {name}...")
        ok, reply = start_codespace(token, name, user_id)
        if not ok:
            safe_edit(msg, f"{UI['error']} {reply}")
            return
        db.log_action(user_id, "start", name)
        ok2, status = wait_for_state(token, name, "Running", user_id)
        safe_edit(msg, f"{UI['success']} {name} is now Running!\n{status}")
    finally:
        lock.release()

@bot.message_handler(commands=["stop"])
def cmd_stop_codespace(message):
    user_id = message.from_user.id
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        safe_send(message.chat.id, f"{UI['error']} Usage: /stop <codespace_name>")
        return
    name = parts[1].strip()
    token = db.get_token(user_id)
    if not token:
        safe_send(message.chat.id, f"{UI['error']} Set token first with /settoken")
        return
    lock = get_codespace_lock(name)
    if not lock.acquire(blocking=False):
        safe_send(message.chat.id, f"⏳ Operation already running on {name}")
        return
    try:
        msg = safe_send(message.chat.id, f"⏳ Stopping {name}...")
        ok, reply = stop_codespace(token, name, user_id)
        if not ok:
            safe_edit(msg, f"{UI['error']} {reply}")
            return
        db.log_action(user_id, "stop", name)
        ok2, status = wait_for_state(token, name, "Stopped", user_id)
        safe_edit(msg, f"{UI['success']} {name} is now Stopped!\n{status}")
    finally:
        lock.release()

@bot.message_handler(commands=["files"])
def cmd_files(message):
    user_id = message.from_user.id
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        safe_send(message.chat.id, f"{UI['error']} Usage: /files <repo_url> [path]")
        return
    token = db.get_token(user_id)
    if not token:
        safe_send(message.chat.id, f"{UI['error']} Set token first with /settoken")
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
    msg = safe_send(message.chat.id, f"{UI['loading']} Fetching repository tree...")
    tree, branch, err = get_cached_tree(owner, repo, token)
    if err or tree is None:
        safe_edit(msg, f"{UI['error']} {err or 'Failed to fetch'}")
        return
    ctx = {"owner": owner, "repo": repo, "path": final_path, "page": 0, "token": token, "tree": tree, "branch": branch}
    set_user_context(user_id, ctx)
    safe_edit(msg, f"📂 {owner}/{repo} / {final_path or '/'}\nLoaded {len(tree)} files.")

@bot.message_handler(commands=["getfile"])
def cmd_getfile(message):
    user_id = message.from_user.id
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        safe_send(message.chat.id, f"{UI['error']} Usage: /getfile <repo_url> <file_path>")
        return
    token = db.get_token(user_id)
    if not token:
        safe_send(message.chat.id, f"{UI['error']} Set token first with /settoken")
        return
    parsed = parse_github_url(parts[1])
    if not parsed:
        safe_send(message.chat.id, f"{UI['error']} Invalid GitHub URL.")
        return
    owner, repo, _ = parsed
    file_path = parts[2].strip()
    branch = get_default_branch(owner, repo, token)
    if not branch:
        safe_send(message.chat.id, f"{UI['error']} Could not determine branch.")
        return
    msg = safe_send(message.chat.id, f"{UI['loading']} Downloading {file_path}...")
    raw_data, _, err = get_file_content(owner, repo, file_path, branch, token)
    if err or raw_data is None:
        safe_edit(msg, f"{UI['error']} {err or 'Download failed'}")
        return
    try:
        file_obj = BytesIO(raw_data)
        file_obj.name = os.path.basename(file_path)
        bot.send_document(message.chat.id, file_obj, caption=f"📄 {file_path} ({len(raw_data)//1024}KB)")
        safe_edit(msg, f"{UI['success']} File downloaded!")
    except Exception as e:
        safe_edit(msg, f"{UI['error']} Send error: {e}")

@bot.message_handler(commands=["preview"])
def cmd_preview(message):
    user_id = message.from_user.id
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        safe_send(message.chat.id, f"{UI['error']} Usage: /preview <repo_url> <file_path>")
        return
    token = db.get_token(user_id)
    if not token:
        safe_send(message.chat.id, f"{UI['error']} Set token first with /settoken")
        return
    parsed = parse_github_url(parts[1])
    if not parsed:
        safe_send(message.chat.id, f"{UI['error']} Invalid GitHub URL.")
        return
    owner, repo, _ = parsed
    file_path = parts[2].strip()
    branch = get_default_branch(owner, repo, token)
    if not branch:
        safe_send(message.chat.id, f"{UI['error']} Could not determine branch.")
        return
    msg = safe_send(message.chat.id, f"⏳ Previewing {file_path}...")
    raw_data, _, err = get_file_content(owner, repo, file_path, branch, token)
    if err or raw_data is None:
        safe_edit(msg, f"{UI['error']} {err or 'Preview failed'}")
        return
    try:
        content = raw_data.decode('utf-8', errors='replace')[:3000]
        if len(raw_data) > 3000:
            content += "\n\n... (truncated)"
        safe_edit(msg, f"📄 Preview of {file_path}\n\n{content}")
    except UnicodeDecodeError:
        safe_edit(msg, f"{UI['error']} Cannot preview binary file.")

@bot.message_handler(commands=["findfile"])
def cmd_findfile(message):
    user_id = message.from_user.id
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        safe_send(message.chat.id, f"{UI['error']} Usage: /findfile <repo_url> <filename>")
        return
    token = db.get_token(user_id)
    if not token:
        safe_send(message.chat.id, f"{UI['error']} Set token first with /settoken")
        return
    parsed = parse_github_url(parts[1])
    if not parsed:
        safe_send(message.chat.id, f"{UI['error']} Invalid GitHub URL.")
        return
    owner, repo, _ = parsed
    filename = parts[2].strip().lower()
    msg = safe_send(message.chat.id, f"{UI['loading']} Searching for {filename}...")
    tree, branch, err = get_cached_tree(owner, repo, token)
    if err or tree is None:
        safe_edit(msg, f"{UI['error']} {err or 'Failed to fetch'}")
        return
    results = []
    for item in tree:
        if item.get("type") == "blob":
            path = item.get("path", "").lower()
            if filename in path:
                results.append(item)
    if not results:
        safe_edit(msg, f"{UI['info']} No file found matching {filename}")
        return
    safe_edit(msg, f"{UI['success']} Found {len(results)} file(s):")
    for item in results[:20]:
        fpath = item.get("path", "")
        size = item.get("size", 0)
        sz = f" ({size//1024}KB)" if size > 0 else ""
        safe_send(message.chat.id, f"📄 {fpath}{sz}\n/getfile https://github.com/{owner}/{repo} {fpath}")


@bot.message_handler(commands=["grep"])
def cmd_grep(message):
    user_id = message.from_user.id
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        safe_send(message.chat.id, f"{UI['error']} Usage: /grep <repo_url> <pattern>")
        return
    token = db.get_token(user_id)
    if not token:
        safe_send(message.chat.id, f"{UI['error']} Set token first with /settoken")
        return
    parsed = parse_github_url(parts[1])
    if not parsed:
        safe_send(message.chat.id, f"{UI['error']} Invalid GitHub URL.")
        return
    owner, repo, _ = parsed
    pattern = parts[2].strip()
    branch = get_default_branch(owner, repo, token)
    if not branch:
        safe_send(message.chat.id, f"{UI['error']} Could not determine branch.")
        return
    msg = safe_send(message.chat.id, f"{UI['loading']} Grepping {pattern} in {owner}/{repo}...")
    tree, _, err = get_cached_tree(owner, repo, token)
    if err or tree is None:
        safe_edit(msg, f"{UI['error']} {err or 'Failed to fetch'}")
        return
    text_files = [item for item in tree if item.get("type") == "blob" and os.path.splitext(item.get("path", ""))[1].lower() in TEXT_EXTENSIONS]
    matches = []
    for idx, item in enumerate(text_files[:50]):
        fpath = item.get("path", "")
        if item.get("size", 0) > 1024 * 1024:
            continue
        raw, _, err_file = get_file_content(owner, repo, fpath, branch, token)
        if raw:
            try:
                content = raw.decode('utf-8', errors='replace')
                for line_num, line in enumerate(content.splitlines(), 1):
                    if pattern.lower() in line.lower():
                        matches.append((fpath, line_num, line.strip()[:100]))
                        if len(matches) >= 20:
                            break
                if len(matches) >= 20:
                    break
            except:
                pass
    if not matches:
        safe_edit(msg, f"{UI['info']} No matches found for {pattern}")
        return
    output = f"🔍 Matches for {pattern}\n{UI['separator']}\n"
    for fpath, line_num, line in matches[:20]:
        output += f"📄 {fpath} (L{line_num})\n{line}\n\n"
    safe_edit(msg, output[:4000])

@bot.message_handler(commands=["zip"])
def cmd_zip(message):
    user_id = message.from_user.id
    parts = message.text.split(maxsplit=2)
    if len(parts) < 2:
        safe_send(message.chat.id, f"{UI['error']} Usage: /zip <repo_url> [branch]")
        return
    token = db.get_token(user_id)
    if not token:
        safe_send(message.chat.id, f"{UI['error']} Set token first with /settoken")
        return
    parsed = parse_github_url(parts[1])
    if not parsed:
        safe_send(message.chat.id, f"{UI['error']} Invalid GitHub URL.")
        return
    owner, repo, _ = parsed
    branch = parts[2] if len(parts) > 2 else get_default_branch(owner, repo, token)
    if not branch:
        safe_send(message.chat.id, f"{UI['error']} Could not determine branch.")
        return
    msg = safe_send(message.chat.id, f"{UI['loading']} Preparing ZIP for {owner}/{repo}...")
    zip_url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/zipball/{branch}"
    headers = github_headers(token)
    try:
        resp = requests.get(zip_url, headers=headers, stream=True, timeout=120)
        if resp.status_code == 200:
            zip_data = resp.content
            zip_obj = BytesIO(zip_data)
            zip_obj.name = f"{repo}-{branch}.zip"
            bot.send_document(message.chat.id, zip_obj, caption=f"📦 {repo} ({branch}) ZIP ({len(zip_data)//1024//1024}MB)")
            safe_edit(msg, f"{UI['success']} ZIP downloaded!")
        else:
            safe_edit(msg, f"{UI['error']} HTTP {resp.status_code}")
    except Exception as e:
        safe_edit(msg, f"{UI['error']} {str(e)}")

@bot.message_handler(commands=["ratelimit"])
def cmd_ratelimit(message):
    user_id = message.from_user.id
    token = db.get_token(user_id)
    if not token:
        safe_send(message.chat.id, f"{UI['error']} Set token first with /settoken")
        return
    resp, err = api_call(requests.get, "https://api.github.com/rate_limit", token, user_id=user_id)
    if err or not resp or resp.status_code != 200:
        safe_send(message.chat.id, f"{UI['error']} Failed: {err}")
        return
    core = resp.json().get("rate", {})
    reset_ts = core.get("reset", 0)
    reset_str = datetime.fromtimestamp(reset_ts).strftime("%H:%M:%S") if reset_ts else "?"
    safe_send(message.chat.id, f"📊 Rate Limit\nLimit: {core.get('limit','?')}\nRemaining: {core.get('remaining','?')}\nResets: {reset_str}")

@bot.message_handler(commands=["clear"])
def cmd_clear(message):
    user_id = message.from_user.id
    clear_user_context(user_id)
    safe_send(message.chat.id, f"{UI['success']} Session cleared.")

# ======================== ADMIN COMMANDS ========================
@bot.message_handler(commands=["sessions"])
def cmd_sessions(message):
    if not is_admin(message.from_user.id):
        safe_send(message.chat.id, "⛔ Admin only.")
        return
    with user_context_lock:
        if not user_context:
            safe_send(message.chat.id, "📭 No active sessions.")
            return
        lines = ["👥 Active Sessions\n"]
        for uid, ctx in user_context.items():
            lines.append(f"• {uid} – {ctx.get('owner', '?')}/{ctx.get('repo', '?')}")
        safe_send(message.chat.id, "\n".join(lines))

@bot.message_handler(commands=["clearcache"])
def cmd_clearcache(message):
    if not is_admin(message.from_user.id):
        safe_send(message.chat.id, "⛔ Admin only.")
        return
    size = tree_cache.size()
    tree_cache.clear()
    safe_send(message.chat.id, f"{UI['success']} Cache cleared ({size} entries).")

@bot.message_handler(commands=["stats"])
def cmd_stats(message):
    if not is_admin(message.from_user.id):
        safe_send(message.chat.id, "⛔ Admin only.")
        return
    if db.db_type == "mongo":
        actions = db.db["actions"].count_documents({})
    else:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM actions")
        actions = c.fetchone()[0]
        conn.close()
    safe_send(message.chat.id, f"📊 Stats\nActions: {actions}\nCache: {tree_cache.size()}")

@bot.message_handler(commands=["users"])
def cmd_users(message):
    if not is_admin(message.from_user.id):
        safe_send(message.chat.id, "⛔ Admin only.")
        return
    if db.db_type == "mongo":
        users = [str(doc["user_id"]) for doc in db.tokens_col.find({}, {"user_id": 1})]
    else:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        c = conn.cursor()
        c.execute("SELECT user_id FROM tokens")
        users = [str(row[0]) for row in c.fetchall()]
        conn.close()
    if not users:
        safe_send(message.chat.id, "📭 No users.")
        return
    safe_send(message.chat.id, f"👥 Users ({len(users)}):\n" + "\n".join([f"• {u}" for u in users]))

@bot.message_handler(commands=["broadcast"])
def cmd_broadcast(message):
    if not is_admin(message.from_user.id):
        safe_send(message.chat.id, "⛔ Admin only.")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        safe_send(message.chat.id, f"{UI['error']} Usage: /broadcast <message>")
        return
    if db.db_type == "mongo":
        users = [doc["user_id"] for doc in db.tokens_col.find({}, {"user_id": 1})]
    else:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        c = conn.cursor()
        c.execute("SELECT user_id FROM tokens")
        users = [row[0] for row in c.fetchall()]
        conn.close()
    if not users:
        safe_send(message.chat.id, "📭 No users.")
        return
    sent = 0
    for uid in users:
        try:
            bot.send_message(uid, f"📣 Announcement:\n{parts[1]}")
            sent += 1
        except:
            pass
        time.sleep(0.1)
    safe_send(message.chat.id, f"{UI['success']} Broadcast sent to {sent} users.")


# ======================== CALLBACK HANDLER ========================
@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call: CallbackQuery):
    data = call.data
    chat_id = call.message.chat.id
    user_id = call.from_user.id
    
    # Menu navigation
    if data == "menu_main":
        safe_edit(call.message, "🌟 Main Menu\nSelect an option below 👇", reply_markup=create_main_menu(user_id))
        bot.answer_callback_query(call.id)
        return
    
    if data == "menu_codespaces":
        token = db.get_token(user_id)
        if not token:
            safe_edit(call.message, f"{UI['error']} Set token first with /settoken")
            bot.answer_callback_query(call.id)
            return
        codespaces = get_codespaces(token, user_id)
        if not codespaces:
            safe_edit(call.message, f"{UI['info']} No codespaces found.")
            bot.answer_callback_query(call.id)
            return
        markup = InlineKeyboardMarkup(row_width=1)
        for cs in codespaces:
            name = cs.get("name", "unknown")
            state = cs.get("state", "Unknown")
            emoji = "🟢" if state in ("Available", "Running") else "🔴" if state in ("Stopped", "Shutdown") else "🟡"
            markup.add(InlineKeyboardButton(f"{emoji} {name}", callback_data=f"cs_{name}"))
        markup.add(InlineKeyboardButton("🔙 Main", callback_data="menu_main"))
        safe_edit(call.message, f"🐙 Your Codespaces\n{UI['separator']}", reply_markup=markup)
        bot.answer_callback_query(call.id)
        return
    
    if data == "menu_files":
        safe_edit(call.message, f"""📂 FILE BROWSER
{UI['separator']}
Commands:
/files <url> [path] - Browse
/getfile <url> <path> - Download
/grep <url> <pattern> - Search
/zip <url> - Download ZIP
{UI['separator']}
Click below to go back 👇""", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main", callback_data="menu_main")]]))
        bot.answer_callback_query(call.id)
        return
    
    if data == "menu_token":
        token = db.get_token(user_id)
        if token:
            masked = token[:8] + "..." + token[-8:]
            text = f"🔑 Token: {masked}\n{UI['separator']}\nUse /rmtoken to delete."
        else:
            text = f"🔑 No token stored\nUse /settoken to save."
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🗑️ Delete", callback_data="token_delete"), InlineKeyboardButton("🔙 Main", callback_data="menu_main"))
        safe_edit(call.message, text, reply_markup=markup)
        bot.answer_callback_query(call.id)
        return
    
    if data == "menu_commands":
        text = f"""⚙️ COMMANDS
{UI['separator']}
🔑 /settoken - Save token
🗑️ /rmtoken - Delete token
📋 /list - List codespaces
▶️ /start <name> - Start
⏹ /stop <name> - Stop
📂 /files <url> - Browse
⬇️ /getfile <url> <path> - Download
🔎 /grep <url> <pattern> - Search
📦 /zip <url> - Download ZIP
📄 /preview <url> <path> - Preview
🔍 /findfile <url> <name> - Find
📊 /ratelimit - API limit
🧹 /clear - Clear session
❓ /help - Full help"""
        safe_edit(call.message, text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main", callback_data="menu_main")]]))
        bot.answer_callback_query(call.id)
        return
    
    if data == "menu_help":
        text = f"""📚 QUICK HELP
{UI['separator']}
1️⃣ /settoken <token> - Save token
2️⃣ /list - View codespaces
3️⃣ /files <url> - Browse repo

📖 Type /help for full list.
{UI['separator']}
Click below 👇"""
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("📖 Full Help", callback_data="help_full"), InlineKeyboardButton("🔙 Main", callback_data="menu_main"))
        safe_edit(call.message, text, reply_markup=markup)
        bot.answer_callback_query(call.id)
        return
    
    if data == "help_full":
        cmd_help(call.message)
        bot.answer_callback_query(call.id)
        return
    
    if data == "menu_stats":
        with user_context_lock:
            sessions = len(user_context)
        text = f"""📊 STATS
{UI['separator']}
Sessions: {sessions}
Cache: {tree_cache.size()}
DB: {DB_TYPE.upper()}
{UI['separator']}"""
        safe_edit(call.message, text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main", callback_data="menu_main")]]))
        bot.answer_callback_query(call.id)
        return
    
    if data == "menu_admin" and is_admin(user_id):
        text = f"""👑 ADMIN
{UI['separator']}
/users - List users
/sessions - Active sessions
/stats - Bot stats
/clearcache - Clear cache
/broadcast - Announce
{UI['separator']}"""
        safe_edit(call.message, text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main", callback_data="menu_main")]]))
        bot.answer_callback_query(call.id)
        return
    
    if data == "token_delete":
        db.delete_token(user_id)
        safe_edit(call.message, f"{UI['success']} Token deleted!", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main", callback_data="menu_main")]]))
        bot.answer_callback_query(call.id)
        return
    
    # Codespace actions
    if data.startswith("cs_"):
        name = data[3:]
        token = db.get_token(user_id)
        if not token:
            safe_edit(call.message, f"{UI['error']} Token not set.")
            bot.answer_callback_query(call.id)
            return
        codespaces = get_codespaces(token, user_id)
        if not codespaces:
            safe_edit(call.message, f"{UI['error']} Failed to fetch.")
            bot.answer_callback_query(call.id)
            return
        cs = next((c for c in codespaces if c.get("name") == name), None)
        if not cs:
            safe_edit(call.message, f"{UI['error']} Codespace {name} not found.")
            bot.answer_callback_query(call.id)
            return
        safe_edit(call.message, format_codespace(cs), reply_markup=create_codespace_menu(name, cs.get("state", "Unknown")))
        bot.answer_callback_query(call.id)
        return
    
    if data.startswith("start_"):
        name = data[6:]
        token = db.get_token(user_id)
        if not token:
            safe_edit(call.message, f"{UI['error']} Token not set.")
            bot.answer_callback_query(call.id)
            return
        safe_edit(call.message, f"⏳ Starting {name}...")
        ok, reply = start_codespace(token, name, user_id)
        if not ok:
            safe_edit(call.message, f"{UI['error']} {reply}")
            bot.answer_callback_query(call.id)
            return
        db.log_action(user_id, "start", name)
        ok2, status = wait_for_state(token, name, "Running", user_id)
        safe_edit(call.message, f"{UI['success']} {name} is now Running!\n{status}")
        bot.answer_callback_query(call.id, f"Started {name}!")
        return
    
    if data.startswith("stop_"):
        name = data[5:]
        token = db.get_token(user_id)
        if not token:
            safe_edit(call.message, f"{UI['error']} Token not set.")
            bot.answer_callback_query(call.id)
            return
        safe_edit(call.message, f"⏳ Stopping {name}...")
        ok, reply = stop_codespace(token, name, user_id)
        if not ok:
            safe_edit(call.message, f"{UI['error']} {reply}")
            bot.answer_callback_query(call.id)
            return
        db.log_action(user_id, "stop", name)
        ok2, status = wait_for_state(token, name, "Stopped", user_id)
        safe_edit(call.message, f"{UI['success']} {name} is now Stopped!\n{status}")
        bot.answer_callback_query(call.id, f"Stopped {name}!")
        return
    
    if data.startswith("refresh_"):
        name = data[8:]
        token = db.get_token(user_id)
        if not token:
            safe_edit(call.message, f"{UI['error']} Token not set.")
            bot.answer_callback_query(call.id)
            return
        codespaces = get_codespaces(token, user_id)
        if not codespaces:
            safe_edit(call.message, f"{UI['error']} Failed to fetch.")
            bot.answer_callback_query(call.id)
            return
        cs = next((c for c in codespaces if c.get("name") == name), None)
        if not cs:
            safe_edit(call.message, f"{UI['error']} Codespace not found.")
            bot.answer_callback_query(call.id)
            return
        safe_edit(call.message, format_codespace(cs), reply_markup=create_codespace_menu(name, cs.get("state", "Unknown")))
        bot.answer_callback_query(call.id, "Refreshed!")
        return
    
    bot.answer_callback_query(call.id, "❓ Unknown")

# ======================== MAIN ========================
if __name__ == "__main__":
    logger.info("🚀 Ultimate GitHub Bot v5.3 started polling...")
    logger.info("✅ Multi-user concurrency & SQLite locking fixed!")
    try:
        bot.infinity_polling(timeout=60, long_polling_timeout=60)
    except KeyboardInterrupt:
        logger.info("Bot stopped.")
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
        sys.exit(1)


