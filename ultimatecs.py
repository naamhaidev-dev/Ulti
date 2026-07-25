#!/usr/bin/env python3
"""
GitHub Codespace + File Browser Bot — ULTIMATE
All features integrated: progress, rate-limit warnings, session reset,
download progress, admin sessions, clear cache, better errors, grep, zip.
"""

import os
import sys
import time
import re
import sqlite3
import logging
import threading
import base64
import zipfile
import io
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
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB
PAGE_SIZE = 15
CACHE_TTL = 300  # 5 minutes
DOWNLOAD_CHUNK_SIZE = 1024 * 1024  # 1MB chunks
TEXT_EXTENSIONS = {
    '.txt', '.md', '.py', '.java', '.c', '.cpp', '.h', '.hpp',
    '.js', '.html', '.css', '.json', '.xml', '.yaml', '.yml',
    '.properties', '.gradle', '.kts', '.sh', '.bash', '.bat',
    '.ps1', '.rb', '.go', '.rs', '.php', '.lua', '.r', '.swift',
    '.kt', '.scala', '.groovy', '.tf', '.conf', '.ini', '.cfg',
    '.toml', '.lock', '.gitignore', '.dockerignore'
}

# ======================== LOGGING ========================
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ======================== DATABASE ========================
class TokenDB:
    def __init__(self, db_type: str = "sqlite", **kwargs):
        self.db_type = db_type.lower()
        self._lock = threading.Lock()
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
            c.execute("CREATE TABLE IF NOT EXISTS tokens (user_id INTEGER PRIMARY KEY, token TEXT NOT NULL, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
            c.execute("CREATE TABLE IF NOT EXISTS actions (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, action TEXT, details TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)")
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
        if self.db_type == "mongo":
            self.tokens_col.update_one({"user_id": user_id}, {"$set": {"token": token, "updated_at": datetime.utcnow()}}, upsert=True)
        else:
            with self._lock:
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                c = conn.cursor()
                c.execute("INSERT OR REPLACE INTO tokens (user_id, token, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)", (user_id, token))
                conn.commit()
                conn.close()
    
    def get_token(self, user_id: int) -> Optional[str]:
        if self.db_type == "mongo":
            doc = self.tokens_col.find_one({"user_id": user_id})
            return doc["token"] if doc else None
        else:
            with self._lock:
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                c = conn.cursor()
                c.execute("SELECT token FROM tokens WHERE user_id = ?", (user_id,))
                row = c.fetchone()
                conn.close()
                return row[0] if row else None
    
    def delete_token(self, user_id: int):
        if self.db_type == "mongo":
            self.tokens_col.delete_one({"user_id": user_id})
        else:
            with self._lock:
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                c = conn.cursor()
                c.execute("DELETE FROM tokens WHERE user_id = ?", (user_id,))
                conn.commit()
                conn.close()
    
    def log_action(self, user_id: int, action: str, details: str = ""):
        if self.db_type == "mongo":
            self.db["actions"].insert_one({"user_id": user_id, "action": action, "details": details, "timestamp": datetime.utcnow()})
        else:
            with self._lock:
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
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
    logger.error(f"DB init fail: {e}")
    sys.exit(1)

# ======================== BOT ========================
bot = TeleBot(BOT_TOKEN)

# ======================== LOCKS & STATE ========================
code_locks: Dict[str, threading.Lock] = {}
code_lock_lock = threading.Lock()
user_context: Dict[int, Dict] = {}
tree_cache: Dict[str, Tuple[List[Dict], str, float]] = {}
rate_limit_warnings: Dict[int, bool] = {}  # track if we already warned for low limit

def get_codespace_lock(name: str) -> threading.Lock:
    with code_lock_lock:
        if name not in code_locks:
            code_locks[name] = threading.Lock()
        return code_locks[name]

# ======================== HELPER FUNCTIONS ========================
def safe_edit(msg, text, parse_mode="Markdown", reply_markup=None):
    try:
        bot.edit_message_text(text, msg.chat.id, msg.message_id, parse_mode=parse_mode, reply_markup=reply_markup)
    except Exception:
        pass

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def github_headers(token: Optional[str] = None) -> Dict[str, str]:
    h = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "CodespaceBot/2.0"}
    if token: h["Authorization"] = f"Bearer {token}"
    return h

def user_friendly_error(err: str) -> str:
    """Map technical errors to user-friendly messages with actionable suggestions."""
    err_lower = err.lower()
    if "403" in err or "access denied" in err_lower:
        return "❌ Access denied. This might be a private repo. Make sure your token has the `repo` scope.\n\nUse: `/settoken <token_with_repo_scope>`"
    if "404" in err:
        return "❌ Not found. Check the repository name, branch, or file path."
    if "rate limit" in err_lower or "429" in err:
        return "⚠️ GitHub API rate limit exceeded. Please wait a few minutes and try again."
    if "timeout" in err_lower:
        return "⏰ Request timed out. The repository might be very large. Try again later."
    if "connection" in err_lower:
        return "🌐 Network error. Check your internet connection."
    if "branch" in err_lower and "resolve" in err_lower:
        return "❌ Could not determine the default branch. Make sure the repository exists and your token has access."
    if "scope" in err_lower:
        return "❌ Token missing required scope. Use a token with `repo` (for private repos) and `codespace` scopes."
    return f"❌ {err}"

# ======================== ADVANCED: GIT TREES API ========================
def get_default_branch(owner: str, repo: str, token: Optional[str] = None) -> Optional[str]:
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}"
    resp, err = api_call(requests.get, url, token, retries=1)
    if resp and resp.status_code == 200:
        return resp.json().get("default_branch", "main")
    return None

def get_repo_tree(owner: str, repo: str, branch: str, token: Optional[str] = None) -> Tuple[Optional[List[Dict]], Optional[str]]:
    # Get commit SHA
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/git/refs/heads/{branch}"
    resp, err = api_call(requests.get, url, token)
    if err:
        return None, f"Branch error: {err}"
    if not resp or resp.status_code != 200:
        return None, f"Could not find branch '{branch}'"
    commit_sha = resp.json().get("object", {}).get("sha")
    if not commit_sha:
        return None, "Could not get commit SHA"
    # Get tree SHA from commit
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/git/commits/{commit_sha}"
    resp, err = api_call(requests.get, url, token)
    if err or not resp or resp.status_code != 200:
        return None, "Could not get commit tree"
    tree_sha = resp.json().get("tree", {}).get("sha")
    if not tree_sha:
        return None, "Could not get tree SHA"
    # Get recursive tree
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/git/trees/{tree_sha}?recursive=1"
    resp, err = api_call(requests.get, url, token)
    if err:
        return None, f"Tree fetch error: {err}"
    if not resp or resp.status_code != 200:
        return None, f"Could not fetch tree (HTTP {resp.status_code if resp else 'None'})"
    data = resp.json()
    tree = data.get("tree", [])
    truncated = data.get("truncated", False)
    if truncated:
        logger.warning(f"Tree truncated for {owner}/{repo} (over 100k files)")
    return tree, None

def get_cached_tree(owner: str, repo: str, token: Optional[str] = None, progress_msg=None) -> Tuple[Optional[List[Dict]], Optional[str], Optional[str]]:
    cache_key = f"{owner}/{repo}"
    now = time.time()
    if cache_key in tree_cache:
        tree, branch, timestamp = tree_cache[cache_key]
        if now - timestamp < CACHE_TTL:
            return tree, branch, None
    branch = get_default_branch(owner, repo, token)
    if not branch:
        return None, None, "Could not determine default branch"
    # Show progress spinner (fake)
    if progress_msg:
        spinner = ["⏳", "⌛", "⏳", "⌛"]
        for i in range(8):  # 8 steps, 1 sec each
            try:
                bot.edit_message_text(f"{spinner[i%4]} Fetching repository tree... ({owner}/{repo})", progress_msg.chat.id, progress_msg.message_id)
            except:
                pass
            time.sleep(0.5)
    tree, err = get_repo_tree(owner, repo, branch, token)
    if err or tree is None:
        return None, None, err or "Failed to fetch tree"
    tree_cache[cache_key] = (tree, branch, now)
    if progress_msg:
        safe_edit(progress_msg, f"✅ Tree fetched for {owner}/{repo} ({len(tree)} files)")
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
        else:
            if item.get("type") == "blob":
                files.append(item)
    return files, sorted(dirs_set)

def search_files_by_pattern(tree: List[Dict], pattern: str) -> List[Dict]:
    pattern_lower = pattern.lower()
    results = []
    for item in tree:
        if item.get("type") == "blob":
            path = item.get("path", "").lower()
            if pattern_lower in path:
                results.append(item)
    return results

def get_file_content_direct(owner: str, repo: str, branch: str, path: str, token: Optional[str] = None, progress_msg=None) -> Tuple[Optional[bytes], Optional[str], Optional[str]]:
    encoded_path = quote(path, safe="")
    raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{encoded_path}"
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        # Stream with progress
        resp = requests.get(raw_url, headers=headers, timeout=60, stream=True)
        if resp.status_code == 200:
            total_size = int(resp.headers.get("Content-Length", 0))
            if total_size > MAX_FILE_SIZE:
                return None, None, f"File too large ({total_size//1024//1024}MB). Max: {MAX_FILE_SIZE//1024//1024}MB"
            # Download in chunks
            data = bytearray()
            downloaded = 0
            last_update = 0
            for chunk in resp.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                if chunk:
                    data.extend(chunk)
                    downloaded += len(chunk)
                    if progress_msg and total_size > 0:
                        percent = int((downloaded / total_size) * 100)
                        if percent % 5 == 0 and percent != last_update:
                            last_update = percent
                            try:
                                bot.edit_message_text(f"⬇️ Downloading... {percent}% ({downloaded//1024//1024}MB / {total_size//1024//1024}MB)", progress_msg.chat.id, progress_msg.message_id)
                            except:
                                pass
            raw = bytes(data)
            return raw, resp.headers.get("Content-Type", "text/plain"), None
        elif resp.status_code == 403:
            return None, None, "❌ Access denied. Private repo? Token needs `repo` scope."
        elif resp.status_code == 404:
            return None, None, f"❌ File `{path}` not found"
        else:
            return None, None, f"❌ HTTP {resp.status_code}"
    except requests.Timeout:
        return None, None, "Download timeout (file may be too large)"
    except Exception as e:
        return None, None, f"Download error: {e}"

def get_file_content(owner: str, repo: str, path: str, branch: str, token: Optional[str] = None, progress_msg=None) -> Tuple[Optional[bytes], Optional[str], Optional[str]]:
    # Try raw first with progress
    raw_data, content_type, err = get_file_content_direct(owner, repo, branch, path, token, progress_msg)
    if raw_data is not None:
        return raw_data, content_type, None
    # If 404, maybe file is in subfolder but path given without full? We already have full path.
    if "404" not in str(err):
        url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/contents/{path}"
        resp, err2 = api_call(requests.get, url, token)
        if resp and resp.status_code == 200:
            data = resp.json()
            if "content" in data:
                raw = base64.b64decode(data["content"])
                if len(raw) > MAX_FILE_SIZE:
                    return None, None, f"File too large ({len(raw)//1024}KB)"
                return raw, "text/plain", None
    return None, None, err

def parse_github_url(text: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not text.startswith("http"):
        text = "https://" + text
    parsed = urlparse(text)
    if parsed.netloc not in ("github.com", "www.github.com"):
        return None, None, None
    path = parsed.path.strip("/")
    parts = path.split("/")
    if len(parts) < 2:
        return None, None, None
    owner = parts[0]
    repo_name = parts[1].replace(".git", "")
    subpath = ""
    if len(parts) > 4 and parts[2] in ("tree", "blob", "raw"):
        subpath = "/".join(parts[4:])
    elif len(parts) > 2:
        subpath = "/".join(parts[2:])
    return owner, repo_name, subpath if subpath else None

# ======================== RATE LIMIT WARNING ========================
def check_rate_limit_and_warn(user_id: int, resp: requests.Response):
    """Check rate limit headers and warn user if remaining low."""
    if resp and resp.headers:
        remaining = resp.headers.get("X-RateLimit-Remaining")
        if remaining and remaining.isdigit():
            rem = int(remaining)
            if rem < 10 and not rate_limit_warnings.get(user_id, False):
                rate_limit_warnings[user_id] = True
                bot.send_message(user_id, f"⚠️ *GitHub API rate limit low!* Only {rem} requests remaining. Please avoid heavy usage.", parse_mode="Markdown")
            elif rem > 20:
                rate_limit_warnings[user_id] = False  # reset warning if enough left

# ======================== API CALL WITH RETRY & RATE LIMIT ========================
def api_call(method, url, token=None, retries=MAX_RETRIES, user_id=None):
    headers = github_headers(token)
    for attempt in range(retries + 1):
        try:
            resp = method(url, headers=headers, timeout=30)
            # Check rate limit and warn user
            if user_id and resp:
                check_rate_limit_and_warn(user_id, resp)
            if resp.status_code == 429:
                reset = int(resp.headers.get("X-RateLimit-Reset", 0))
                wait = max(1, reset - int(time.time()))
                logger.warning(f"Rate limited, waiting {wait}s")
                if user_id:
                    bot.send_message(user_id, f"⚠️ Rate limit hit! Waiting {wait}s...")
                time.sleep(wait + 2)
                continue
            if resp.status_code >= 500:
                if attempt < retries:
                    time.sleep(RETRY_BACKOFF ** (attempt + 1))
                    continue
                return None, f"GitHub server error (HTTP {resp.status_code})"
            return resp, None
        except requests.Timeout:
            if attempt < retries:
                time.sleep(RETRY_BACKOFF ** (attempt + 1))
                continue
            return None, "GitHub API timeout"
        except requests.ConnectionError as e:
            if attempt < retries:
                time.sleep(RETRY_BACKOFF ** (attempt + 1))
                continue
            return None, f"Connection error: {e}"
        except Exception as e:
            return None, f"Error: {e}"
    return None, "Max retries reached"

# ======================== CODESPACE HELPERS ========================
def get_codespaces(token, user_id=None):
    url = f"{GITHUB_API_BASE}/user/codespaces"
    resp, err = api_call(requests.get, url, token, user_id=user_id)
    if err: return None
    if resp and resp.status_code == 200: return resp.json().get("codespaces", [])
    return None

def start_codespace(token, name, user_id=None):
    resp, err = api_call(requests.post, f"{GITHUB_API_BASE}/user/codespaces/{name}/start", token, user_id=user_id)
    if err: return False, err
    if resp and resp.status_code in (200, 202): return True, "Start request accepted"
    return False, f"Failed (status {resp.status_code})"

def stop_codespace(token, name, user_id=None):
    resp, err = api_call(requests.post, f"{GITHUB_API_BASE}/user/codespaces/{name}/stop", token, user_id=user_id)
    if err: return False, err
    if resp and resp.status_code in (200, 202): return True, "Stop request accepted"
    return False, f"Failed (status {resp.status_code})"

def wait_for_state(token, name, target, user_id=None, timeout=45, interval=3):
    start = time.time()
    state = "Unknown"
    while time.time() - start < timeout:
        cs = get_codespaces(token, user_id)
        if cs is None:
            return False, "Status fetch failed"
        found = next((c for c in cs if c.get("name") == name), None)
        if not found:
            return False, f"Codespace {name} not found"
        state = found.get("state", "Unknown")
        if target == "Running" and state in ("Available", "Running", "Starting"):
            return True, f"State: {state}"
        if target == "Stopped" and state in ("Stopped", "Shutdown"):
            return True, f"State: {state}"
        time.sleep(interval)
    return False, f"Timeout waiting for {target} (current: {state})"

def format_codespace(cs):
    name = cs.get("name", "N/A")
    state = cs.get("state", "Unknown")
    repo = cs.get("repository", {}).get("full_name", "N/A")
    machine = cs.get("machine", {})
    machine_name = machine.get("display_name") or machine.get("name", "N/A")
    location = cs.get("location", "N/A")
    created = (cs.get("created_at") or "N/A")[:10]
    emoji = {"Available":"🟢","Starting":"🟡","Running":"🟢","Stopping":"🟠","Stopped":"🔴","Shutdown":"🔴","Deleted":"💀","Queued":"⏳"}.get(state, "❓")
    return f"*{emoji} {name}*\n📦 *Repo:* `{repo}`\n💻 *Machine:* {machine_name}\n📍 *Location:* {location}\n📅 *Created:* {created}\n📊 *Status:* `{state}`"

def get_action_buttons(cs):
    state = cs.get("state", "Unknown")
    name = cs.get("name", "unknown")
    markup = InlineKeyboardMarkup(row_width=2)
    buttons = []
    if state in ("Stopped", "Shutdown"): buttons.append(InlineKeyboardButton("▶️ Start", callback_data=f"start_{name}"))
    elif state in ("Available", "Running", "Starting", "Queued"): buttons.append(InlineKeyboardButton("⏹ Stop", callback_data=f"stop_{name}"))
    buttons.append(InlineKeyboardButton("🔄 Refresh", callback_data=f"refresh_{name}"))
    markup.add(*buttons)
    return markup

# ======================== FILE BROWSER ========================
def send_file_message(chat_id, user_id, page=0, edit_msg_id=None):
    """Send new or edit existing file browser message."""
    ctx = user_context.get(user_id)
    if not ctx or ctx.get("mode") != "files":
        bot.send_message(chat_id, "❌ Session expired. Use /files again.")
        return None
    tree = ctx.get("tree")
    if not tree:
        bot.send_message(chat_id, "❌ Tree not loaded.")
        return None
    path = ctx.get("path", "")
    files, dirs = list_directory_from_tree(tree, path)
    total_files = len(files)
    total_pages = (total_files + PAGE_SIZE - 1) // PAGE_SIZE if total_files > 0 else 1
    page = min(max(page, 0), total_pages - 1)
    start = page * PAGE_SIZE
    end = min(start + PAGE_SIZE, total_files)
    page_files = files[start:end]
    owner = ctx.get("owner", "")
    repo = ctx.get("repo", "")
    # Build text
    display = [f"📂 *{owner}/{repo}* / `{path or '/'}`"]
    display.append(f"📁 {len(dirs)} folders, 📄 {total_files} files (Page {page+1}/{total_pages})\n")
    for d in dirs:
        display.append(f"📁 {d}/")
    for f in page_files:
        name = f.get("path", "").split("/")[-1]
        size = f.get("size", 0)
        sz = f" ({size//1024}KB)" if size > 0 else ""
        display.append(f"📄 {name}{sz}")
    # Buttons
    buttons = []
    if path:
        parent = "/".join(path.split("/")[:-1])
        buttons.append(("⬆️ ..", f"dir|{owner}|{repo}|{parent}"))
    for d in dirs:
        np = f"{path}/{d}" if path else d
        buttons.append((f"📁 {d}", f"dir|{owner}|{repo}|{np}"))
    for f in page_files[:30]:
        fname = f.get("path", "").split("/")[-1]
        fp = f.get("path", "")
        buttons.append((f"📄 {fname}", f"file|{owner}|{repo}|{fp}"))
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"pg|{page-1}"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"pg|{page+1}"))
    nav.append(InlineKeyboardButton("🔄 Refresh", callback_data=f"refresh_tree|{owner}|{repo}"))
    markup = InlineKeyboardMarkup(row_width=2)
    if nav:
        markup.row(*nav[:2])
        if len(nav) > 2:
            markup.row(nav[2])
    row = []
    for label, cb in buttons[:50]:
        row.append(InlineKeyboardButton(label, callback_data=cb))
        if len(row) == 2:
            markup.row(*row)
            row = []
    if row:
        markup.row(*row)
    text = "\n".join(display)
    if edit_msg_id:
        try:
            bot.edit_message_text(text, chat_id, edit_msg_id, parse_mode="Markdown", reply_markup=markup)
            return None
        except Exception as e:
            logger.warning(f"Edit failed: {e}, sending new")
            return bot.send_message(chat_id, text, parse_mode="Markdown", reply_markup=markup)
    else:
        return bot.send_message(chat_id, text, parse_mode="Markdown", reply_markup=markup)

# ======================== GREP (CONTENT SEARCH) ========================
def grep_repo(owner, repo, branch, token, pattern, progress_msg, user_id):
    """Search pattern in all text files of the repo."""
    # First get tree
    tree, err = get_repo_tree(owner, repo, branch, token)
    if err or tree is None:
        return f"❌ {err or 'Could not fetch tree'}"
    # Filter text files
    text_files = []
    for item in tree:
        if item.get("type") == "blob":
            path = item.get("path", "")
            ext = os.path.splitext(path)[1].lower()
            if ext in TEXT_EXTENSIONS or os.path.basename(path) in ['.gitignore', '.dockerignore']:
                text_files.append(item)
    if not text_files:
        return "📭 No text files found in repository."
    total = len(text_files)
    safe_edit(progress_msg, f"🔍 Searching {total} text files for `{pattern}`...")
    matches = []
    for idx, item in enumerate(text_files):
        if idx % 10 == 0:
            try:
                safe_edit(progress_msg, f"🔍 Searching {idx+1}/{total} files...")
            except:
                pass
        file_path = item.get("path", "")
        size = item.get("size", 0)
        if size > 1024 * 1024:  # skip >1MB files for grep
            continue
        raw, _, err = get_file_content(owner, repo, file_path, branch, token)
        if err or raw is None:
            continue
        try:
            content = raw.decode('utf-8', errors='replace')
            lines = content.splitlines()
            for line_num, line in enumerate(lines, 1):
                if pattern.lower() in line.lower():
                    matches.append((file_path, line_num, line.strip()[:150]))
                    if len(matches) >= 20:
                        break
            if len(matches) >= 20:
                break
        except:
            pass
    safe_edit(progress_msg, f"✅ Grep complete: found {len(matches)} matches.")
    if not matches:
        return f"🔍 No matches found for `{pattern}`."
    output = f"🔍 *Matches for `{pattern}` in {owner}/{repo}* (showing {len(matches)})\n\n"
    for file_path, line_num, line in matches:
        output += f"📄 `{file_path}` (line {line_num})\n`{line}`\n\n"
    if len(output) > 4000:
        output = output[:3500] + "\n\n... (truncated)"
    return output

# ======================== COMMAND HANDLERS ========================
@bot.message_handler(commands=["start"])
def cmd_start_handler(message):
    parts = message.text.split(maxsplit=1)
    user_id = message.from_user.id
    if len(parts) == 1:
        bot.reply_to(message, "💥 𝙋𝘼𝙄𝘿 𝘽𝙊𝙏 24\\*7 💯\n🚀 GitHub Codespace Controller\n👑 OG SAGAR 😈\n\nUse /help for commands.")
        return
    name = parts[1].strip()
    if not re.match(r"^[a-z0-9-]+$", name):
        bot.reply_to(message, "❌ Invalid codespace name."); return
    token = db.get_token(user_id)
    if not token: bot.reply_to(message, "❌ /settoken pehle karo."); return
    lock = get_codespace_lock(name)
    if not lock.acquire(blocking=False):
        bot.reply_to(message, f"⏳ Operation already running on `{name}`", parse_mode="Markdown"); return
    try:
        msg = bot.reply_to(message, f"⏳ Starting `{name}`...", parse_mode="Markdown")
        ok, reply = start_codespace(token, name, user_id)
        if not ok: safe_edit(msg, f"❌ {reply}"); return
        db.log_action(user_id, "start", name)
        ok2, status = wait_for_state(token, name, "Running", user_id)
        if ok2:
            safe_edit(msg, f"✅ `{name}` is now Running!\n{status}")
        else:
            safe_edit(msg, f"⚠️ Start request sent but state not confirmed.\n{status}")
    finally:
        lock.release()

@bot.message_handler(commands=["stop"])
def cmd_stop(message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "❌ Usage: /stop <codespace_name>", parse_mode="Markdown"); return
    user_id = message.from_user.id
    name = parts[1].strip()
    if not re.match(r"^[a-z0-9-]+$", name):
        bot.reply_to(message, "❌ Invalid codespace name."); return
    token = db.get_token(user_id)
    if not token: bot.reply_to(message, "❌ /settoken pehle karo."); return
    lock = get_codespace_lock(name)
    if not lock.acquire(blocking=False):
        bot.reply_to(message, f"⏳ Operation already running on `{name}`", parse_mode="Markdown"); return
    try:
        msg = bot.reply_to(message, f"⏳ Stopping `{name}`...", parse_mode="Markdown")
        ok, reply = stop_codespace(token, name, user_id)
        if not ok: safe_edit(msg, f"❌ {reply}"); return
        db.log_action(user_id, "stop", name)
        ok2, status = wait_for_state(token, name, "Stopped", user_id)
        if ok2:
            safe_edit(msg, f"✅ `{name}` is now Stopped!\n{status}")
        else:
            safe_edit(msg, f"⚠️ Stop request sent but state not confirmed.\n{status}")
    finally:
        lock.release()

@bot.message_handler(commands=["list"])
def cmd_list(message):
    user_id = message.from_user.id
    token = db.get_token(user_id)
    if not token: bot.reply_to(message, "❌ /settoken pehle karo."); return
    spinner = bot.reply_to(message, "⏳ Fetching codespaces...")
    codespaces = get_codespaces(token, user_id)
    if codespaces is None: safe_edit(spinner, "❌ Fetch failed. Check token."); return
    if not codespaces: safe_edit(spinner, "📭 No codespaces."); return
    try: bot.delete_message(spinner.chat.id, spinner.message_id)
    except: pass
    for cs in codespaces:
        bot.send_message(message.chat.id, format_codespace(cs), parse_mode="Markdown", reply_markup=get_action_buttons(cs))
    db.log_action(user_id, "list", f"listed {len(codespaces)}")

@bot.message_handler(commands=["tokens"])
def cmd_tokens(message):
    user_id = message.from_user.id
    token = db.get_token(user_id)
    if not token: bot.reply_to(message, "📭 No token stored."); return
    masked = token[:8] + "..." + token[-8:] if len(token) > 16 else "***"
    bot.reply_to(message, f"📜 *Your token:*\n`{masked}`", parse_mode="Markdown")

@bot.message_handler(commands=["rmtoken"])
def cmd_rmtoken(message):
    user_id = message.from_user.id
    db.delete_token(user_id)
    bot.reply_to(message, "✅ Token deleted successfully.")

@bot.message_handler(commands=["settoken"])
def cmd_settoken(message):
    user_id = message.from_user.id
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2: bot.reply_to(message, "❌ Usage: /settoken <token>"); return
    token = parts[1].strip()
    if not token.startswith(("ghp_","gho_","ghu_","ghs_","ghr_")) or len(token) < 20:
        bot.reply_to(message, "❌ Invalid token format."); return
    codespaces = get_codespaces(token, user_id)
    if codespaces is None:
        bot.reply_to(message, "❌ Invalid token. Need `codespace` scope."); return
    db.save_token(user_id, token)
    db.log_action(user_id, "settoken", "saved")
    bot.reply_to(message, f"✅ Token saved! Found {len(codespaces)} codespace(s).")

@bot.message_handler(commands=["ratelimit"])
def cmd_ratelimit(message):
    user_id = message.from_user.id
    token = db.get_token(user_id)
    if not token: bot.reply_to(message, "❌ /settoken pehle karo."); return
    resp, err = api_call(requests.get, "https://api.github.com/rate_limit", token, user_id=user_id)
    if err or not resp or resp.status_code != 200:
        bot.reply_to(message, f"❌ Failed: {err}"); return
    core = resp.json().get("rate", {})
    reset_ts = core.get("reset", 0)
    reset_str = datetime.fromtimestamp(reset_ts).strftime("%H:%M:%S") if reset_ts else "?"
    bot.reply_to(message, f"📊 *Rate Limit*\nLimit: `{core.get('limit','?')}`\nRemaining: `{core.get('remaining','?')}`\nResets at: `{reset_str}`", parse_mode="Markdown")

# ======================== FILE BROWSING COMMANDS ========================
@bot.message_handler(commands=["files"])
def cmd_files(message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "❌ Usage: /files <repo_url> [path]\nExample: `/files https://github.com/EchoMusicApp/Echo-Music android/`", parse_mode="Markdown")
        return
    user_id = message.from_user.id
    token = db.get_token(user_id)
    text = parts[1].strip()
    if " " in text:
        url_part, path_part = text.split(" ", 1)
    else:
        url_part, path_part = text, ""
    parsed = parse_github_url(url_part)
    if not parsed:
        bot.reply_to(message, "❌ Invalid GitHub URL.")
        return
    owner, repo, initial_path = parsed
    final_path = path_part if path_part else (initial_path if initial_path else "")
    user_context[user_id] = {
        "owner": owner,
        "repo": repo,
        "path": final_path,
        "page": 0,
        "token": token,
        "mode": "files",
        "message_id": None
    }
    msg = bot.reply_to(message, "⏳ Fetching repository tree... This may take a moment for large repos.")
    tree, branch, err = get_cached_tree(owner, repo, token, progress_msg=msg)
    if err or tree is None:
        safe_edit(msg, f"❌ {err or 'Failed to fetch repository'}")
        return
    user_context[user_id]["tree"] = tree
    user_context[user_id]["branch"] = branch
    # Send first page
    sent = send_file_message(message.chat.id, user_id, 0, edit_msg_id=None)
    if sent:
        user_context[user_id]["message_id"] = sent.message_id
    try: bot.delete_message(msg.chat.id, msg.message_id)
    except: pass

@bot.message_handler(commands=["findfile"])
def cmd_findfile(message):
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        bot.reply_to(message, "❌ Usage: /findfile <repo_url> <filename>\nExample: `/findfile https://github.com/EchoMusicApp/Echo-Music lint.xml`", parse_mode="Markdown")
        return
    user_id = message.from_user.id
    token = db.get_token(user_id)
    url_part, filename = parts[1], parts[2]
    parsed = parse_github_url(url_part)
    if not parsed:
        bot.reply_to(message, "❌ Invalid GitHub URL.")
        return
    owner, repo, _ = parsed
    msg = bot.reply_to(message, f"⏳ Searching for `{filename}` in {owner}/{repo}...", parse_mode="Markdown")
    tree, branch, err = get_cached_tree(owner, repo, token, progress_msg=msg)
    if err or tree is None:
        safe_edit(msg, f"❌ {err or 'Failed to fetch repository'}")
        return
    results = search_files_by_pattern(tree, filename)
    if not results:
        safe_edit(msg, f"❌ No file found matching `{filename}`")
        return
    if len(results) > 20:
        safe_edit(msg, f"🔍 Found {len(results)} files matching `{filename}`. Showing first 20:")
    else:
        safe_edit(msg, f"🔍 Found {len(results)} file(s) matching `{filename}`:")
    for item in results[:20]:
        fpath = item.get("path", "Unknown")
        size = item.get("size", 0)
        sz = f" ({size//1024}KB)" if size > 0 else ""
        bot.send_message(
            msg.chat.id,
            f"📄 `{fpath}`{sz}\n⬇️ `/getfile https://github.com/{owner}/{repo} {fpath}`",
            parse_mode="Markdown"
        )

@bot.message_handler(commands=["getfile"])
def cmd_getfile(message):
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        bot.reply_to(message, "❌ Usage: /getfile <repo_url> <file_path>\nExample: `/getfile https://github.com/EchoMusicApp/Echo-Music android/app/src/main/lint.xml`", parse_mode="Markdown")
        return
    user_id = message.from_user.id
    token = db.get_token(user_id)
    url_part, file_path = parts[1], parts[2]
    parsed = parse_github_url(url_part)
    if not parsed:
        bot.reply_to(message, "❌ Invalid GitHub URL.")
        return
    owner, repo, _ = parsed
    branch = get_default_branch(owner, repo, token)
    if not branch:
        bot.reply_to(message, "❌ Could not determine default branch.")
        return
    msg = bot.reply_to(message, f"⏳ Downloading `{file_path}`...", parse_mode="Markdown")
    raw_data, _, err = get_file_content(owner, repo, file_path, branch, token, progress_msg=msg)
    if err or raw_data is None:
        safe_edit(msg, f"❌ {err or 'Download failed'}")
        return
    try:
        file_obj = BytesIO(raw_data)
        file_obj.name = os.path.basename(file_path)
        bot.send_document(
            message.chat.id,
            file_obj,
            caption=f"📄 `{file_path}` ({len(raw_data)//1024}KB)\n📦 {owner}/{repo}"
        )
        bot.delete_message(msg.chat.id, msg.message_id)
    except Exception as e:
        safe_edit(msg, f"❌ Send error: {e}")

@bot.message_handler(commands=["preview"])
def cmd_preview(message):
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        bot.reply_to(message, "❌ Usage: /preview <repo_url> <file_path>", parse_mode="Markdown")
        return
    user_id = message.from_user.id
    token = db.get_token(user_id)
    url_part, file_path = parts[1], parts[2]
    parsed = parse_github_url(url_part)
    if not parsed:
        bot.reply_to(message, "❌ Invalid GitHub URL.")
        return
    owner, repo, _ = parsed
    branch = get_default_branch(owner, repo, token)
    if not branch:
        bot.reply_to(message, "❌ Could not determine default branch.")
        return
    msg = bot.reply_to(message, f"⏳ Previewing `{file_path}`...", parse_mode="Markdown")
    raw_data, _, err = get_file_content(owner, repo, file_path, branch, token, progress_msg=msg)
    if err or raw_data is None:
        safe_edit(msg, f"❌ {err or 'Preview failed'}")
        return
    try:
        content = raw_data.decode('utf-8', errors='replace')[:3000]
        if len(raw_data) > 3000:
            content += "\n\n... (truncated, use /getfile for full)"
        safe_edit(msg, f"📄 *Preview of `{file_path}`*\n```\n{content}\n```", parse_mode="Markdown")
    except UnicodeDecodeError:
        safe_edit(msg, f"❌ Cannot preview binary file `{file_path}`. Use /getfile to download.")

# ======================== NEW: GREP COMMAND ========================
@bot.message_handler(commands=["grep"])
def cmd_grep(message):
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        bot.reply_to(message, "❌ Usage: /grep <repo_url> <pattern>\nExample: `/grep https://github.com/EchoMusicApp/Echo-Music .setOnClickListener`", parse_mode="Markdown")
        return
    user_id = message.from_user.id
    token = db.get_token(user_id)
    url_part, pattern = parts[1], parts[2]
    parsed = parse_github_url(url_part)
    if not parsed:
        bot.reply_to(message, "❌ Invalid GitHub URL.")
        return
    owner, repo, _ = parsed
    branch = get_default_branch(owner, repo, token)
    if not branch:
        bot.reply_to(message, "❌ Could not determine default branch.")
        return
    msg = bot.reply_to(message, f"🔍 Grepping `{pattern}` in {owner}/{repo}...", parse_mode="Markdown")
    result = grep_repo(owner, repo, branch, token, pattern, msg, user_id)
    if len(result) > 4000:
        # Split into multiple messages
        chunks = [result[i:i+4000] for i in range(0, len(result), 4000)]
        for chunk in chunks:
            bot.send_message(msg.chat.id, chunk, parse_mode="Markdown")
        bot.delete_message(msg.chat.id, msg.message_id)
    else:
        safe_edit(msg, result, parse_mode="Markdown")

# ======================== NEW: ZIP DOWNLOAD ========================
@bot.message_handler(commands=["zip"])
def cmd_zip(message):
    parts = message.text.split(maxsplit=2)
    if len(parts) < 2:
        bot.reply_to(message, "❌ Usage: /zip <repo_url> [branch]\nExample: `/zip https://github.com/EchoMusicApp/Echo-Music main`", parse_mode="Markdown")
        return
    user_id = message.from_user.id
    token = db.get_token(user_id)
    url_part = parts[1]
    branch = parts[2] if len(parts) > 2 else None
    parsed = parse_github_url(url_part)
    if not parsed:
        bot.reply_to(message, "❌ Invalid GitHub URL.")
        return
    owner, repo, _ = parsed
    if not branch:
        branch = get_default_branch(owner, repo, token)
        if not branch:
            bot.reply_to(message, "❌ Could not determine default branch.")
            return
    msg = bot.reply_to(message, f"⏳ Preparing zip for {owner}/{repo} ({branch})...", parse_mode="Markdown")
    zip_url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/zipball/{branch}"
    headers = github_headers(token)
    try:
        resp = requests.get(zip_url, headers=headers, stream=True, timeout=120)
        if resp.status_code == 200:
            total_size = int(resp.headers.get("Content-Length", 0))
            if total_size > 100 * 1024 * 1024:  # 100MB
                safe_edit(msg, "❌ Repository too large (>100MB) to download as zip.")
                return
            data = bytearray()
            downloaded = 0
            for chunk in resp.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                if chunk:
                    data.extend(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        percent = int((downloaded / total_size) * 100)
                        if percent % 10 == 0:
                            safe_edit(msg, f"⏳ Downloading zip... {percent}%")
            safe_edit(msg, "✅ Zip downloaded, sending...")
            zip_data = bytes(data)
            zip_obj = BytesIO(zip_data)
            zip_obj.name = f"{repo}-{branch}.zip"
            bot.send_document(message.chat.id, zip_obj, caption=f"📦 `{repo}` ({branch}) zip archive ({len(zip_data)//1024//1024}MB)")
            bot.delete_message(msg.chat.id, msg.message_id)
        else:
            err = user_friendly_error(f"HTTP {resp.status_code}")
            safe_edit(msg, f"❌ Failed to download zip: {err}")
    except Exception as e:
        safe_edit(msg, f"❌ Error: {str(e)}")

# ======================== NEW: CLEAR SESSION ========================
@bot.message_handler(commands=["clear"])
def cmd_clear(message):
    user_id = message.from_user.id
    if user_id in user_context:
        # Clear cache for that repo
        ctx = user_context[user_id]
        cache_key = f"{ctx.get('owner')}/{ctx.get('repo')}"
        if cache_key in tree_cache:
            del tree_cache[cache_key]
        del user_context[user_id]
        bot.reply_to(message, "✅ Session cleared and cache invalidated.")
    else:
        bot.reply_to(message, "✅ No active session.")

# ======================== ADMIN COMMANDS ========================
@bot.message_handler(commands=["sessions"])
def cmd_sessions(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "⛔ Admin only."); return
    if not user_context:
        bot.reply_to(message, "📭 No active user sessions.")
        return
    lines = ["👥 *Active User Sessions*\n"]
    for uid, ctx in user_context.items():
        owner = ctx.get("owner", "?")
        repo = ctx.get("repo", "?")
        path = ctx.get("path", "/")
        mode = ctx.get("mode", "files")
        lines.append(f"• `{uid}` – {owner}/{repo} – `{path}` ({mode})")
    bot.reply_to(message, "\n".join(lines), parse_mode="Markdown")

@bot.message_handler(commands=["clearcache"])
def cmd_clearcache(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "⛔ Admin only."); return
    size = len(tree_cache)
    tree_cache.clear()
    bot.reply_to(message, f"✅ Tree cache cleared ({size} entries removed).")

@bot.message_handler(commands=["users"])
def cmd_users(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "⛔ Admin only."); return
    if DB_TYPE == "mongo":
        users = db.tokens_col.distinct("user_id")
    else:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT user_id FROM tokens")
        users = [row[0] for row in c.fetchall()]
        conn.close()
    if not users:
        bot.reply_to(message, "📭 No users.")
        return
    bot.reply_to(message, f"👥 Users ({len(users)}):\n" + "\n".join([f"• `{uid}`" for uid in users]), parse_mode="Markdown")

@bot.message_handler(commands=["broadcast"])
def cmd_broadcast(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "⛔ Admin only."); return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "❌ Usage: /broadcast <message>", parse_mode="Markdown"); return
    msg_text = parts[1]
    if DB_TYPE == "mongo":
        users = db.tokens_col.distinct("user_id")
    else:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT user_id FROM tokens")
        users = [row[0] for row in c.fetchall()]
        conn.close()
    if not users:
        bot.reply_to(message, "📭 No users.")
        return
    sent = 0
    for uid in users:
        try:
            bot.send_message(uid, f"📣 *Announcement:*\n{msg_text}", parse_mode="Markdown")
            sent += 1
        except:
            pass
        time.sleep(0.2)
    bot.reply_to(message, f"✅ Broadcast sent to {sent} users.")

@bot.message_handler(commands=["stats"])
def cmd_stats(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "⛔ Admin only."); return
    if DB_TYPE == "mongo":
        actions_count = db.db["actions"].count_documents({})
    else:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM actions")
        actions_count = c.fetchone()[0]
        conn.close()
    bot.reply_to(message, f"📊 *Stats*\nTotal actions logged: `{actions_count}`", parse_mode="Markdown")

# ======================== HELP ========================
@bot.message_handler(commands=["help"])
def cmd_help(message):
    lines = [
        "💥 𝙋𝘼𝙄𝘿 𝘽𝙊𝙏 24\\*7 💯",
        "👑 *OG SAGAR 😈*",
        "━━━━━━━━━━━━━━━━━━━━━━━━━",
        "🔑 `/settoken <token>` – Save GitHub token",
        "🗑️ `/rmtoken` – Delete stored token",
        "📋 `/list` – Show codespaces",
        "▶️ `/start <name>` – Start a codespace",
        "⏹ `/stop <name>` – Stop a codespace",
        "📜 `/tokens` – View stored token",
        "📊 `/ratelimit` – Check API rate limit",
        "━━━━━━━━━━━━━━━━━━━━━━━━━",
        "📂 `/files <repo_url> [path]` – Browse ANY repo",
        "🔍 `/findfile <repo_url> <filename>` – Search file by name",
        "🔎 `/grep <repo_url> <pattern>` – Search file content (text files)",
        "⬇️ `/getfile <repo_url> <file_path>` – Download ANY file",
        "📄 `/preview <repo_url> <file_path>` – Preview text file",
        "📦 `/zip <repo_url> [branch]` – Download repo as ZIP",
        "━━━━━━━━━━━━━━━━━━━━━━━━━",
        "🧹 `/clear` – Reset your browsing session",
        "━━━━━━━━━━━━━━━━━━━━━━━━━",
        "👑 Admin: `/users` `/broadcast` `/stats` `/sessions` `/clearcache`",
        "━━━━━━━━━━━━━━━━━━━━━━━━━",
        "❓ `/help` – This message",
    ]
    bot.reply_to(message, "\n".join(lines), parse_mode="Markdown")

# ======================== CALLBACK HANDLER ========================
@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call: CallbackQuery):
    data = call.data
    chat_id = call.message.chat.id
    user_id = call.from_user.id

    # Pagination
    if data.startswith("pg|"):
        page = int(data.split("|")[1])
        ctx = user_context.get(user_id)
        if ctx:
            ctx["page"] = page
            msg_id = ctx.get("message_id")
            send_file_message(chat_id, user_id, page, edit_msg_id=msg_id)
        bot.answer_callback_query(call.id)
        return

    # Refresh tree
    if data.startswith("refresh_tree|"):
        parts = data.split("|")
        if len(parts) >= 3:
            _, owner, repo = parts[0], parts[1], parts[2]
            cache_key = f"{owner}/{repo}"
            if cache_key in tree_cache:
                del tree_cache[cache_key]
            token = db.get_token(user_id)
            msg = bot.send_message(chat_id, "🔄 Refreshing repository tree...")
            tree, branch, err = get_cached_tree(owner, repo, token, progress_msg=msg)
            if err or tree is None:
                safe_edit(msg, f"❌ {err or 'Failed to fetch'}")
                bot.answer_callback_query(call.id)
                return
            ctx = user_context.get(user_id)
            if ctx:
                ctx["tree"] = tree
                ctx["branch"] = branch
                ctx["page"] = 0
                msg_id = ctx.get("message_id")
                safe_edit(msg, "✅ Tree refreshed!")
                send_file_message(chat_id, user_id, 0, edit_msg_id=msg_id)
            else:
                safe_edit(msg, "✅ Tree refreshed! Use /files to start.")
        bot.answer_callback_query(call.id)
        return

    # Directory navigation
    if data.startswith("dir|"):
        parts = data.split("|")
        if len(parts) != 4:
            bot.answer_callback_query(call.id, "Invalid")
            return
        _, owner, repo, path = parts
        ctx = user_context.get(user_id)
        if ctx:
            ctx["owner"] = owner
            ctx["repo"] = repo
            ctx["path"] = path
            ctx["page"] = 0
        if not ctx or ctx.get("tree") is None:
            token = db.get_token(user_id)
            tree, branch, err = get_cached_tree(owner, repo, token)
            if err or tree is None:
                bot.send_message(chat_id, f"❌ {err or 'Failed to fetch'}")
                bot.answer_callback_query(call.id)
                return
            if ctx:
                ctx["tree"] = tree
                ctx["branch"] = branch
        msg_id = ctx.get("message_id") if ctx else None
        send_file_message(chat_id, user_id, 0, edit_msg_id=msg_id)
        bot.answer_callback_query(call.id, "Updated")
        return

    # File download
    if data.startswith("file|"):
        parts = data.split("|")
        if len(parts) != 4:
            bot.answer_callback_query(call.id, "Invalid")
            return
        _, owner, repo, file_path = parts
        token = db.get_token(user_id)
        if not token:
            bot.answer_callback_query(call.id, "❌ Token not set", show_alert=True)
            return
        bot.answer_callback_query(call.id, f"⬇️ Downloading {os.path.basename(file_path)}...")
        branch = get_default_branch(owner, repo, token)
        if not branch:
            bot.send_message(chat_id, "❌ Could not determine branch.")
            return
        msg = bot.send_message(chat_id, f"⏳ Downloading `{file_path}`...", parse_mode="Markdown")
        raw_data, _, err = get_file_content(owner, repo, file_path, branch, token, progress_msg=msg)
        if err or raw_data is None:
            safe_edit(msg, f"❌ {err or 'Download failed'}")
            return
        try:
            file_obj = BytesIO(raw_data)
            file_obj.name = os.path.basename(file_path)
            bot.send_document(chat_id, file_obj, caption=f"📄 `{file_path}` ({len(raw_data)//1024}KB)")
            bot.delete_message(msg.chat.id, msg.message_id)
        except Exception as e:
            safe_edit(msg, f"❌ Send error: {e}")
        return

    # Codespace actions
    token = db.get_token(user_id)
    if not token:
        bot.answer_callback_query(call.id, "❌ Token not set", show_alert=True)
        return

    if data.startswith("refresh_"):
        name = data[8:]
        bot.answer_callback_query(call.id, f"🔄 Refreshing {name}...")
        cs_list = get_codespaces(token, user_id)
        if cs_list is None:
            safe_edit(call.message, "❌ Fetch failed")
            return
        cs = next((c for c in cs_list if c.get("name") == name), None)
        if not cs:
            safe_edit(call.message, f"❌ Codespace `{name}` not found", parse_mode="Markdown")
            return
        new_text = format_codespace(cs)
        new_markup = get_action_buttons(cs)
        try:
            bot.edit_message_text(new_text, chat_id, call.message.message_id, parse_mode="Markdown", reply_markup=new_markup)
        except:
            pass
        db.log_action(user_id, "refresh", name)
        return

    if data.startswith("start_"):
        name = data[6:]
        bot.answer_callback_query(call.id, f"▶️ Starting {name}...")
        lock = get_codespace_lock(name)
        if not lock.acquire(blocking=False):
            bot.send_message(chat_id, f"⏳ Already running on `{name}`", parse_mode="Markdown")
            return
        try:
            try:
                bot.edit_message_text(f"⏳ Starting `{name}`...", chat_id, call.message.message_id, parse_mode="Markdown")
            except: pass
            ok, reply = start_codespace(token, name, user_id)
            if not ok:
                bot.send_message(chat_id, f"❌ {reply}"); return
            db.log_action(user_id, "start", name)
            ok2, status = wait_for_state(token, name, "Running", user_id)
            cs_list = get_codespaces(token, user_id)
            cs = next((c for c in cs_list if c.get("name") == name), None) if cs_list else None
            if cs:
                new_text = format_codespace(cs)
                new_markup = get_action_buttons(cs)
                try:
                    bot.edit_message_text(new_text, chat_id, call.message.message_id, parse_mode="Markdown", reply_markup=new_markup)
                except: pass
            else:
                bot.send_message(chat_id, f"✅ `{name}` started. Use /list to refresh.")
        finally:
            lock.release()
        return

    if data.startswith("stop_"):
        name = data[5:]
        bot.answer_callback_query(call.id, f"⏹ Stopping {name}...")
        lock = get_codespace_lock(name)
        if not lock.acquire(blocking=False):
            bot.send_message(chat_id, f"⏳ Already running on `{name}`", parse_mode="Markdown")
            return
        try:
            try:
                bot.edit_message_text(f"⏳ Stopping `{name}`...", chat_id, call.message.message_id, parse_mode="Markdown")
            except: pass
            ok, reply = stop_codespace(token, name, user_id)
            if not ok:
                bot.send_message(chat_id, f"❌ {reply}"); return
            db.log_action(user_id, "stop", name)
            ok2, status = wait_for_state(token, name, "Stopped", user_id)
            cs_list = get_codespaces(token, user_id)
            cs = next((c for c in cs_list if c.get("name") == name), None) if cs_list else None
            if cs:
                new_text = format_codespace(cs)
                new_markup = get_action_buttons(cs)
                try:
                    bot.edit_message_text(new_text, chat_id, call.message.message_id, parse_mode="Markdown", reply_markup=new_markup)
                except: pass
            else:
                bot.send_message(chat_id, f"✅ `{name}` stopped. Use /list to refresh.")
        finally:
            lock.release()
        return

    bot.answer_callback_query(call.id, "❓ Unknown action")

# ======================== START ========================
if __name__ == "__main__":
    logger.info("🚀 Ultimate GitHub Bot started polling...")
    try:
        bot.infinity_polling()
    except KeyboardInterrupt:
        logger.info("Bot stopped.")
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
        sys.exit(1)