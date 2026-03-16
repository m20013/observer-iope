"""
Instagram Follower Monitor Bot — واجهة أزرار كاملة
====================================================
✅ كل شيء بأزرار — لا حاجة لكتابة أوامر
✅ قائمة رئيسية للمستخدم
✅ لوحة تحكم كاملة للأدمن بأزرار
"""

import asyncio
import json
import logging
import random
import re
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timedelta

import instaloader
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ─────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────
TELEGRAM_BOT_TOKEN = "5369101126:AAGRqa0yBQyK3HxyIF46yi_RYsYQDgCFKrI"
ADMIN_IDS          = [1148510962]

DB_PATH       = "monitor.db"
SETTINGS_FILE = "settings.json"
TRIAL_DAYS    = 3
MAX_TARGETS   = 5
MIN_DELAY     = 2.0
MAX_DELAY     = 5.0
# ─────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("IGBot")

# ══════════════════════════════════════════
#  Default Settings
# ══════════════════════════════════════════
DEFAULT_SETTINGS = {
    "sub_required": False,
    "payment_info": "للاشتراك تواصل: @YourSupportUsername",
    "plans": {
        "trial": {
            "name": "تجربة مجانية", "days": 3,
            "price": 0, "emoji": "🎁", "check_hours": 12,
        },
        "week_basic": {
            "name": "أسبوعي عادي", "days": 7,
            "price": 9, "emoji": "🥈", "check_hours": 8,
        },
        "week_pro": {
            "name": "أسبوعي برو", "days": 7,
            "price": 20, "emoji": "🥇", "check_hours": 6,
        },
        "month": {
            "name": "شهري", "days": 30,
            "price": 50, "emoji": "💎", "check_hours": 3,
        },
        "year": {
            "name": "سنوي VIP", "days": 365,
            "price": 199, "emoji": "👑", "check_hours": 1,
        },
    },
    "ig_accounts": [],
}


# ══════════════════════════════════════════
#  Settings
# ══════════════════════════════════════════
class Settings:
    def __init__(self, path=SETTINGS_FILE):
        self.path  = path
        self._data = {}
        self._load()

    def _load(self):
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                self._data = {**DEFAULT_SETTINGS, **loaded}
                for k, v in DEFAULT_SETTINGS["plans"].items():
                    if k not in self._data["plans"]:
                        self._data["plans"][k] = v
        except (FileNotFoundError, json.JSONDecodeError):
            self._data = DEFAULT_SETTINGS.copy()
            self._save()

    def _save(self):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, ensure_ascii=False, indent=2)

    @property
    def sub_required(self) -> bool:
        return self._data.get("sub_required", False)

    @property
    def payment_info(self) -> str:
        return self._data.get("payment_info", "")

    @property
    def plans(self) -> dict:
        return self._data.get("plans", DEFAULT_SETTINGS["plans"])

    @property
    def ig_accounts(self) -> list[dict]:
        return self._data.get("ig_accounts", [])

    def get_active_accounts(self) -> list[dict]:
        return [a for a in self.ig_accounts if a.get("active", True)]

    def toggle_sub_required(self) -> bool:
        val = not self._data.get("sub_required", False)
        self._data["sub_required"] = val
        self._save()
        return val

    def update_payment_info(self, text: str):
        self._data["payment_info"] = text
        self._save()

    def update_plan_price(self, key: str, price: int) -> bool:
        if key in self._data["plans"]:
            self._data["plans"][key]["price"] = price
            self._save()
            return True
        return False

    def update_plan_interval(self, key: str, hours: float) -> bool:
        if key in self._data["plans"]:
            self._data["plans"][key]["check_hours"] = hours
            self._save()
            return True
        return False

    def add_ig_account(self, username: str, password: str) -> bool:
        accounts = self._data.setdefault("ig_accounts", [])
        for a in accounts:
            if a["username"] == username:
                a["password"] = password
                a["active"]   = True
                self._save()
                return False
        accounts.append({"username": username, "password": password, "active": True})
        self._save()
        return True

    def remove_ig_account(self, username: str) -> bool:
        before = len(self._data.get("ig_accounts", []))
        self._data["ig_accounts"] = [
            a for a in self._data.get("ig_accounts", [])
            if a["username"] != username
        ]
        self._save()
        return len(self._data["ig_accounts"]) < before

    def toggle_ig_account(self, username: str):
        for a in self._data.get("ig_accounts", []):
            if a["username"] == username:
                a["active"] = not a.get("active", True)
                self._save()
                return a["active"]
        return None


# ══════════════════════════════════════════
#  FSM States
# ══════════════════════════════════════════
class AddTarget(StatesGroup):
    waiting = State()

class BroadcastState(StatesGroup):
    waiting = State()

class GrantSubState(StatesGroup):
    get_id   = State()
    get_plan = State()
    get_days = State()

class AddIGAccountState(StatesGroup):
    get_username = State()
    get_password = State()

class EditPriceState(StatesGroup):
    get_plan  = State()
    get_price = State()

class EditIntervalState(StatesGroup):
    get_plan  = State()
    get_hours = State()

class EditPaymentState(StatesGroup):
    get_text = State()

class BanState(StatesGroup):
    get_id = State()

class UnbanState(StatesGroup):
    get_id = State()


# ══════════════════════════════════════════
#  Database
# ══════════════════════════════════════════
class Database:
    def __init__(self, path=DB_PATH):
        self.path = path
        self._init()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init(self):
        with self._conn() as c:
            c.executescript("""
                CREATE TABLE IF NOT EXISTS users (
                    chat_id      INTEGER PRIMARY KEY,
                    username     TEXT,
                    full_name    TEXT,
                    joined_at    TEXT DEFAULT (datetime('now')),
                    trial_ends   TEXT,
                    sub_ends     TEXT,
                    sub_plan     TEXT DEFAULT 'trial',
                    is_banned    INTEGER DEFAULT 0,
                    is_paused    INTEGER DEFAULT 0,
                    total_checks INTEGER DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS targets (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id     INTEGER NOT NULL,
                    ig_username TEXT    NOT NULL,
                    added_at    TEXT    DEFAULT (datetime('now')),
                    last_check  TEXT,
                    UNIQUE(chat_id, ig_username)
                );
                CREATE TABLE IF NOT EXISTS followers (
                    user_id   TEXT    NOT NULL,
                    username  TEXT    NOT NULL,
                    full_name TEXT,
                    chat_id   INTEGER NOT NULL,
                    ig_target TEXT    NOT NULL,
                    PRIMARY KEY (user_id, chat_id, ig_target)
                );
                CREATE TABLE IF NOT EXISTS unfollow_log (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id     INTEGER NOT NULL,
                    ig_target   TEXT    NOT NULL,
                    unfollower  TEXT    NOT NULL,
                    user_id     TEXT,
                    detected_at TEXT    DEFAULT (datetime('now'))
                );
                CREATE TABLE IF NOT EXISTS sub_log (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id    INTEGER NOT NULL,
                    plan       TEXT,
                    days       INTEGER,
                    price      INTEGER DEFAULT 0,
                    granted_by INTEGER,
                    granted_at TEXT    DEFAULT (datetime('now'))
                );
            """)

    def _parse_dt(self, s) -> datetime | None:
        if not s:
            return None
        try:
            return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    def register_user(self, chat_id: int, username: str, full_name: str):
        trial = (datetime.now() + timedelta(days=TRIAL_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as c:
            c.execute("""
                INSERT INTO users (chat_id,username,full_name,trial_ends,sub_plan)
                VALUES (?,?,?,?,'trial')
                ON CONFLICT(chat_id) DO UPDATE SET
                    username=excluded.username, full_name=excluded.full_name
            """, (chat_id, username or "", full_name or "", trial))

    def get_user(self, chat_id: int) -> dict | None:
        with self._conn() as c:
            r = c.execute("SELECT * FROM users WHERE chat_id=?", (chat_id,)).fetchone()
        return dict(r) if r else None

    def get_all_users(self) -> list[dict]:
        with self._conn() as c:
            rows = c.execute("SELECT * FROM users ORDER BY joined_at DESC").fetchall()
        return [dict(r) for r in rows]

    def is_active(self, chat_id: int) -> bool:
        u   = self.get_user(chat_id)
        now = datetime.now()
        if not u:
            return False
        t = self._parse_dt(u.get("trial_ends"))
        s = self._parse_dt(u.get("sub_ends"))
        return (t and t > now) or (s and s > now)

    def is_in_trial(self, chat_id: int) -> bool:
        u = self.get_user(chat_id)
        t = self._parse_dt(u.get("trial_ends")) if u else None
        return bool(t and t > datetime.now())

    def days_left(self, chat_id: int) -> int:
        u    = self.get_user(chat_id)
        now  = datetime.now()
        best = now
        for key in ("trial_ends", "sub_ends"):
            dt = self._parse_dt(u.get(key)) if u else None
            if dt and dt > best:
                best = dt
        return max((best - now).days, 0)

    def get_user_plan(self, chat_id: int) -> str:
        u = self.get_user(chat_id)
        if not u:
            return "trial"
        if self._parse_dt(u.get("sub_ends")) and \
           self._parse_dt(u.get("sub_ends")) > datetime.now():
            return u.get("sub_plan") or "trial"
        return "trial"

    def grant_subscription(self, chat_id: int, days: int, plan: str, price=0, granted_by=0):
        u    = self.get_user(chat_id)
        now  = datetime.now()
        base = now
        s    = self._parse_dt(u.get("sub_ends")) if u else None
        if s and s > now:
            base = s
        new_end = (base + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as c:
            c.execute(
                "UPDATE users SET sub_ends=?, sub_plan=? WHERE chat_id=?",
                (new_end, plan, chat_id),
            )
            c.execute(
                "INSERT INTO sub_log (chat_id,plan,days,price,granted_by) VALUES (?,?,?,?,?)",
                (chat_id, plan, days, price, granted_by),
            )

    def revoke_subscription(self, chat_id: int):
        with self._conn() as c:
            c.execute(
                "UPDATE users SET sub_ends=NULL, sub_plan='trial' WHERE chat_id=?",
                (chat_id,),
            )

    def ban_user(self, chat_id: int):
        with self._conn() as c:
            c.execute("UPDATE users SET is_banned=1 WHERE chat_id=?", (chat_id,))

    def unban_user(self, chat_id: int):
        with self._conn() as c:
            c.execute("UPDATE users SET is_banned=0 WHERE chat_id=?", (chat_id,))

    def pause_user(self, chat_id: int):
        with self._conn() as c:
            c.execute("UPDATE users SET is_paused=1 WHERE chat_id=?", (chat_id,))

    def resume_user(self, chat_id: int):
        with self._conn() as c:
            c.execute("UPDATE users SET is_paused=0 WHERE chat_id=?", (chat_id,))

    def increment_checks(self, chat_id: int):
        with self._conn() as c:
            c.execute(
                "UPDATE users SET total_checks=total_checks+1 WHERE chat_id=?",
                (chat_id,),
            )

    def is_banned(self, chat_id: int) -> bool:
        u = self.get_user(chat_id)
        return bool(u and u["is_banned"])

    def is_paused(self, chat_id: int) -> bool:
        u = self.get_user(chat_id)
        return bool(u and u["is_paused"])

    def add_target(self, chat_id: int, ig_username: str) -> bool:
        try:
            with self._conn() as c:
                c.execute(
                    "INSERT INTO targets (chat_id,ig_username) VALUES (?,?)",
                    (chat_id, ig_username),
                )
            return True
        except sqlite3.IntegrityError:
            return False

    def remove_target(self, chat_id: int, ig_username: str):
        with self._conn() as c:
            c.execute(
                "DELETE FROM targets WHERE chat_id=? AND ig_username=?",
                (chat_id, ig_username),
            )
            c.execute(
                "DELETE FROM followers WHERE chat_id=? AND ig_target=?",
                (chat_id, ig_username),
            )

    def get_user_targets(self, chat_id: int) -> list[str]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT ig_username FROM targets WHERE chat_id=?", (chat_id,)
            ).fetchall()
        return [r["ig_username"] for r in rows]

    def count_user_targets(self, chat_id: int) -> int:
        with self._conn() as c:
            return c.execute(
                "SELECT COUNT(*) FROM targets WHERE chat_id=?", (chat_id,)
            ).fetchone()[0]

    def update_last_check(self, chat_id: int, ig_username: str):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as c:
            c.execute(
                "UPDATE targets SET last_check=? WHERE chat_id=? AND ig_username=?",
                (now, chat_id, ig_username),
            )

    def get_targets_due(self, plan_hours: dict) -> list[dict]:
        now = datetime.now()
        with self._conn() as c:
            rows = c.execute("""
                SELECT t.chat_id, t.ig_username, t.last_check, u.sub_plan
                FROM targets t JOIN users u ON t.chat_id=u.chat_id
                WHERE u.is_banned=0 AND u.is_paused=0
            """).fetchall()
        due = []
        for r in rows:
            row      = dict(r)
            hours    = plan_hours.get(row.get("sub_plan") or "trial", 12)
            last     = self._parse_dt(row.get("last_check"))
            if not last or (now - last).total_seconds() >= hours * 3600:
                due.append(row)
        return due

    def get_followers(self, chat_id: int, ig_target: str) -> dict[str, str]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT user_id,username FROM followers WHERE chat_id=? AND ig_target=?",
                (chat_id, ig_target),
            ).fetchall()
        return {r["user_id"]: r["username"] for r in rows}

    def save_followers(self, chat_id: int, ig_target: str, followers: list[dict]):
        with self._conn() as c:
            c.execute(
                "DELETE FROM followers WHERE chat_id=? AND ig_target=?",
                (chat_id, ig_target),
            )
            c.executemany(
                "INSERT INTO followers (user_id,username,full_name,chat_id,ig_target) "
                "VALUES (:user_id,:username,:full_name,:chat_id,:ig_target)",
                [{**f, "chat_id": chat_id, "ig_target": ig_target} for f in followers],
            )

    def log_unfollow(self, chat_id, ig_target, unfollower, uid):
        with self._conn() as c:
            c.execute(
                "INSERT INTO unfollow_log (chat_id,ig_target,unfollower,user_id) VALUES (?,?,?,?)",
                (chat_id, ig_target, unfollower, uid),
            )

    def get_unfollow_log(self, chat_id, ig_target, limit=10) -> list[dict]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT unfollower,detected_at FROM unfollow_log "
                "WHERE chat_id=? AND ig_target=? ORDER BY detected_at DESC LIMIT ?",
                (chat_id, ig_target, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_bot_stats(self) -> dict:
        now = datetime.now()
        with self._conn() as c:
            total = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            as_ = tr_ = ex_ = 0
            for u in [dict(r) for r in c.execute("SELECT * FROM users").fetchall()]:
                s = self._parse_dt(u.get("sub_ends"))
                t = self._parse_dt(u.get("trial_ends"))
                if s and s > now:   as_ += 1
                elif t and t > now: tr_ += 1
                else:               ex_ += 1
            banned  = c.execute("SELECT COUNT(*) FROM users WHERE is_banned=1").fetchone()[0]
            targets = c.execute("SELECT COUNT(*) FROM targets").fetchone()[0]
            unf     = c.execute("SELECT COUNT(*) FROM unfollow_log").fetchone()[0]
            rev     = c.execute("SELECT COALESCE(SUM(price),0) FROM sub_log").fetchone()[0]
        return {
            "total": total, "active_sub": as_, "trial_only": tr_,
            "expired": ex_, "banned": banned, "targets": targets,
            "unfollows": unf, "revenue": rev,
        }

    def get_expiring_soon(self, hours=24) -> list[dict]:
        soon = (datetime.now() + timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as c:
            rows = c.execute("""
                SELECT chat_id,full_name,sub_ends FROM users
                WHERE sub_ends IS NOT NULL AND sub_ends BETWEEN ? AND ? AND is_banned=0
            """, (now, soon)).fetchall()
        return [dict(r) for r in rows]


# ══════════════════════════════════════════
#  IG Account Pool
# ══════════════════════════════════════════
class IGAccountPool:
    def __init__(self, cfg: Settings):
        self.cfg      = cfg
        self._loaders: dict[str, instaloader.Instaloader] = {}
        self._index   = 0

    def _make_loader(self):
        return instaloader.Instaloader(
            sleep=True, quiet=True,
            download_pictures=False, download_videos=False,
            download_video_thumbnails=False, save_metadata=False,
        )

    def login_all(self) -> int:
        accounts = self.cfg.get_active_accounts()
        if not accounts:
            return 0
        success = 0
        for acc in accounts:
            if self.login_one(acc["username"], acc["password"]):
                success += 1
        return success

    def login_one(self, username: str, password: str) -> bool:
        L = self._make_loader()
        try:
            try:
                L.load_session_from_file(username)
                self._loaders[username] = L
                return True
            except FileNotFoundError:
                pass
            L.login(username, password)
            L.save_session_to_file()
            self._loaders[username] = L
            return True
        except Exception as e:
            logger.error("❌ @%s: %s", username, e)
            return False

    def logout_one(self, username: str):
        self._loaders.pop(username, None)

    def _next_loader(self):
        active    = self.cfg.get_active_accounts()
        available = [
            (a["username"], self._loaders[a["username"]])
            for a in active if a["username"] in self._loaders
        ]
        if not available:
            return None
        self._index = self._index % len(available)
        result      = available[self._index]
        self._index = (self._index + 1) % len(available)
        return result

    def fetch_followers(self, target: str) -> list[dict] | None:
        info = self._next_loader()
        if not info:
            return None
        uname, L = info
        try:
            profile = instaloader.Profile.from_username(L.context, target)
        except Exception as e:
            logger.error("❌ بروفايل @%s: %s", target, e)
            return None
        followers = []
        try:
            for f in profile.get_followers():
                followers.append({
                    "user_id":   str(f.userid),
                    "username":  f.username,
                    "full_name": f.full_name or "",
                })
                time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
        except instaloader.exceptions.TooManyRequestsException:
            time.sleep(900)
            return None
        except Exception as e:
            logger.error("❌ متابعي @%s: %s", target, e)
            return None
        return followers

    def count(self) -> int:
        return len(self._loaders)

    def list_accounts(self) -> list[str]:
        return list(self._loaders.keys())


# ══════════════════════════════════════════
#  Monitor Service
# ══════════════════════════════════════════
class MonitorService:
    def __init__(self, db: Database, pool: IGAccountPool, bot: Bot, cfg: Settings):
        self.db   = db
        self.pool = pool
        self.bot  = bot
        self.cfg  = cfg

    async def _send(self, chat_id: int, text: str, **kw):
        try:
            await self.bot.send_message(
                chat_id, text, parse_mode="HTML",
                disable_web_page_preview=True, **kw,
            )
        except Exception as e:
            logger.error("❌ إرسال %s: %s", chat_id, e)

    async def _notify_admins(self, text: str):
        for aid in ADMIN_IDS:
            await self._send(aid, text)

    async def check(self, chat_id: int, ig_target: str, silent=False):
        if self.db.is_banned(chat_id) or self.db.is_paused(chat_id):
            return
        if self.cfg.sub_required and not self.db.is_active(chat_id):
            if not silent:
                await self._send(
                    chat_id,
                    "⛔ <b>انتهت فترة تجربتك!</b>\nاشترك لمواصلة المراقبة 👇",
                    reply_markup=sub_kb(),
                )
            return

        old      = self.db.get_followers(chat_id, ig_target)
        new_data = await asyncio.get_event_loop().run_in_executor(
            None, self.pool.fetch_followers, ig_target
        )
        self.db.update_last_check(chat_id, ig_target)

        if new_data is None:
            if not silent:
                await self._send(chat_id, f"⚠️ تعذّر جلب بيانات <b>@{ig_target}</b>.")
            return

        new          = {f["user_id"]: f for f in new_data}
        old_ids      = set(old.keys())
        new_ids      = set(new.keys())
        unfollowed   = old_ids - new_ids
        new_followed = new_ids - old_ids

        self.db.save_followers(chat_id, ig_target, new_data)
        self.db.increment_checks(chat_id)

        for uid in unfollowed:
            uname = old[uid]
            self.db.log_unfollow(chat_id, ig_target, uname, uid)
            await self._send(
                chat_id,
                f"👋 <b>إلغاء متابعة!</b>\n\n"
                f"🎯 الحساب: <b>@{ig_target}</b>\n"
                f"👤 المستخدم: <b>@{uname}</b>\n"
                f"🔗 <a href='https://instagram.com/{uname}/'>فتح الحساب</a>\n"
                f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            )
            await asyncio.sleep(0.3)

        if old_ids and not silent:
            plan_key  = self.db.get_user_plan(chat_id)
            h         = self.cfg.plans.get(plan_key, {}).get("check_hours", 12)
            h_txt     = f"{int(h*60)} دقيقة" if h < 1 else f"{h} ساعة"
            await self._send(
                chat_id,
                f"📊 <b>تقرير @{ig_target}</b>\n\n"
                f"👥 المتابعون: <b>{len(new_ids)}</b>\n"
                f"🔴 ألغوا المتابعة: <b>{len(unfollowed)}</b>\n"
                f"🟢 متابعون جدد: <b>{len(new_followed)}</b>\n"
                f"⏱ الفحص القادم: بعد <b>{h_txt}</b>\n"
                f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            )

    async def run_due(self):
        plan_hours = {k: v.get("check_hours", 12) for k, v in self.cfg.plans.items()}
        due        = self.db.get_targets_due(plan_hours)
        for t in due:
            if not self.cfg.sub_required or self.db.is_active(t["chat_id"]):
                await self.check(t["chat_id"], t["ig_username"], silent=True)
                await asyncio.sleep(random.uniform(5, 10))

    async def notify_expiring(self):
        for u in self.db.get_expiring_soon(24):
            await self._send(
                u["chat_id"],
                "⚠️ <b>ينتهي اشتراكك خلال 24 ساعة!</b>\nجدد الآن 👇",
                reply_markup=sub_kb(),
            )


# ══════════════════════════════════════════
#  Keyboard Builders
# ══════════════════════════════════════════

def main_menu_kb(is_admin=False, is_paused=False) -> ReplyKeyboardMarkup:
    """الكيبورد الرئيسي الثابت أسفل الشاشة."""
    rows = [
        [KeyboardButton(text="➕ إضافة حساب"),   KeyboardButton(text="📋 حساباتي")],
        [KeyboardButton(text="🔍 فحص فوري"),      KeyboardButton(text="📈 إحصائياتي")],
        [KeyboardButton(text="📜 سجل إلغاءات"),   KeyboardButton(text="💳 الاشتراك")],
        [KeyboardButton(text="⏸ إيقاف مؤقت" if not is_paused else "▶️ استئناف")],
    ]
    if is_admin:
        rows.append([KeyboardButton(text="🛠 لوحة الأدمن")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def admin_panel_kb(sub_required: bool) -> InlineKeyboardMarkup:
    """لوحة تحكم الأدمن بالكامل — inline buttons."""
    sub_icon = "🟢" if sub_required else "🔴"
    return InlineKeyboardMarkup(inline_keyboard=[
        # الصف 1
        [
            InlineKeyboardButton(text="📊 إحصائيات البوت",  callback_data="adm:stats"),
            InlineKeyboardButton(text="👥 المستخدمون",       callback_data="adm:users"),
        ],
        # الصف 2
        [
            InlineKeyboardButton(text="📸 حسابات IG",        callback_data="adm:iglist"),
            InlineKeyboardButton(text="➕ إضافة حساب IG",    callback_data="adm:igadd"),
        ],
        # الصف 3
        [
            InlineKeyboardButton(text="💳 منح اشتراك",       callback_data="adm:grant"),
            InlineKeyboardButton(text="🚫 سحب اشتراك",       callback_data="adm:revoke"),
        ],
        # الصف 4
        [
            InlineKeyboardButton(text="⏳ ينتهون قريباً",    callback_data="adm:expiring"),
            InlineKeyboardButton(text="🔄 فحص إجباري",       callback_data="adm:forcecheck"),
        ],
        # الصف 5
        [
            InlineKeyboardButton(text="🚫 حظر مستخدم",       callback_data="adm:ban"),
            InlineKeyboardButton(text="✅ رفع حظر",          callback_data="adm:unban"),
        ],
        # الصف 6
        [
            InlineKeyboardButton(text="💰 تعديل الأسعار",    callback_data="adm:editprice"),
            InlineKeyboardButton(text="⏱ تعديل معدل الفحص", callback_data="adm:editinterval"),
        ],
        # الصف 7
        [
            InlineKeyboardButton(text="📲 معلومات الدفع",    callback_data="adm:editpayment"),
            InlineKeyboardButton(text="⚙️ الإعدادات",        callback_data="adm:settings"),
        ],
        # الصف 8
        [
            InlineKeyboardButton(
                text=f"{sub_icon} الاشتراك الإجباري",
                callback_data="adm:togglesub",
            ),
            InlineKeyboardButton(text="📢 إذاعة جماعية",    callback_data="adm:broadcast"),
        ],
    ])


def plans_kb(cfg: Settings) -> InlineKeyboardMarkup:
    buttons = []
    for key, p in cfg.plans.items():
        if key == "trial":
            continue
        h     = p.get("check_hours", 12)
        h_txt = f"{int(h*60)}د" if h < 1 else f"{h}س"
        buttons.append([InlineKeyboardButton(
            text=f"{p['emoji']} {p['name']} — {p['price']} ريال | فحص/{h_txt}",
            callback_data=f"buy:{key}",
        )])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def sub_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="💳 خطط الاشتراك", callback_data="show_plans")
    ]])


# ══════════════════════════════════════════
#  Handlers
# ══════════════════════════════════════════
def register_handlers(
    dp: Dispatcher,
    db: Database,
    monitor: MonitorService,
    pool: IGAccountPool,
    cfg: Settings,
):
    def is_admin(cid): return cid in ADMIN_IDS

    def h_fmt(hours: float) -> str:
        return f"{int(hours*60)} دقيقة" if hours < 1 else f"{hours} ساعة"

    async def sub_guard(msg: Message) -> bool:
        if not cfg.sub_required or db.is_active(msg.chat.id):
            return True
        await msg.answer(
            "⛔ <b>انتهت فترة تجربتك!</b>\nاشترك للاستمرار 👇",
            parse_mode="HTML",
            reply_markup=sub_kb(),
        )
        return False

    async def send_main_menu(msg: Message, text: str = ""):
        uid     = msg.chat.id
        paused  = db.is_paused(uid)
        adm     = is_admin(uid)
        kb      = main_menu_kb(is_admin=adm, is_paused=paused)
        await msg.answer(
            text or "🏠 <b>القائمة الرئيسية</b>",
            parse_mode="HTML",
            reply_markup=kb,
        )

    # ────────────────────────────────────────
    #  /start
    # ────────────────────────────────────────
    @dp.message(Command("start"))
    async def cmd_start(msg: Message, state: FSMContext):
        await state.clear()
        db.register_user(msg.chat.id, msg.from_user.username, msg.from_user.full_name)
        if db.is_banned(msg.chat.id):
            await msg.answer("🚫 أنت محظور.")
            return

        plan_key  = db.get_user_plan(msg.chat.id)
        plan_info = cfg.plans.get(plan_key, {})
        h         = plan_info.get("check_hours", 12)
        days      = db.days_left(msg.chat.id)

        if cfg.sub_required:
            if db.is_in_trial(msg.chat.id):
                sub_line = f"🎁 تجربة مجانية — <b>{days} يوم متبق</b> | فحص كل {h_fmt(h)}"
            elif db.is_active(msg.chat.id):
                sub_line = (
                    f"✅ {plan_info.get('emoji','')} {plan_info.get('name','')} "
                    f"— <b>{days} يوم</b> | فحص كل {h_fmt(h)}"
                )
            else:
                sub_line = "⛔ انتهت فترتك المجانية"
        else:
            sub_line = f"✅ مجاني | فحص كل {h_fmt(h)}"

        await send_main_menu(
            msg,
            f"👋 <b>أهلاً {msg.from_user.first_name}!</b>\n\n"
            f"🔍 <b>بوت مراقبة متابعي إنستغرام</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"{sub_line}\n\n"
            f"اختر من القائمة أدناه 👇",
        )

    # ────────────────────────────────────────
    #  أزرار المستخدم الرئيسية
    # ────────────────────────────────────────

    @dp.message(F.text == "➕ إضافة حساب")
    async def btn_add(msg: Message, state: FSMContext):
        db.register_user(msg.chat.id, msg.from_user.username, msg.from_user.full_name)
        if db.is_banned(msg.chat.id) or not await sub_guard(msg):
            return
        if db.count_user_targets(msg.chat.id) >= MAX_TARGETS:
            await msg.answer(f"⚠️ الحد الأقصى <b>{MAX_TARGETS} حسابات</b>.", parse_mode="HTML")
            return
        if pool.count() == 0:
            await msg.answer("⚠️ البوت غير جاهز، تواصل مع الإدارة.")
            return
        await state.set_state(AddTarget.waiting)
        await msg.answer(
            "📝 أرسل يوزر حساب الإنستغرام:\n<i>مثال: cristiano</i>",
            parse_mode="HTML",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="❌ إلغاء")]],
                resize_keyboard=True,
            ),
        )

    @dp.message(AddTarget.waiting)
    async def receive_username(msg: Message, state: FSMContext):
        if msg.text == "❌ إلغاء":
            await state.clear()
            await send_main_menu(msg, "تم الإلغاء.")
            return
        await state.clear()
        raw = msg.text.strip().lstrip("@").lower()
        if not re.match(r'^[a-zA-Z0-9_.]{1,30}$', raw):
            await send_main_menu(msg, "❌ يوزر غير صالح.")
            return
        if not db.add_target(msg.chat.id, raw):
            await send_main_menu(msg, f"⚠️ @{raw} موجود مسبقاً!")
            return

        plan_key  = db.get_user_plan(msg.chat.id)
        plan_info = cfg.plans.get(plan_key, {})
        h         = plan_info.get("check_hours", 12)

        await send_main_menu(
            msg,
            f"✅ تمت إضافة <b>@{raw}</b>!\n"
            f"⏱ سيُفحص كل <b>{h_fmt(h)}</b>\n"
            f"🔄 يبدأ الفحص الآن…",
        )

        u = msg.from_user
        await monitor._notify_admins(
            f"📩 <b>يوزر إنستغرام جديد</b>\n\n"
            f"👤 <a href='tg://user?id={msg.chat.id}'>{u.full_name}</a>\n"
            f"🆔 ID: <code>{msg.chat.id}</code>\n"
            f"📛 تيليجرام: @{u.username or 'لا يوجد'}\n"
            f"📸 إنستغرام: <b>@{raw}</b>\n"
            f"💳 الخطة: <b>{plan_info.get('name','تجربة')}</b>\n"
            f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        )
        asyncio.create_task(monitor.check(msg.chat.id, raw))

    @dp.message(F.text == "📋 حساباتي")
    async def btn_list(msg: Message):
        if db.is_banned(msg.chat.id) or not await sub_guard(msg):
            return
        targets = db.get_user_targets(msg.chat.id)
        if not targets:
            await msg.answer(
                "📭 لا توجد حسابات مُراقَبة.\nاضغط <b>➕ إضافة حساب</b> للبدء.",
                parse_mode="HTML",
            )
            return

        lines = ["📋 <b>حساباتك المُراقَبة:</b>\n"]
        for t in targets:
            cnt = len(db.get_followers(msg.chat.id, t))
            lines.append(f"• @{t} — <b>{cnt}</b> متابع")

        # أزرار حذف لكل حساب
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"❌ حذف @{t}", callback_data=f"rm:{t}")]
            for t in targets
        ])
        await msg.answer("\n".join(lines), parse_mode="HTML", reply_markup=kb)

    @dp.callback_query(F.data.startswith("rm:"))
    async def cb_remove(cb: types.CallbackQuery):
        t = cb.data.split(":", 1)[1]
        db.remove_target(cb.message.chat.id, t)
        await cb.message.edit_text(
            f"✅ تم إيقاف مراقبة <b>@{t}</b>.", parse_mode="HTML"
        )
        await cb.answer()

    @dp.message(F.text == "🔍 فحص فوري")
    async def btn_check(msg: Message):
        if db.is_banned(msg.chat.id) or not await sub_guard(msg):
            return
        targets = db.get_user_targets(msg.chat.id)
        if not targets:
            await msg.answer("📭 لا توجد حسابات. اضغط ➕ إضافة حساب.")
            return
        await msg.answer(f"🔄 فحص فوري لـ <b>{len(targets)}</b> حساب…", parse_mode="HTML")
        for t in targets:
            await monitor.check(msg.chat.id, t)
        await msg.answer("✅ انتهى الفحص.")

    @dp.message(F.text == "📈 إحصائياتي")
    async def btn_stats(msg: Message):
        if db.is_banned(msg.chat.id):
            return
        db.register_user(msg.chat.id, msg.from_user.username, msg.from_user.full_name)
        user      = db.get_user(msg.chat.id)
        targets   = db.get_user_targets(msg.chat.id)
        total_f   = sum(len(db.get_followers(msg.chat.id, t)) for t in targets)
        plan_key  = db.get_user_plan(msg.chat.id)
        plan_info = cfg.plans.get(plan_key, {})
        h         = plan_info.get("check_hours", 12)

        if db.is_in_trial(msg.chat.id):
            sub_s = f"🎁 تجربة — <b>{db.days_left(msg.chat.id)} يوم</b>"
        elif db.is_active(msg.chat.id):
            sub_s = f"✅ {plan_info.get('name','')} — <b>{db.days_left(msg.chat.id)} يوم</b>"
        else:
            sub_s = "⛔ منتهي"

        await msg.answer(
            f"📈 <b>إحصائياتك</b>\n\n"
            f"👤 <b>{msg.from_user.full_name}</b>\n"
            f"📅 الانضمام: <b>{(user['joined_at'] or '')[:10]}</b>\n"
            f"💳 الاشتراك: {sub_s}\n"
            f"⚡ معدل الفحص: كل <b>{h_fmt(h)}</b>\n"
            f"🔔 الحالة: <b>{'⏸ مُوقَف' if db.is_paused(msg.chat.id) else '▶️ نشط'}</b>\n"
            f"🎯 حسابات: <b>{len(targets)}</b>\n"
            f"👥 متابعون محفوظون: <b>{total_f}</b>\n"
            f"🔄 فحوصات: <b>{user['total_checks'] if user else 0}</b>",
            parse_mode="HTML",
        )

    @dp.message(F.text == "📜 سجل إلغاءات")
    async def btn_history(msg: Message):
        if db.is_banned(msg.chat.id) or not await sub_guard(msg):
            return
        targets = db.get_user_targets(msg.chat.id)
        if not targets:
            await msg.answer("📭 لا توجد حسابات.")
            return
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"📜 @{t}", callback_data=f"hist:{t}")]
            for t in targets
        ])
        await msg.answer("اختر الحساب:", reply_markup=kb)

    @dp.callback_query(F.data.startswith("hist:"))
    async def cb_history(cb: types.CallbackQuery):
        t    = cb.data.split(":", 1)[1]
        logs = db.get_unfollow_log(cb.message.chat.id, t)
        if not logs:
            await cb.message.edit_text(f"📭 لا يوجد سجل لـ @{t}.")
        else:
            lines = [f"📜 <b>آخر إلغاءات @{t}:</b>\n"]
            for l in logs:
                lines.append(f"• @{l['unfollower']} — <i>{l['detected_at'][:16]}</i>")
            await cb.message.edit_text("\n".join(lines), parse_mode="HTML")
        await cb.answer()

    @dp.message(F.text == "💳 الاشتراك")
    async def btn_subscribe(msg: Message):
        db.register_user(msg.chat.id, msg.from_user.username, msg.from_user.full_name)
        days   = db.days_left(msg.chat.id)
        status = (
            f"⏳ لديك <b>{days} يوم متبقية</b>"
            if db.is_active(msg.chat.id) else "⛔ لا يوجد اشتراك فعّال"
        )
        await msg.answer(
            f"💳 <b>خطط الاشتراك</b>\n━━━━━━━━━━━━━━━━━━━\n{status}\n\nاختر الخطة:",
            parse_mode="HTML",
            reply_markup=plans_kb(cfg),
        )

    @dp.callback_query(F.data == "show_plans")
    async def cb_show_plans(cb: types.CallbackQuery):
        await cb.message.answer(
            "💳 <b>خطط الاشتراك</b>\n━━━━━━━━━━━━━━━━━━━\nاختر الخطة:",
            parse_mode="HTML",
            reply_markup=plans_kb(cfg),
        )
        await cb.answer()

    @dp.callback_query(F.data.startswith("buy:"))
    async def cb_buy(cb: types.CallbackQuery):
        key  = cb.data.split(":", 1)[1]
        plan = cfg.plans.get(key)
        if not plan:
            await cb.answer("خطأ.")
            return
        h = plan.get("check_hours", 12)
        await cb.message.answer(
            f"{plan['emoji']} <b>خطة {plan['name']}</b>\n\n"
            f"💰 السعر: <b>{plan['price']} ريال</b>\n"
            f"📅 المدة: <b>{plan['days']} يوم</b>\n"
            f"⚡ معدل الفحص: كل <b>{h_fmt(h)}</b>\n\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"📲 {cfg.payment_info}\n\n"
            f"📌 أرسل للدعم:\n"
            f"<code>اشتراك {plan['name']} - ID: {cb.message.chat.id}</code>",
            parse_mode="HTML",
        )
        await cb.answer()

    @dp.message(F.text.in_({"⏸ إيقاف مؤقت", "▶️ استئناف"}))
    async def btn_pause_resume(msg: Message):
        if db.is_banned(msg.chat.id):
            return
        if msg.text == "⏸ إيقاف مؤقت":
            db.pause_user(msg.chat.id)
            await send_main_menu(msg, "⏸ تم إيقاف الإشعارات مؤقتاً.")
        else:
            db.resume_user(msg.chat.id)
            await send_main_menu(msg, "▶️ تم استئناف الإشعارات.")

    # ────────────────────────────────────────
    #  لوحة الأدمن
    # ────────────────────────────────────────

    @dp.message(F.text == "🛠 لوحة الأدمن")
    async def btn_admin(msg: Message):
        if not is_admin(msg.chat.id):
            return
        await msg.answer(
            "🛠 <b>لوحة تحكم الأدمن</b>\n\nاختر من الأزرار أدناه:",
            parse_mode="HTML",
            reply_markup=admin_panel_kb(cfg.sub_required),
        )

    # ── إحصائيات البوت ────────────────────
    @dp.callback_query(F.data == "adm:stats")
    async def adm_stats(cb: types.CallbackQuery):
        if not is_admin(cb.message.chat.id):
            return
        s      = db.get_bot_stats()
        sub_st = "🟢 مفعّل" if cfg.sub_required else "🔴 مُعطَّل"
        await cb.message.answer(
            f"📊 <b>إحصائيات البوت</b>\n\n"
            f"👥 المستخدمون: <b>{s['total']}</b>\n"
            f"💳 مشتركون: <b>{s['active_sub']}</b>\n"
            f"🎁 تجربة: <b>{s['trial_only']}</b>\n"
            f"⛔ منتهي: <b>{s['expired']}</b>\n"
            f"🚫 محظورون: <b>{s['banned']}</b>\n"
            f"🎯 حسابات مُراقَبة: <b>{s['targets']}</b>\n"
            f"👋 إلغاءات: <b>{s['unfollows']}</b>\n"
            f"💰 الإيرادات: <b>{s['revenue']} ريال</b>\n"
            f"📸 حسابات IG: <b>{pool.count()}/{len(cfg.ig_accounts)}</b>\n"
            f"🔐 اشتراك إجباري: {sub_st}",
            parse_mode="HTML",
        )
        await cb.answer()

    # ── المستخدمون ────────────────────────
    @dp.callback_query(F.data == "adm:users")
    async def adm_users(cb: types.CallbackQuery):
        if not is_admin(cb.message.chat.id):
            return
        users = db.get_all_users()
        now   = datetime.now()
        lines = [f"👥 <b>المستخدمون ({len(users)}):</b>\n"]
        for u in users[:25]:
            if u["is_banned"]:
                icon = "🚫"
            elif db._parse_dt(u.get("sub_ends")) and db._parse_dt(u.get("sub_ends")) > now:
                icon = "💳"
            elif db._parse_dt(u.get("trial_ends")) and db._parse_dt(u.get("trial_ends")) > now:
                icon = "🎁"
            else:
                icon = "⛔"
            name = u["full_name"] or u["username"] or str(u["chat_id"])
            lines.append(f"{icon} <b>{name}</b> — <code>{u['chat_id']}</code>")
        if len(users) > 25:
            lines.append(f"\n<i>...و {len(users)-25} آخرين</i>")
        await cb.message.answer("\n".join(lines), parse_mode="HTML")
        await cb.answer()

    # ── حسابات IG ─────────────────────────
    @dp.callback_query(F.data == "adm:iglist")
    async def adm_iglist(cb: types.CallbackQuery):
        if not is_admin(cb.message.chat.id):
            return
        accounts = cfg.ig_accounts
        if not accounts:
            await cb.message.answer("📭 لا توجد حسابات IG.\nاضغط ➕ إضافة حساب IG.")
            await cb.answer()
            return
        lines = [f"📸 <b>حسابات إنستغرام ({len(accounts)}):</b>\n"]
        buttons = []
        for a in accounts:
            uname  = a["username"]
            active = a.get("active", True)
            logged = "✅" if uname in pool.list_accounts() else "❌"
            st     = "🟢" if active else "🔴"
            lines.append(f"{st} @{uname} {logged}")
            buttons.append([
                InlineKeyboardButton(text=f"{'🔴 تعطيل' if active else '🟢 تفعيل'} @{uname}", callback_data=f"tgig:{uname}"),
                InlineKeyboardButton(text=f"🗑 حذف @{uname}", callback_data=f"rmig:{uname}"),
            ])
        await cb.message.answer(
            "\n".join(lines),
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        )
        await cb.answer()

    @dp.callback_query(F.data == "adm:igadd")
    async def adm_igadd(cb: types.CallbackQuery, state: FSMContext):
        if not is_admin(cb.message.chat.id):
            return
        await state.set_state(AddIGAccountState.get_username)
        await cb.message.answer("📸 أرسل يوزر حساب إنستغرام الجديد:")
        await cb.answer()

    @dp.message(AddIGAccountState.get_username)
    async def ig_get_username(msg: Message, state: FSMContext):
        raw = msg.text.strip().lstrip("@").lower()
        if not re.match(r'^[a-zA-Z0-9_.]{1,30}$', raw):
            await msg.answer("❌ يوزر غير صالح.")
            return
        await state.update_data(new_ig_user=raw)
        await state.set_state(AddIGAccountState.get_password)
        await msg.answer(f"🔑 أرسل كلمة مرور @{raw}:")

    @dp.message(AddIGAccountState.get_password)
    async def ig_get_password(msg: Message, state: FSMContext):
        data  = await state.get_data()
        uname = data["new_ig_user"]
        passw = msg.text.strip()
        await state.clear()
        try:
            await msg.delete()
        except Exception:
            pass
        wait = await msg.answer(f"🔄 جاري تسجيل الدخول بـ @{uname}…")
        success = await asyncio.get_event_loop().run_in_executor(
            None, pool.login_one, uname, passw
        )
        if success:
            cfg.add_ig_account(uname, passw)
            await wait.edit_text(
                f"✅ تم إضافة <b>@{uname}</b>!\nإجمالي الحسابات: <b>{pool.count()}</b>",
                parse_mode="HTML",
            )
        else:
            await wait.edit_text(f"❌ فشل تسجيل الدخول بـ @{uname}")

    @dp.callback_query(F.data.startswith("rmig:"))
    async def cb_rmig(cb: types.CallbackQuery):
        uname = cb.data.split(":", 1)[1]
        cfg.remove_ig_account(uname)
        pool.logout_one(uname)
        await cb.message.edit_text(
            f"✅ تم حذف <b>@{uname}</b>. حسابات متبقية: <b>{pool.count()}</b>",
            parse_mode="HTML",
        )
        await cb.answer()

    @dp.callback_query(F.data.startswith("tgig:"))
    async def cb_tgig(cb: types.CallbackQuery):
        uname  = cb.data.split(":", 1)[1]
        result = cfg.toggle_ig_account(uname)
        status = "🟢 نشط" if result else "🔴 معطّل"
        await cb.message.answer(f"✅ @{uname} الآن: {status}")
        await cb.answer()

    # ── منح اشتراك ────────────────────────
    @dp.callback_query(F.data == "adm:grant")
    async def adm_grant(cb: types.CallbackQuery, state: FSMContext):
        if not is_admin(cb.message.chat.id):
            return
        await state.set_state(GrantSubState.get_id)
        await cb.message.answer("أرسل Chat ID المستخدم:")
        await cb.answer()

    @dp.message(GrantSubState.get_id)
    async def grant_get_id(msg: Message, state: FSMContext):
        if not msg.text.lstrip("-").isdigit():
            await msg.answer("❌ أرسل ID رقمي.")
            return
        await state.update_data(target_id=int(msg.text))
        await state.set_state(GrantSubState.get_plan)
        buttons = [
            [InlineKeyboardButton(
                text=f"{p['emoji']} {p['name']} ({p['days']} يوم)",
                callback_data=f"grant:{key}",
            )]
            for key, p in cfg.plans.items() if key != "trial"
        ] + [[InlineKeyboardButton(text="🎁 أيام مخصصة", callback_data="grant:custom")]]
        await msg.answer("اختر الخطة:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

    @dp.callback_query(F.data.startswith("grant:"))
    async def cb_grant(cb: types.CallbackQuery, state: FSMContext):
        data = await state.get_data()
        tid  = data.get("target_id")
        key  = cb.data.split(":", 1)[1]
        if key == "custom":
            await state.update_data(plan_key="custom")
            await state.set_state(GrantSubState.get_days)
            await cb.message.answer("أدخل عدد الأيام:")
            await cb.answer()
            return
        plan = cfg.plans.get(key)
        if not plan or not tid:
            await cb.answer("خطأ.")
            return
        await state.clear()
        if not db.get_user(tid):
            db.register_user(tid, "", "")
        db.grant_subscription(tid, plan["days"], key, plan["price"], cb.message.chat.id)
        h = plan.get("check_hours", 12)
        await cb.message.edit_text(
            f"✅ تم منح <b>{plan['name']}</b> للمستخدم <code>{tid}</code>",
            parse_mode="HTML",
        )
        await monitor._send(
            tid,
            f"🎉 <b>تم تفعيل اشتراكك!</b>\n\n"
            f"{plan['emoji']} الخطة: <b>{plan['name']}</b>\n"
            f"📅 المدة: <b>{plan['days']} يوم</b>\n"
            f"⚡ معدل الفحص: كل <b>{h_fmt(h)}</b>",
        )
        await cb.answer()

    @dp.message(GrantSubState.get_days)
    async def grant_custom_days(msg: Message, state: FSMContext):
        if not msg.text.isdigit():
            await msg.answer("❌ أرسل رقماً.")
            return
        data = await state.get_data()
        days = int(msg.text)
        tid  = data["target_id"]
        await state.clear()
        if not db.get_user(tid):
            db.register_user(tid, "", "")
        db.grant_subscription(tid, days, "trial", 0, msg.chat.id)
        await msg.answer(f"✅ تم منح <b>{days} يوم</b> للمستخدم <code>{tid}</code>", parse_mode="HTML")
        await monitor._send(tid, f"🎁 <b>تم منحك {days} يوم!</b>\nاضغط ➕ إضافة حساب للبدء.")

    # ── سحب اشتراك ────────────────────────
    @dp.callback_query(F.data == "adm:revoke")
    async def adm_revoke(cb: types.CallbackQuery):
        if not is_admin(cb.message.chat.id):
            return
        users = [u for u in db.get_all_users() if not u["is_banned"]]
        now   = datetime.now()
        subbed = [
            u for u in users
            if db._parse_dt(u.get("sub_ends")) and db._parse_dt(u.get("sub_ends")) > now
        ]
        if not subbed:
            await cb.message.answer("📭 لا يوجد مشتركون حالياً.")
            await cb.answer()
            return
        buttons = [
            [InlineKeyboardButton(
                text=f"🚫 {u['full_name'] or u['chat_id']}",
                callback_data=f"dorevoke:{u['chat_id']}",
            )]
            for u in subbed[:20]
        ]
        await cb.message.answer(
            "اختر المستخدم لسحب اشتراكه:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        )
        await cb.answer()

    @dp.callback_query(F.data.startswith("dorevoke:"))
    async def cb_dorevoke(cb: types.CallbackQuery):
        tid = int(cb.data.split(":", 1)[1])
        db.revoke_subscription(tid)
        await cb.message.edit_text(f"✅ تم سحب اشتراك <code>{tid}</code>.", parse_mode="HTML")
        await monitor._send(tid, "⚠️ تم إلغاء اشتراكك.")
        await cb.answer()

    # ── ينتهون قريباً ─────────────────────
    @dp.callback_query(F.data == "adm:expiring")
    async def adm_expiring(cb: types.CallbackQuery):
        if not is_admin(cb.message.chat.id):
            return
        exp = db.get_expiring_soon(48)
        if not exp:
            await cb.message.answer("✅ لا يوجد اشتراكات تنتهي خلال 48 ساعة.")
        else:
            lines = ["⏳ <b>تنتهي قريباً:</b>\n"]
            for u in exp:
                lines.append(
                    f"• <b>{u['full_name'] or u['chat_id']}</b> — "
                    f"<code>{u['chat_id']}</code> — <i>{(u['sub_ends'] or '')[:16]}</i>"
                )
            await cb.message.answer("\n".join(lines), parse_mode="HTML")
        await cb.answer()

    # ── فحص إجباري ────────────────────────
    @dp.callback_query(F.data == "adm:forcecheck")
    async def adm_forcecheck(cb: types.CallbackQuery):
        if not is_admin(cb.message.chat.id):
            return
        await cb.message.answer("🔄 بدء الفحص الإجباري في الخلفية…")
        asyncio.create_task(monitor.run_due())
        await cb.answer()

    # ── حظر ───────────────────────────────
    @dp.callback_query(F.data == "adm:ban")
    async def adm_ban(cb: types.CallbackQuery, state: FSMContext):
        if not is_admin(cb.message.chat.id):
            return
        await state.set_state(BanState.get_id)
        await cb.message.answer("أرسل Chat ID المستخدم لحظره:")
        await cb.answer()

    @dp.message(BanState.get_id)
    async def do_ban(msg: Message, state: FSMContext):
        if not msg.text.lstrip("-").isdigit():
            await msg.answer("❌ أرسل ID رقمي.")
            return
        tid = int(msg.text)
        await state.clear()
        db.ban_user(tid)
        await msg.answer(f"🚫 تم حظر <code>{tid}</code>.", parse_mode="HTML")
        await monitor._send(tid, "🚫 تم حظرك من البوت.")

    # ── رفع حظر ───────────────────────────
    @dp.callback_query(F.data == "adm:unban")
    async def adm_unban(cb: types.CallbackQuery, state: FSMContext):
        if not is_admin(cb.message.chat.id):
            return
        await state.set_state(UnbanState.get_id)
        await cb.message.answer("أرسل Chat ID المستخدم لرفع الحظر:")
        await cb.answer()

    @dp.message(UnbanState.get_id)
    async def do_unban(msg: Message, state: FSMContext):
        if not msg.text.lstrip("-").isdigit():
            await msg.answer("❌ أرسل ID رقمي.")
            return
        tid = int(msg.text)
        await state.clear()
        db.unban_user(tid)
        await msg.answer(f"✅ رُفع الحظر عن <code>{tid}</code>.", parse_mode="HTML")
        await monitor._send(tid, "✅ تم رفع الحظر عنك.")

    # ── تعديل الأسعار ─────────────────────
    @dp.callback_query(F.data == "adm:editprice")
    async def adm_editprice(cb: types.CallbackQuery, state: FSMContext):
        if not is_admin(cb.message.chat.id):
            return
        await state.set_state(EditPriceState.get_plan)
        buttons = [
            [InlineKeyboardButton(
                text=f"{p['emoji']} {p['name']} ({p['price']} ريال)",
                callback_data=f"editprc:{key}",
            )]
            for key, p in cfg.plans.items()
        ]
        await cb.message.answer(
            "💰 اختر الخطة:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        )
        await cb.answer()

    @dp.callback_query(F.data.startswith("editprc:"))
    async def cb_editprc(cb: types.CallbackQuery, state: FSMContext):
        key = cb.data.split(":", 1)[1]
        await state.update_data(plan_key=key)
        await state.set_state(EditPriceState.get_price)
        plan = cfg.plans.get(key, {})
        await cb.message.answer(
            f"💰 <b>{plan.get('name','')}</b> — السعر الحالي: <b>{plan.get('price',0)} ريال</b>\n\nأرسل السعر الجديد:",
            parse_mode="HTML",
        )
        await cb.answer()

    @dp.message(EditPriceState.get_price)
    async def do_editprice(msg: Message, state: FSMContext):
        if not msg.text.isdigit():
            await msg.answer("❌ أرسل رقماً.")
            return
        data  = await state.get_data()
        key   = data["plan_key"]
        price = int(msg.text)
        await state.clear()
        cfg.update_plan_price(key, price)
        plan = cfg.plans.get(key, {})
        await msg.answer(f"✅ سعر <b>{plan.get('name','')}</b> الآن: <b>{price} ريال</b>", parse_mode="HTML")

    # ── تعديل معدل الفحص ──────────────────
    @dp.callback_query(F.data == "adm:editinterval")
    async def adm_editinterval(cb: types.CallbackQuery, state: FSMContext):
        if not is_admin(cb.message.chat.id):
            return
        await state.set_state(EditIntervalState.get_plan)
        buttons = []
        for key, p in cfg.plans.items():
            h = p.get("check_hours", 12)
            buttons.append([InlineKeyboardButton(
                text=f"{p['emoji']} {p['name']} (حالياً: {h_fmt(h)})",
                callback_data=f"editint:{key}",
            )])
        await cb.message.answer(
            "⏱ اختر الخطة:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        )
        await cb.answer()

    @dp.callback_query(F.data.startswith("editint:"))
    async def cb_editint(cb: types.CallbackQuery, state: FSMContext):
        key = cb.data.split(":", 1)[1]
        await state.update_data(plan_key=key)
        await state.set_state(EditIntervalState.get_hours)
        await cb.message.answer(
            "⏱ أرسل معدل الفحص بالساعات:\n"
            "<i>1 = ساعة | 0.5 = 30 دقيقة | 0.25 = 15 دقيقة</i>",
            parse_mode="HTML",
        )
        await cb.answer()

    @dp.message(EditIntervalState.get_hours)
    async def do_editinterval(msg: Message, state: FSMContext):
        try:
            hours = float(msg.text.replace(",", "."))
            assert hours > 0
        except Exception:
            await msg.answer("❌ أرسل رقماً موجباً مثل: 1 أو 0.5")
            return
        data = await state.get_data()
        key  = data["plan_key"]
        await state.clear()
        cfg.update_plan_interval(key, hours)
        plan = cfg.plans.get(key, {})
        await msg.answer(
            f"✅ <b>{plan.get('name','')}</b> الآن: كل <b>{h_fmt(hours)}</b>",
            parse_mode="HTML",
        )

    # ── معلومات الدفع ─────────────────────
    @dp.callback_query(F.data == "adm:editpayment")
    async def adm_editpayment(cb: types.CallbackQuery, state: FSMContext):
        if not is_admin(cb.message.chat.id):
            return
        await state.set_state(EditPaymentState.get_text)
        await cb.message.answer(
            f"📲 الحالي:\n<code>{cfg.payment_info}</code>\n\nأرسل النص الجديد:",
            parse_mode="HTML",
        )
        await cb.answer()

    @dp.message(EditPaymentState.get_text)
    async def do_editpayment(msg: Message, state: FSMContext):
        await state.clear()
        cfg.update_payment_info(msg.text.strip())
        await msg.answer("✅ تم تحديث معلومات الدفع.")

    # ── الإعدادات ─────────────────────────
    @dp.callback_query(F.data == "adm:settings")
    async def adm_settings(cb: types.CallbackQuery):
        if not is_admin(cb.message.chat.id):
            return
        plans_txt = ""
        for key, p in cfg.plans.items():
            h = p.get("check_hours", 12)
            plans_txt += f"  {p['emoji']} {p['name']}: <b>{p['price']} ريال</b> | {h_fmt(h)}\n"
        accs_txt = ""
        for a in cfg.ig_accounts:
            st = "🟢" if a.get("active", True) else "🔴"
            accs_txt += f"  {st} @{a['username']}\n"
        sub_st = "🟢 مفعّل" if cfg.sub_required else "🔴 مُعطَّل"
        await cb.message.answer(
            f"⚙️ <b>الإعدادات الحالية</b>\n\n"
            f"🔐 اشتراك إجباري: {sub_st}\n\n"
            f"📸 <b>حسابات IG:</b>\n{accs_txt or '  لا يوجد\n'}\n"
            f"💳 <b>الخطط:</b>\n{plans_txt}\n"
            f"📲 <b>الدفع:</b>\n{cfg.payment_info}",
            parse_mode="HTML",
        )
        await cb.answer()

    # ── تفعيل/تعطيل الاشتراك الإجباري ────
    @dp.callback_query(F.data == "adm:togglesub")
    async def adm_togglesub(cb: types.CallbackQuery):
        if not is_admin(cb.message.chat.id):
            return
        val    = cfg.toggle_sub_required()
        status = "🟢 مفعّل" if val else "🔴 مُعطَّل"
        await cb.message.answer(
            f"🔐 الاشتراك الإجباري الآن: <b>{status}</b>",
            parse_mode="HTML",
            reply_markup=admin_panel_kb(val),
        )
        await cb.answer()

    # ── إذاعة جماعية ──────────────────────
    @dp.callback_query(F.data == "adm:broadcast")
    async def adm_broadcast(cb: types.CallbackQuery, state: FSMContext):
        if not is_admin(cb.message.chat.id):
            return
        users  = db.get_all_users()
        active = [u for u in users if not u["is_banned"]]
        await state.set_state(BroadcastState.waiting)
        await cb.message.answer(
            f"📢 سيتم الإرسال لـ <b>{len(active)}</b> مستخدم.\n\nأرسل الرسالة:",
            parse_mode="HTML",
        )
        await cb.answer()

    @dp.message(BroadcastState.waiting)
    async def do_broadcast(msg: Message, state: FSMContext):
        await state.clear()
        users  = db.get_all_users()
        sent   = failed = 0
        text   = f"📢 <b>رسالة من الإدارة:</b>\n\n{msg.text}"
        for u in users:
            if u["is_banned"]:
                continue
            try:
                await monitor._send(u["chat_id"], text)
                sent += 1
                await asyncio.sleep(0.05)
            except Exception:
                failed += 1
        await msg.answer(
            f"📢 <b>اكتملت الإذاعة</b>\n✅ أُرسلت: <b>{sent}</b>\n❌ فشل: <b>{failed}</b>",
            parse_mode="HTML",
        )


# ══════════════════════════════════════════
#  Main
# ══════════════════════════════════════════
async def main():
    if "YOUR_TELEGRAM" in TELEGRAM_BOT_TOKEN:
        logger.error("❌ ضع TELEGRAM_BOT_TOKEN في CONFIG!")
        return

    cfg  = Settings()
    db   = Database()
    pool = IGAccountPool(cfg)
    bot  = Bot(token=TELEGRAM_BOT_TOKEN)
    dp   = Dispatcher(storage=MemoryStorage())

    count = await asyncio.get_event_loop().run_in_executor(None, pool.login_all)
    if count == 0:
        logger.warning("⚠️ لا توجد حسابات IG — أضف من لوحة الأدمن")

    monitor = MonitorService(db=db, pool=pool, bot=bot, cfg=cfg)
    register_handlers(dp, db, monitor, pool, cfg)

    scheduler = AsyncIOScheduler()
    scheduler.add_job(monitor.run_due,         "interval", minutes=1,  id="smart_check")
    scheduler.add_job(monitor.notify_expiring, "interval", hours=12,   id="expiry_notify")
    scheduler.start()

    logger.info("🤖 البوت يعمل | %d حساب IG", pool.count())

    try:
        await dp.start_polling(bot, allowed_updates=["message", "callback_query"])
    finally:
        scheduler.shutdown()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
