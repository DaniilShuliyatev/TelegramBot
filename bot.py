import asyncio
import json
import os
import re
from datetime import datetime, timedelta, time as dtime
from typing import Optional, List, Dict, Any

import aiosqlite
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import pytz

# -------------------- Настройки --------------------
load_dotenv()

API_TOKEN = os.getenv("API_TOKEN")
ADMIN_ID = os.getenv("ADMIN_ID")  # первичный админ (optional)
DB_PATH = os.getenv("DB_PATH", "scheduler.db")

if not API_TOKEN:
    raise RuntimeError("API_TOKEN not set in environment (.env)")

# Таймзона Europe/Moscow (GMT+3)
TZ = pytz.timezone("Europe/Moscow")

# Без parse_mode="HTML", чтобы не ловить ошибки на <...>
bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# -------------------- Утилиты --------------------
def now_trunc_min() -> datetime:
    # Текущее время в Europe/Moscow, обрезанные секунды/микросекунды
    return datetime.now(TZ).replace(second=0, microsecond=0)

def parse_chat_identifier(text: str) -> Optional[str]:
    text = text.strip()
    if re.fullmatch(r"-?\d+", text):
        return text
    m = re.search(r"(?:t\.me/|telegram\.me/)([A-Za-z0-9_]+)", text)
    if m:
        return "@" + m.group(1)
    if text.startswith("@"):
        return text
    return None

def time_str_to_time(t_str: str) -> dtime:
    h, m = map(int, t_str.split(":"))
    return dtime(hour=h, minute=m)

def combine_date_time(d: datetime.date, t: dtime) -> datetime:
    # Локализуем к TZ, чтобы все сравнения/форматирования были консистентны
    naive = datetime.combine(d, t)
    return TZ.localize(naive)

def schedule_to_str(sch: Dict[str, Any]) -> str:
    t = sch.get("type")
    if t == "once":
        return f"One-off at {sch.get('datetime')}"
    if t == "daily":
        return f"Daily at {sch.get('time')}"
    if t == "multiple_daily":
        return f"Multiple daily at {', '.join(sch.get('times', []))}"
    if t == "weekly":
        return f"Weekly on {', '.join(sch.get('days', []))} at {', '.join(sch.get('times', []))}"
    if t == "monthly":
        return f"Monthly on {', '.join(map(str, sch.get('days', [])))} at {', '.join(sch.get('times', []))}"
    if t == "weekdays":
        return f"Weekdays at {sch.get('time')}"
    if t == "weekends":
        return f"Weekends at {sch.get('time')}"
    return json.dumps(sch)

# -------------------- DB --------------------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS chats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            identifier TEXT UNIQUE,
            title TEXT,
            added_at TEXT)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chats TEXT,
            text TEXT,
            file_id TEXT,
            file_type TEXT,
            schedule JSON,
            next_run TEXT,
            enabled INTEGER DEFAULT 1,
            created_by INTEGER,
            created_at TEXT)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS send_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER,
            chat_identifier TEXT,
            status TEXT,
            info TEXT,
            ts TEXT)""")
        await db.execute("""CREATE TABLE IF NOT EXISTS admins (
            user_id TEXT PRIMARY KEY,
            added_at TEXT)""")
        if ADMIN_ID:
            await db.execute("INSERT OR IGNORE INTO admins (user_id, added_at) VALUES (?, ?)",
                             (str(ADMIN_ID), now_trunc_min().isoformat()))
        await db.commit()

async def add_admin(user_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO admins (user_id, added_at) VALUES (?, ?)",
                         (user_id, now_trunc_min().isoformat()))
        await db.commit()

async def remove_admin(user_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM admins WHERE user_id = ?", (user_id,))
        await db.commit()

async def list_admins() -> List[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id FROM admins ORDER BY added_at")
        rows = await cur.fetchall()
        return [r[0] for r in rows]

async def is_admin(user: types.User) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT 1 FROM admins WHERE user_id = ?", (str(user.id),))
        return await cur.fetchone() is not None

async def add_chat(identifier: str, title: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                "INSERT INTO chats (identifier, title, added_at) VALUES (?, ?, ?)",
                (identifier, title, now_trunc_min().isoformat())
            )
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

async def list_chats() -> List[Dict[str, str]]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT identifier, title FROM chats ORDER BY id")
        rows = await cur.fetchall()
        return [{"identifier": r[0], "title": r[1] or r[0]} for r in rows]


async def remove_chat(identifier: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM chats WHERE identifier = ?", (identifier,))
        await db.commit()
        return True

async def count_chats() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM chats")
        r = await cur.fetchone()
        return int(r[0])

async def add_task(chats: List[str], text: str, file_id: Optional[str],
                   file_type: Optional[str], schedule: Dict[str, Any],
                   created_by: int) -> int:
    next_run = compute_next_run_from_schedule(schedule)
    next_run_str = next_run.strftime("%Y-%m-%d %H:%M") if next_run else None
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO tasks (chats, text, file_id, file_type, schedule, next_run, enabled, created_by, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (json.dumps(chats), text, file_id, file_type, json.dumps(schedule),
             next_run_str, 1, created_by, now_trunc_min().isoformat())
        )
        await db.commit()
        return cur.lastrowid

async def update_task_next_run(task_id: int, next_run: Optional[datetime]):
    next_run_str = next_run.strftime("%Y-%m-%d %H:%M") if next_run else None
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE tasks SET next_run = ? WHERE id = ?", (next_run_str, task_id))
        await db.commit()

async def get_task(task_id: int) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id, chats, text, file_id, file_type, schedule, next_run, enabled, created_by FROM tasks WHERE id = ?",
            (task_id,)
        )
        row = await cur.fetchone()
        if not row:
            return None
        return {
            "id": row[0],
            "chats": json.loads(row[1]),
            "text": row[2],
            "file_id": row[3],
            "file_type": row[4],
            "schedule": json.loads(row[5]),
            "next_run": row[6],
            "enabled": bool(row[7]),
            "created_by": row[8]
        }

async def list_tasks_db() -> List[Dict[str, Any]]:
    out = []
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id, chats, text, file_id, file_type, schedule, next_run, enabled FROM tasks ORDER BY id")
        rows = await cur.fetchall()
        for r in rows:
            out.append({
                "id": r[0],
                "chats": json.loads(r[1]),
                "text": r[2],
                "file_id": r[3],
                "file_type": r[4],
                "schedule": json.loads(r[5]),
                "next_run": r[6],
                "enabled": bool(r[7])
            })
    return out

async def delete_task(task_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
        await db.commit()

async def set_task_enabled(task_id: int, enabled: bool):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE tasks SET enabled = ? WHERE id = ?", (1 if enabled else 0, task_id))
        await db.commit()

async def log_send(task_id: int, chat_identifier: str, status: str, info: str = ""):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO send_logs (task_id, chat_identifier, status, info, ts) VALUES (?, ?, ?, ?, ?)",
            (task_id, chat_identifier, status, info, now_trunc_min().isoformat())
        )
        await db.commit()

# -------------------- Кнопка отмены --------------------
def cancel_kb():
    return types.ReplyKeyboardMarkup(
        keyboard=[[types.KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )

main_menu = types.ReplyKeyboardMarkup(
    keyboard=[
        [types.KeyboardButton(text="➕ Добавить чат"), types.KeyboardButton(text="📋 Список чатов")],
        [types.KeyboardButton(text="🆕 Новая задача"), types.KeyboardButton(text="📌 Задачи")],
        [types.KeyboardButton(text="🗑️ Удалить чат")]
    ],
    resize_keyboard=True
)


@dp.message(F.text == "❌ Отмена")
async def cancel_handler(m: types.Message, state: FSMContext):
    await state.clear()
    await m.reply("Действие отменено.", reply_markup=types.ReplyKeyboardRemove())

# -------------------- Scheduler helpers --------------------
def compute_next_run_from_schedule(schedule: Dict[str, Any], base_dt: Optional[datetime] = None) -> Optional[datetime]:
    if base_dt is None:
        base_dt = now_trunc_min()

    ttype = schedule.get("type")

    if ttype == "once":
        dt_str = schedule.get("datetime")
        if not dt_str:
            return None
        # Парсим как локальное время Europe/Moscow
        naive = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
        dt = TZ.localize(naive)
        return dt if dt >= base_dt else None

    if ttype == "daily":
        tm = time_str_to_time(schedule.get("time"))
        candidate = combine_date_time(base_dt.date(), tm)
        return candidate if candidate >= base_dt else candidate + timedelta(days=1)

    if ttype == "multiple_daily":
        times = sorted(schedule.get("times", []))
        for ts in times:
            tm = time_str_to_time(ts)
            cand = combine_date_time(base_dt.date(), tm)
            if cand >= base_dt:
                return cand
        if times:
            tm = time_str_to_time(times[0])
            return combine_date_time(base_dt.date() + timedelta(days=1), tm)
        return None

    if ttype == "weekly":
        day_map = {"mon":0,"tue":1,"wed":2,"thu":3,"fri":4,"sat":5,"sun":6}
        days = schedule.get("days", [])
        times = schedule.get("times", [])
        if not days or not times:
            return None
        candidates = []
        for dname in days:
            wd = day_map.get(dname.lower()[:3])
            if wd is None:
                continue
            for ts in times:
                tm = time_str_to_time(ts)
                days_ahead = (wd - base_dt.weekday()) % 7
                cand_date = base_dt.date() + timedelta(days=days_ahead)
                cand = combine_date_time(cand_date, tm)
                if cand < base_dt:
                    cand += timedelta(weeks=1)
                candidates.append(cand)
        return min(candidates) if candidates else None

    if ttype == "monthly":
        days = schedule.get("days", [])
        times = schedule.get("times", [])
        if not days or not times:
            return None
        candidates = []
        for day in days:
            for ts in times:
                try:
                    tm = time_str_to_time(ts)
                except:
                    continue
                year, month = base_dt.year, base_dt.month
                try:
                    cand = TZ.localize(datetime(year, month, int(day), tm.hour, tm.minute))
                    if cand >= base_dt:
                        candidates.append(cand)
                    else:
                        nxt = (datetime(year, month, 1) + relativedelta(months=1))
                        cand2 = TZ.localize(datetime(nxt.year, nxt.month, int(day), tm.hour, tm.minute))
                        candidates.append(cand2)
                except Exception:
                    # если день не существует в месяце — пробуем дальше
                    for i in range(1, 13):
                        nxt = (datetime(year, month, 1) + relativedelta(months=i))
                        try:
                            cand2 = TZ.localize(datetime(nxt.year, nxt.month, int(day), tm.hour, tm.minute))
                            if cand2 >= base_dt:
                                candidates.append(cand2)
                                break
                        except:
                            continue
        return min(candidates) if candidates else None

    if ttype == "weekdays":
        tm = time_str_to_time(schedule.get("time"))
        cand = combine_date_time(base_dt.date(), tm)
        if base_dt.weekday() < 5 and cand >= base_dt:
            return cand
        for i in range(1, 8):
            d = base_dt + timedelta(days=i)
            if d.weekday() < 5:
                return combine_date_time(d.date(), tm)
        return None

    if ttype == "weekends":
        tm = time_str_to_time(schedule.get("time"))
        cand = combine_date_time(base_dt.date(), tm)
        if base_dt.weekday() >= 5 and cand >= base_dt:
            return cand
        for i in range(1, 8):
            d = base_dt + timedelta(days=i)
            if d.weekday() >= 5:
                return combine_date_time(d.date(), tm)
        return None

    return None

# -------------------- Message sending --------------------
async def send_message_to_chat(chat_identifier: str, text: str,
                                file_id: Optional[str], file_type: Optional[str]) -> (bool, str):
    try:
        if file_id:
            if file_type == "photo":
                await bot.send_photo(chat_identifier, file_id, caption=text or "", parse_mode="HTML")
            elif file_type == "video":
                await bot.send_video(chat_identifier, file_id, caption=text or "", parse_mode="HTML")
            elif file_type == "document":
                await bot.send_document(chat_identifier, file_id, caption=text or "", parse_mode="HTML")
            elif file_type == "audio":
                await bot.send_audio(chat_identifier, file_id, caption=text or "", parse_mode="HTML")
            elif file_type == "voice":
                await bot.send_voice(chat_identifier, file_id, caption=text or "", parse_mode="HTML")
            elif file_type == "sticker":
                await bot.send_sticker(chat_identifier, file_id)
            else:
                await bot.send_message(chat_identifier, text or "", parse_mode="HTML")
        else:
            await bot.send_message(chat_identifier, text or "", parse_mode="HTML")

        return True, "ok"
    except Exception as e:
        return False, str(e)


# -------------------- FSM States --------------------
class NewTask(StatesGroup):
    choosing_source = State()
    choosing_from_list = State()
    entering_manual = State()
    entering_content = State()
    choosing_schedule_type = State()
    entering_once = State()
    entering_daily = State()
    entering_multiple_daily = State()
    entering_weekly = State()
    entering_monthly = State()
    choosing_weekmode = State()
    entering_weekmode_time = State()

class EditTask(StatesGroup):
    choosing_action = State()
    editing_text = State()
    editing_time = State()
    editing_time_type = State() 
    removing_group = State() 
    groups_action = State()      # выбор "➕ Добавить" или "🗑️ Удалить"
    editing_groups = State()     # ввод номеров для добавления
    removing_group = State()


# -------------------- Команды: старт и админы --------------------
@dp.message(Command("start"))
async def cmd_start(m: types.Message):
    await m.reply(
        "Привет! Я бот-рассыльщик.\nВыберите действие:",
        reply_markup=main_menu
    )
@dp.message(F.text == "➕ Добавить чат")
async def btn_addchat(m: types.Message, state: FSMContext):
    await cmd_addchat(m, state)

@dp.message(F.text == "📋 Список чатов")
async def btn_chats(m: types.Message):
    await cmd_chats(m)

@dp.message(F.text == "🆕 Новая задача")
async def btn_newtask(m: types.Message, state: FSMContext):
    await cmd_newtask(m, state)

@dp.message(F.text == "📌 Задачи")
async def btn_tasks(m: types.Message):
    await cmd_tasks(m)

@dp.message(F.text == "🗑️ Удалить чат")
async def btn_removechat(m: types.Message, state: FSMContext):
    await cmd_removechat(m, state)


@dp.message(Command("addadmin"))
async def cmd_addadmin(m: types.Message):
    if not await is_admin(m.from_user):
        return await m.reply("Только администратор может добавлять других админов.")
    parts = m.text.strip().split()
    if len(parts) != 2:
        return await m.reply("Использование: /addadmin user_id")
    user_id = parts[1]
    await add_admin(user_id)
    await m.reply(f"Администратор {user_id} добавлен.")

@dp.message(Command("removeadmin"))
async def cmd_removeadmin(m: types.Message):
    if not await is_admin(m.from_user):
        return await m.reply("Только администратор может удалять админов.")
    parts = m.text.strip().split()
    if len(parts) != 2:
        return await m.reply("Использование: /removeadmin user_id")
    user_id = parts[1]
    await remove_admin(user_id)
    await m.reply(f"Администратор {user_id} удалён.")

@dp.message(Command("admins"))
async def cmd_admins(m: types.Message):
    if not await is_admin(m.from_user):
        return await m.reply("Только администратор.")
    admins = await list_admins()
    if not admins:
        return await m.reply("Список админов пуст.")
    await m.reply("Администраторы:\n" + "\n".join(admins))


@dp.message(Command("help"))
async def cmd_help(m: types.Message):
    await cmd_start(m)

# -------------------- States для чатов --------------------
class ChatStates(StatesGroup):
    entering_identifier = State()
    entering_title = State()
    removing_identifier = State()

# -------------------- Добавление чата --------------------
@dp.message(Command("addchat"))
async def cmd_addchat(m: types.Message, state: FSMContext):
    if not await is_admin(m.from_user):
        return await m.reply("Только администратор.")
    await m.reply("Отправь идентификатор чата: @username, https://t.me/username или -100... (ID)",
                  reply_markup=cancel_kb())
    await state.set_state(ChatStates.entering_identifier)

@dp.message(ChatStates.entering_identifier)
async def handle_addchat_identifier(m: types.Message, state: FSMContext):
    parsed = parse_chat_identifier(m.text.strip())
    if not parsed:
        return await m.reply("Не распознал идентификатор. Попробуй ещё раз.", reply_markup=cancel_kb())
    await state.update_data(identifier=parsed)
    await m.reply("Теперь введи название для этого чата (например: 'Рабочая группа').",
                  reply_markup=cancel_kb())
    await state.set_state(ChatStates.entering_title)

@dp.message(ChatStates.entering_title)
async def handle_addchat_title(m: types.Message, state: FSMContext):
    data = await state.get_data()
    identifier = data["identifier"]
    title = m.text.strip()
    ok = await add_chat(identifier, title)
    await m.reply(f"Чат {title} ({identifier}) {'добавлен' if ok else 'уже есть'}",
                  reply_markup=types.ReplyKeyboardRemove())
    await state.clear()

# -------------------- Список чатов --------------------
@dp.message(Command("chats"))
async def cmd_chats(m: types.Message):
    if not await is_admin(m.from_user):
        return await m.reply("Только администратор.")
    chats = await list_chats()
    if not chats:
        return await m.reply("Список чатов пуст.")
    await m.reply("Список чатов:\n\n" + "\n".join(
        f"{i+1}. {c['title']} ({c['identifier']})" for i, c in enumerate(chats)
    ))

# -------------------- Удаление чата --------------------
@dp.message(Command("removechat"))
async def cmd_removechat(m: types.Message, state: FSMContext):
    if not await is_admin(m.from_user):
        return await m.reply("Только администратор.")

    chats = await list_chats()
    if not chats:
        return await m.reply("Список чатов пуст.")

    # Сохраняем список в state, чтобы потом по номеру найти
    await state.update_data(all_chats=chats)

    text = "Список чатов:\n" + "\n".join(
        f"{i+1}. {c['title']} ({c['identifier']})" for i, c in enumerate(chats)
    ) + "\n\nОтправь номер чата для удаления."
    await m.reply(text, reply_markup=cancel_kb())
    await state.set_state(ChatStates.removing_identifier)


@dp.message(ChatStates.removing_identifier)
async def handle_removechat(m: types.Message, state: FSMContext):
    data = await state.get_data()
    chats = data.get("all_chats", [])

    if not m.text.isdigit():
        return await m.reply("Нужно ввести номер из списка.", reply_markup=cancel_kb())

    idx = int(m.text) - 1
    if idx < 0 or idx >= len(chats):
        return await m.reply("Некорректный номер.", reply_markup=cancel_kb())

    chat = chats[idx]
    await remove_chat(chat["identifier"])
    await m.reply(f"Удалён: {chat['title']} ({chat['identifier']})",
                  reply_markup=types.ReplyKeyboardRemove())
    await state.clear()



# -------------------- New task creation flow (FSM) --------------------
@dp.message(Command("newtask"))
async def cmd_newtask(m: types.Message, state: FSMContext):
    if not await is_admin(m.from_user):
        return await m.reply("Только администратор.")
    kb = types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="Выбрать из добавленных")],
            [types.KeyboardButton(text="Ввести вручную")],
            [types.KeyboardButton(text="❌ Отмена")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await m.reply("Создание новой задачи. Выбери способ:", reply_markup=kb)
    await state.set_state(NewTask.choosing_source)

@dp.message(NewTask.choosing_source)
async def newtask_choose_source(m: types.Message, state: FSMContext):
    if m.text == "Выбрать из добавленных":
        chats = await list_chats()
        if not chats:
            await m.reply("Список добавленных пуст. Сначала добавь чаты через /addchat.",
                          reply_markup=types.ReplyKeyboardRemove())
            await state.clear()
            return
        await state.update_data(all_chats=chats)
        text = "Список чатов:\n" + "\n".join(f"{i+1}. {c['title']}" for i, c in enumerate(chats))
        text += "\nОтправь номера через запятую (например: 1,2,5) или 'all'."
        await m.reply(text, reply_markup=cancel_kb())
        await state.set_state(NewTask.choosing_from_list)
    elif m.text == "Ввести вручную":
        await m.reply("Введи идентификаторы через запятую (например: @a, -100123, https://t.me/b)",
                      reply_markup=cancel_kb())
        await state.set_state(NewTask.entering_manual)
        await state.update_data(mode="newtask_manual")
    else:
        await m.answer("Ок", reply_markup=types.ReplyKeyboardRemove())
        await m.reply("Неверный выбор. Отмена.")
        await state.clear()

@dp.message(NewTask.choosing_from_list)
async def newtask_chats_selected_from_list(m: types.Message, state: FSMContext):
    raw = m.text.strip()
    data = await state.get_data()
    chats = data.get("all_chats", [])
    selected = []
    if raw.lower() == "all":
        selected = [c["identifier"] for c in chats]
    else:
        parts = [p.strip() for p in raw.split(",")]
        for p in parts:
            if p.isdigit():
                idx = int(p) - 1
                if 0 <= idx < len(chats):
                    selected.append(chats[idx]["identifier"])
            elif "-" in p:
                try:
                    a, b = map(int, p.split("-", 1))
                    for i in range(a - 1, b):
                        if 0 <= i < len(chats):
                            selected.append(chats[i]["identifier"])
                except:
                    pass

    if not selected:
        await m.reply("Не выбрано ни одного чата. Отмена.", reply_markup=types.ReplyKeyboardRemove())
        await state.clear()
        return

    await state.update_data(chats=selected)   # теперь список строк

    await m.reply("Теперь пришли текст сообщения или отправь медиа. Подпись к медиа станет текстом.",
                  reply_markup=cancel_kb())
    await state.set_state(NewTask.entering_content)

@dp.message(NewTask.entering_content)
async def newtask_get_content(m: types.Message, state: FSMContext):
    file_id, file_type = None, None
    text = m.caption if m.caption else (m.text if m.text else "")
    if m.photo:
        file_id = m.photo[-1].file_id; file_type = "photo"
    elif m.video:
        file_id = m.video.file_id; file_type = "video"
    elif m.document:
        file_id = m.document.file_id; file_type = "document"
    elif m.audio:
        file_id = m.audio.file_id; file_type = "audio"
    elif m.voice:
        file_id = m.voice.file_id; file_type = "voice"
    elif m.sticker:
        file_id = m.sticker.file_id
        file_type = "sticker"
    await state.update_data(text=text, file_id=file_id, file_type=file_type)
    kb = types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="Разово (один раз)")],
            [types.KeyboardButton(text="Ежедневно")],
            [types.KeyboardButton(text="Несколько раз в день")],
            [types.KeyboardButton(text="Еженедельно")],
            [types.KeyboardButton(text="Ежемесячно")],
            [types.KeyboardButton(text="Будни / Выходные")],
            [types.KeyboardButton(text="❌ Отмена")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await m.reply("Выбери режим отправки:", reply_markup=kb)
    await state.set_state(NewTask.choosing_schedule_type)

@dp.message(NewTask.choosing_schedule_type)
async def newtask_choose_schedule_type(m: types.Message, state: FSMContext):
    t = m.text
    if t == "Разово (один раз)":
        await m.reply("Введите дату и время: YYYY-MM-DD HH:MM (например: 2025-10-30 18:30)",
                      reply_markup=cancel_kb())
        await state.set_state(NewTask.entering_once)
    elif t == "Ежедневно":
        await m.reply("Введите время: HH:MM (например: 09:00)", reply_markup=cancel_kb())
        await state.set_state(NewTask.entering_daily)
    elif t == "Несколько раз в день":
        await m.reply("Введите времена через запятую: HH:MM,HH:MM (например: 09:00,13:30,20:00)",
                      reply_markup=cancel_kb())
        await state.set_state(NewTask.entering_multiple_daily)
    elif t == "Еженедельно":
        await m.reply("Введите дни недели и время. Пример: mon,wed,fri 09:00\nДни: mon,tue,wed,thu,fri,sat,sun",
                      reply_markup=cancel_kb())
        await state.set_state(NewTask.entering_weekly)
    elif t == "Ежемесячно":
        await m.reply("Введите числа месяца и время. Пример: 1,15 09:00", reply_markup=cancel_kb())
        await state.set_state(NewTask.entering_monthly)
    elif t == "Будни / Выходные":
        kb = types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="Будни (Mon-Fri)")],
                [types.KeyboardButton(text="Выходные (Sat-Sun)")],
                [types.KeyboardButton(text="❌ Отмена")],
            ],
            resize_keyboard=True,
            one_time_keyboard=True
        )
        await m.reply("Выбери вариант:", reply_markup=kb)
        await state.set_state(NewTask.choosing_weekmode)
    else:
        await m.reply("Неверный выбор. Отмена.", reply_markup=types.ReplyKeyboardRemove())
        await state.clear()

@dp.message(NewTask.entering_once)
async def newtask_schedule_once(m: types.Message, state: FSMContext):
    txt = m.text.strip()
    try:
        naive = datetime.strptime(txt, "%Y-%m-%d %H:%M")
        dt = TZ.localize(naive)
    except:
        await m.reply("Неверный формат. Попробуйте ещё раз.", reply_markup=cancel_kb())
        return
    # храним строку в локальном формате (YYYY-MM-DD HH:MM)
    schedule = {"type": "once", "datetime": dt.strftime("%Y-%m-%d %H:%M")}
    await finalize_newtask(m, state, schedule)

@dp.message(NewTask.entering_daily)
async def newtask_schedule_daily(m: types.Message, state: FSMContext):
    txt = m.text.strip()
    try:
        _ = time_str_to_time(txt)
    except:
        await m.reply("Неверный формат времени.", reply_markup=cancel_kb())
        return
    schedule = {"type": "daily", "time": txt}
    await finalize_newtask(m, state, schedule)

@dp.message(NewTask.entering_multiple_daily)
async def newtask_schedule_multiple_daily(m: types.Message, state: FSMContext):
    parts = [p.strip() for p in m.text.split(",") if p.strip()]
    try:
        for p in parts:
            _ = time_str_to_time(p)
    except:
        await m.reply("Неверный формат времен.", reply_markup=cancel_kb())
        return
    schedule = {"type": "multiple_daily", "times": parts}
    await finalize_newtask(m, state, schedule)

@dp.message(NewTask.entering_weekly)
async def newtask_schedule_weekly(m: types.Message, state: FSMContext):
    try:
        days_part, time_part = m.text.split()
        days = [d.strip().lower() for d in days_part.split(",") if d.strip()]
        _ = time_str_to_time(time_part.strip())
        schedule = {"type": "weekly", "days": days, "times": [time_part.strip()]}
        await finalize_newtask(m, state, schedule)
    except:
        await m.reply("Неверный формат. Пример: mon,wed,fri 09:00", reply_markup=cancel_kb())

@dp.message(NewTask.entering_monthly)
async def newtask_schedule_monthly(m: types.Message, state: FSMContext):
    try:
        days_part, time_part = m.text.split()
        days = [int(x.strip()) for x in days_part.split(",") if x.strip()]
        _ = time_str_to_time(time_part.strip())
        schedule = {"type": "monthly", "days": days, "times": [time_part.strip()]}
        await finalize_newtask(m, state, schedule)
    except:
        await m.reply("Неверный формат. Пример: 1,15 09:00", reply_markup=cancel_kb())

@dp.message(NewTask.choosing_weekmode)
async def newtask_schedule_weekdays_weekends(m: types.Message, state: FSMContext):
    choice = m.text.strip()
    if choice == "Будни (Mon-Fri)":
        await state.update_data(weekmode="weekdays")
        await m.reply("Введите время: HH:MM", reply_markup=cancel_kb())
        await state.set_state(NewTask.entering_weekmode_time)
    elif choice == "Выходные (Sat-Sun)":
        await state.update_data(weekmode="weekends")
        await m.reply("Введите время: HH:MM", reply_markup=cancel_kb())
        await state.set_state(NewTask.entering_weekmode_time)
    else:
        await m.reply("Неверный выбор. Отмена.", reply_markup=types.ReplyKeyboardRemove())
        await state.clear()

@dp.message(NewTask.entering_weekmode_time)
async def finalize_newtask_from_weekmode(m: types.Message, state: FSMContext):
    txt = m.text.strip()
    try:
        _ = time_str_to_time(txt)
    except:
        await m.reply("Неверный формат времени.", reply_markup=cancel_kb())
        return
    data = await state.get_data()
    mode = data.get("weekmode")
    schedule = {"type": mode, "time": txt}
    await finalize_newtask(m, state, schedule)

async def finalize_newtask(m: types.Message, state: FSMContext, schedule: Dict[str, Any]):
    data = await state.get_data()
    chats = data.get("chats", [])
    text = data.get("text", "")
    file_id = data.get("file_id", None)
    file_type = data.get("file_type", None)
    created_by = m.from_user.id

    if not chats:
        await m.reply("Чаты для рассылки не выбраны. Отмена.", reply_markup=types.ReplyKeyboardRemove())
        await state.clear()
        return

    editing_id = data.get("editing_task_id")
    if editing_id:
        # пересчёт next_run
        next_run = compute_next_run_from_schedule(schedule)
        next_run_str = next_run.strftime("%Y-%m-%d %H:%M") if next_run else None

        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE tasks SET chats=?, text=?, file_id=?, file_type=?, schedule=?, next_run=? WHERE id=?",
                (json.dumps(chats), text, file_id, file_type, json.dumps(schedule), next_run_str, editing_id)
            )
            await db.commit()

        msg = f"Задача #{editing_id} обновлена.\n"
        task_id = editing_id
    else:
        task_id = await add_task(chats, text, file_id, file_type, schedule, created_by)
        msg = f"Задача #{task_id} создана.\n"
        next_run = compute_next_run_from_schedule(schedule)
        next_run_str = next_run.strftime("%Y-%m-%d %H:%M") if next_run else "—"




    await m.reply(
        msg +
        f"Расписание: {schedule_to_str(schedule)}\n"
        f"Следующий запуск: {next_run_str}",
        reply_markup=types.ReplyKeyboardRemove()
    )
    await state.clear()



# -------------------- Tasks: список и действия --------------------
@dp.message(Command("tasks"))
async def cmd_tasks(m: types.Message):
    if not await is_admin(m.from_user):
        return await m.reply("Только администратор.")

    tasks = await list_tasks_db()
    if not tasks:
        return await m.reply("Нет задач.")

    # Загружаем список чатов для отображения названий
    all_chats = await list_chats()
    title_by_id = {c["identifier"]: (c["title"] or c["identifier"]) for c in all_chats}

    for t in tasks:
        # преобразуем идентификаторы в названия
        chat_titles = [title_by_id.get(cid, cid) for cid in t["chats"]]

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit:{t['id']}"),
             InlineKeyboardButton(text="🗑 Удалить", callback_data=f"delete:{t['id']}")],
            [InlineKeyboardButton(text="🚀 Отправить сейчас", callback_data=f"sendnow:{t['id']}"),
             InlineKeyboardButton(text=("⏸ Включено" if t['enabled'] else "▶️ Включить"),
                                  callback_data=f"toggle:{t['id']}")]
        ])

        info = (
            f"ID: {t['id']}\n"
            f"Чаты: {', '.join(chat_titles)}\n"
            f"Текст: {t['text'][:200] + ('...' if len(t['text']) > 200 else '')}\n"
            f"Медиа: {'Да' if t['file_id'] else 'Нет'}\n"
            f"Schedule: {schedule_to_str(t['schedule'])}\n"
            f"Next run: {t['next_run']}\n"
            f"Enabled: {t['enabled']}"
        )

        await m.reply(info, reply_markup=kb)


@dp.callback_query(F.data.startswith("delete:"))
async def cb_delete(call: types.CallbackQuery):
    task_id = int(call.data.split(":", 1)[1])
    await delete_task(task_id)
    await call.message.edit_text(f"Задача {task_id} удалена.")
    await call.answer("Удалено")

@dp.callback_query(F.data.startswith("toggle:"))
async def cb_toggle(call: types.CallbackQuery):
    task_id = int(call.data.split(":", 1)[1])
    task = await get_task(task_id)
    if not task:
        return await call.answer("Задача не найдена", show_alert=True)
    await set_task_enabled(task_id, not task["enabled"])
    await call.answer("Статус изменён")
    await call.message.edit_text(f"Задача {task_id} статус изменён. (перезапустите /tasks для обновления списка)")

@dp.callback_query(F.data.startswith("sendnow:"))
async def cb_sendnow(call: types.CallbackQuery):
    task_id = int(call.data.split(":", 1)[1])
    task = await get_task(task_id)
    if not task:
        return await call.answer("Задача не найдена", show_alert=True)
    success, failed = 0, 0
    for ch in task["chats"]:
        ok, info = await send_message_to_chat(ch, task["text"], task["file_id"], task["file_type"])
        if ok:
            success += 1
            await log_send(task_id, ch, "ok", info)
        else:
            failed += 1
            await log_send(task_id, ch, "error", info)
    await call.message.answer(f"Отправлено: {success}, Ошибок: {failed}")
    await call.answer("Отправлено сейчас")

@dp.callback_query(F.data.startswith("edit:"))
async def cb_edit_task(call: types.CallbackQuery, state: FSMContext):
    task_id = int(call.data.split(":", 1)[1])
    task = await get_task(task_id)
    if not task:
        return await call.answer("Задача не найдена", show_alert=True)

    await state.update_data(
        editing_task_id=task_id,
        chats=task["chats"],
        text=task["text"],
        file_id=task["file_id"],
        file_type=task["file_type"],
        schedule=task["schedule"]
    )

    kb = types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="📝 Редактировать текст")],
            [types.KeyboardButton(text="⏰ Редактировать время")],
            [types.KeyboardButton(text="👥 Редактировать группы")],
            [types.KeyboardButton(text="❌ Отмена")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )

    await call.message.answer("Что именно редактируем?", reply_markup=kb)
    await state.set_state(EditTask.choosing_action)
    await call.answer()

@dp.message(EditTask.choosing_action)
async def edit_choose_action(m: types.Message, state: FSMContext):
    choice = m.text.strip()
    if choice == "📝 Редактировать текст":
        await m.reply("Пришли новый текст или медиа для задачи (подпись к медиа станет текстом).",
                      reply_markup=cancel_kb())
        await state.set_state(EditTask.editing_text)

    elif choice == "⏰ Редактировать время":
        kb = types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="Разово (один раз)")],
                [types.KeyboardButton(text="Ежедневно")],
                [types.KeyboardButton(text="Несколько раз в день")],
                [types.KeyboardButton(text="Еженедельно")],
                [types.KeyboardButton(text="Ежемесячно")],
                [types.KeyboardButton(text="Будни / Выходные")],
                [types.KeyboardButton(text="❌ Отмена")],
            ],
            resize_keyboard=True,
            one_time_keyboard=True
        )
        await m.reply("Выбери тип расписания:", reply_markup=kb)
        await state.set_state(EditTask.editing_time_type)


    elif choice == "👥 Редактировать группы":
        kb = types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="➕ Добавить группы")],
                [types.KeyboardButton(text="🗑️ Удалить группу")],
                [types.KeyboardButton(text="❌ Отмена")],
            ],
            resize_keyboard=True,
            one_time_keyboard=True
        )
        await m.reply("Выберите действие с группами:", reply_markup=kb)
        await state.set_state(EditTask.groups_action)


    else:
        await m.reply("Редактирование отменено.", reply_markup=types.ReplyKeyboardRemove())
        await state.clear()

@dp.message(EditTask.editing_text)
async def edit_task_text(m: types.Message, state: FSMContext):
    data = await state.get_data()
    task_id = data["editing_task_id"]

    file_id, file_type = None, None
    text = m.caption if m.caption else (m.text if m.text else "")

    if m.photo:
        file_id = m.photo[-1].file_id; file_type = "photo"
    elif m.video:
        file_id = m.video.file_id; file_type = "video"
    elif m.document:
        file_id = m.document.file_id; file_type = "document"
    elif m.audio:
        file_id = m.audio.file_id; file_type = "audio"
    elif m.voice:
        file_id = m.voice.file_id; file_type = "voice"
    elif m.sticker:
        file_id = m.sticker.file_id; file_type = "sticker"
        text = ""

    # Если пользователь не прислал ни текст ни медиа — оставим как было
    if not text and not file_id and not file_type:
        await m.reply("Контент не изменён. Оставляю прежние текст/медиа.",
                      reply_markup=types.ReplyKeyboardRemove())
        return await state.clear()

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE tasks SET text=?, file_id=?, file_type=? WHERE id=?",
                         (text, file_id, file_type, task_id))
        await db.commit()

    await m.reply("Текст/медиа задачи обновлены ✅", reply_markup=types.ReplyKeyboardRemove())
    await state.clear()

@dp.message(EditTask.editing_time_type)
async def edit_task_time_type(m: types.Message, state: FSMContext):
    t = m.text.strip()
    if t == "Разово (один раз)":
        await m.reply("Введите дату и время: YYYY-MM-DD HH:MM", reply_markup=cancel_kb())
        await state.set_state(EditTask.editing_time)
    elif t == "Ежедневно":
        await m.reply("Введите время: HH:MM", reply_markup=cancel_kb())
        await state.set_state(EditTask.editing_time)
    # и так далее для остальных вариантов...
    else:
        await m.reply("Неверный выбор. Отмена.", reply_markup=types.ReplyKeyboardRemove())
        await state.clear()


@dp.message(EditTask.editing_time)
async def edit_task_time(m: types.Message, state: FSMContext):
    task_id = (await state.get_data())["editing_task_id"]
    txt = m.text.strip()
    schedule = None

    try:
        if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$", txt):
            naive = datetime.strptime(txt, "%Y-%m-%d %H:%M")
            dt = TZ.localize(naive)
            schedule = {"type": "once", "datetime": dt.strftime("%Y-%m-%d %H:%M")}
        elif re.match(r"^\d{2}:\d{2}$", txt):
            _ = time_str_to_time(txt)
            schedule = {"type": "daily", "time": txt}
        elif "," in txt and all(re.match(r"^\d{2}:\d{2}$", p.strip()) for p in txt.split(",")):
            parts = [p.strip() for p in txt.split(",") if p.strip()]
            for p in parts:
                _ = time_str_to_time(p)
            schedule = {"type": "multiple_daily", "times": parts}
        elif " " in txt and any(d in txt.lower() for d in ["mon","tue","wed","thu","fri","sat","sun"]):
            days_part, time_part = txt.split()
            days = [d.strip().lower() for d in days_part.split(",") if d.strip()]
            _ = time_str_to_time(time_part.strip())
            schedule = {"type": "weekly", "days": days, "times": [time_part.strip()]}
        elif " " in txt and any(ch.isdigit() for ch in txt):
            days_part, time_part = txt.split()
            days = [int(x.strip()) for x in days_part.split(",") if x.strip()]
            _ = time_str_to_time(time_part.strip())
            schedule = {"type": "monthly", "days": days, "times": [time_part.strip()]}
        elif txt.lower().startswith("weekdays") or txt.lower().startswith("weekends"):
            parts = txt.split()
            if len(parts) == 2:
                mode, t = parts
                _ = time_str_to_time(t.strip())
                schedule = {"type": mode.lower(), "time": t.strip()}
    except Exception:
        await m.reply("Неверный формат. Попробуйте снова.", reply_markup=cancel_kb())
        return

    if not schedule:
        await m.reply("Неверный формат. Попробуйте снова.", reply_markup=cancel_kb())
        return

    next_run = compute_next_run_from_schedule(schedule)
    next_run_str = next_run.strftime("%Y-%m-%d %H:%M") if next_run else None

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE tasks SET schedule=?, next_run=? WHERE id=?",
                         (json.dumps(schedule), next_run_str, task_id))
        await db.commit()

    await m.reply("Расписание обновлено ✅", reply_markup=types.ReplyKeyboardRemove())
    await state.clear()


@dp.message(EditTask.groups_action)
async def edit_task_groups(m: types.Message, state: FSMContext):
    if m.text == "➕ Добавить группы":
        # получаем все сохранённые чаты из базы
        chats = await list_chats()
        if not chats:
            await m.reply("Список чатов пуст. Добавьте чаты через /addchat.",
                          reply_markup=cancel_kb())
            return

        text = "Сохранённые группы:\n" + "\n".join(
            f"{i+1}. {c['title']} ({c['identifier']})" for i, c in enumerate(chats)
        )
        text += "\n\nВведите номера через запятую (например: 1,2,5) или 'all'."

        # сохраняем список в state, чтобы потом обработать выбор
        await state.update_data(all_chats=chats)
        await m.reply(text, reply_markup=cancel_kb())
        # остаёмся в том же состоянии для обработки ввода номеров
        await state.set_state(EditTask.editing_groups)

    elif m.text == "🗑️ Удалить группу":
        data = await state.get_data()
        task_id = data["editing_task_id"]
        task = await get_task(task_id)
        chats = task["chats"]

        text = "Список групп задачи:\n" + "\n".join(f"{i+1}. {c}" for i, c in enumerate(chats))
        text += "\n\nОтправь номер группы для удаления."
        await m.reply(text, reply_markup=cancel_kb())
        await state.set_state(EditTask.removing_group)

    else:
        await m.reply("Отмена.", reply_markup=types.ReplyKeyboardRemove())
        await state.clear()

@dp.message(EditTask.editing_groups)
async def add_groups_to_task(m: types.Message, state: FSMContext):
    data = await state.get_data()
    chats = data.get("all_chats", [])
    task_id = data.get("editing_task_id")

    text = m.text.strip()
    if text.lower() == "all":
        selected = [c["identifier"] for c in chats]
    else:
        try:
            nums = [int(x.strip()) for x in text.split(",")]
            selected = [chats[i-1]["identifier"] for i in nums if 0 < i <= len(chats)]
        except Exception:
            await m.reply("Некорректный ввод. Введите номера через запятую или 'all'.")
            return

    # обновляем задачу
    task = await get_task(task_id)
    new_chats = list(set(task["chats"] + selected))

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE tasks SET chats=? WHERE id=?",
                         (json.dumps(new_chats), task_id))
        await db.commit()

    await m.reply("Группы обновлены ✅", reply_markup=types.ReplyKeyboardRemove())
    await state.clear()


@dp.message(EditTask.removing_group)
async def remove_group_from_task(m: types.Message, state: FSMContext):
    if not m.text.isdigit():
        await m.reply("Нужно ввести номер из списка.", reply_markup=cancel_kb())
        return

    idx = int(m.text) - 1
    data = await state.get_data()
    task_id = data["editing_task_id"]
    task = await get_task(task_id)
    chats = task["chats"]

    if idx < 0 or idx >= len(chats):
        await m.reply("Некорректный номер.", reply_markup=cancel_kb())
        return

    removed = chats.pop(idx)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE tasks SET chats=? WHERE id=?",
                         (json.dumps(chats), task_id))
        await db.commit()

    await m.reply(f"Группа {removed} удалена ✅", reply_markup=types.ReplyKeyboardRemove())
    await state.clear()


# -------------------- Планировщик --------------------
async def scheduler_loop():
    while True:
        try:
            now = now_trunc_min()
            async with aiosqlite.connect(DB_PATH) as db:
                cur = await db.execute(
                    "SELECT id, chats, text, file_id, file_type, schedule, next_run FROM tasks "
                    "WHERE enabled = 1 AND next_run IS NOT NULL"
                )
                rows = await cur.fetchall()
                for r in rows:
                    task_id = r[0]
                    chats = json.loads(r[1])
                    text = r[2]
                    file_id = r[3]
                    file_type = r[4]
                    schedule = json.loads(r[5])
                    next_run_str = r[6]
                    if not next_run_str:
                        continue
                    try:
                        next_run_dt = TZ.localize(datetime.strptime(next_run_str, "%Y-%m-%d %H:%M"))
                    except:
                        continue
                    if next_run_dt <= now:
                        for ch in chats:
                            ok, info = await send_message_to_chat(ch, text, file_id, file_type)
                            await log_send(task_id, ch, "ok" if ok else "error", info)
                        ttype = schedule.get("type")
                        if ttype == "once":
                            await update_task_next_run(task_id, None)
                            await set_task_enabled(task_id, False)
                        else:
                            nxt = compute_next_run_from_schedule(schedule,
                                                                 base_dt=next_run_dt + timedelta(minutes=1))
                            await update_task_next_run(task_id, nxt)
            await asyncio.sleep(20)
        except Exception as ex:
            print("Scheduler error:", ex)
            await asyncio.sleep(5)

# -------------------- Run --------------------
async def main():
    await init_db()
    print("DB initialized.")
    asyncio.create_task(scheduler_loop())
    print("Scheduler started.")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user")


