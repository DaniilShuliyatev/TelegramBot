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

# -------------------- Настройки --------------------
load_dotenv()

API_TOKEN = os.getenv("API_TOKEN")
ADMIN_ID = os.getenv("ADMIN_ID")  # первичный админ (optional)
DB_PATH = os.getenv("DB_PATH", "scheduler.db")

if not API_TOKEN:
    raise RuntimeError("API_TOKEN not set in environment (.env)")

# Без parse_mode="HTML", чтобы не ловить ошибки на <...>
bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# -------------------- Утилиты --------------------
def now_trunc_min() -> datetime:
    return datetime.now().replace(second=0, microsecond=0)

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
    return datetime.combine(d, t)

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
                             (str(ADMIN_ID), datetime.now().isoformat()))
        await db.commit()

async def add_admin(user_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO admins (user_id, added_at) VALUES (?, ?)",
                         (user_id, datetime.now().isoformat()))
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

async def add_chat(identifier: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                "INSERT INTO chats (identifier, added_at) VALUES (?, ?)",
                (identifier, datetime.now().isoformat())
            )
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

async def list_chats() -> List[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT identifier FROM chats ORDER BY id")
        rows = await cur.fetchall()
        return [r[0] for r in rows]

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
             next_run_str, 1, created_by, datetime.now().isoformat())
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
            (task_id, chat_identifier, status, info, datetime.now().isoformat())
        )
        await db.commit()

# -------------------- Scheduler helpers --------------------
def compute_next_run_from_schedule(schedule: Dict[str, Any], base_dt: Optional[datetime] = None) -> Optional[datetime]:
    if base_dt is None:
        base_dt = now_trunc_min()

    ttype = schedule.get("type")

    if ttype == "once":
        dt_str = schedule.get("datetime")
        if not dt_str:
            return None
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
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
                    cand = datetime(year, month, int(day), tm.hour, tm.minute)
                    if cand >= base_dt:
                        candidates.append(cand)
                    else:
                        nxt = (datetime(year, month, 1) + relativedelta(months=1))
                        cand2 = datetime(nxt.year, nxt.month, int(day), tm.hour, tm.minute)
                        candidates.append(cand2)
                except Exception:
                    for i in range(1, 13):
                        nxt = (datetime(year, month, 1) + relativedelta(months=i))
                        try:
                            cand2 = datetime(nxt.year, nxt.month, int(day), tm.hour, tm.minute)
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
                await bot.send_photo(chat_identifier, file_id, caption=text or "")
            elif file_type == "video":
                await bot.send_video(chat_identifier, file_id, caption=text or "")
            elif file_type == "document":
                await bot.send_document(chat_identifier, file_id, caption=text or "")
            elif file_type == "audio":
                await bot.send_audio(chat_identifier, file_id, caption=text or "")
            elif file_type == "voice":
                await bot.send_voice(chat_identifier, file_id, caption=text or "")
            else:
                await bot.send_message(chat_identifier, text or "")
        else:
            await bot.send_message(chat_identifier, text or "")
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

# -------------------- Команды: старт и админы --------------------
@dp.message(Command("start"))
async def cmd_start(m: types.Message):
    await m.reply(
        "Привет! Я бот-рассыльщик.\n"
        "Команды (админы):\n"
        "/addchat — добавить чат\n"
        "/chats — список чатов\n"
        "/removechat — удалить чат\n"
        "/newtask — создать задачу\n"
        "/tasks — показать задачи\n"
        "/addadmin user_id — добавить администратора\n"
        "/removeadmin user_id — убрать администратора\n"
        "/admins — список админов"
    )

@dp.message(Command("help"))
async def cmd_help(m: types.Message):
    await cmd_start(m)

@dp.message(Command("addadmin"))
async def cmd_addadmin(m: types.Message):
    if not await is_admin(m.from_user):
        return await m.reply("Нет прав.")
    parts = m.text.split()
    if len(parts) < 2:
        return await m.reply("Использование: /addadmin user_id")
    await add_admin(parts[1])
    await m.reply(f"Пользователь {parts[1]} теперь администратор.")

@dp.message(Command("removeadmin"))
async def cmd_removeadmin(m: types.Message):
    if not await is_admin(m.from_user):
        return await m.reply("Нет прав.")
    parts = m.text.split()
    if len(parts) < 2:
        return await m.reply("Использование: /removeadmin user_id")
    await remove_admin(parts[1])
    await m.reply(f"Пользователь {parts[1]} больше не администратор.")

@dp.message(Command("admins"))
async def cmd_admins(m: types.Message):
    if not await is_admin(m.from_user):
        return await m.reply("Нет прав.")
    admins = await list_admins()
    if not admins:
        return await m.reply("Список администраторов пуст.")
    await m.reply("Администраторы:\n" + "\n".join(admins))

# -------------------- Команды: чаты --------------------
@dp.message(Command("addchat"))
async def cmd_addchat(m: types.Message, state: FSMContext):
    if not await is_admin(m.from_user):
        return await m.reply("Только администратор.")
    await m.reply("Отправь идентификатор чата: @username, https://t.me/username или -100... (ID)")
    await state.set_state(NewTask.entering_manual)
    await state.update_data(mode="addchat")

@dp.message(NewTask.entering_manual)
async def handle_addchat_or_manual(m: types.Message, state: FSMContext):
    data = await state.get_data()
    mode = data.get("mode")
    if mode == "addchat":
        parsed = parse_chat_identifier(m.text.strip())
        if not parsed:
            return await m.reply("Не распознал идентификатор. Попробуй ещё раз.")
        cnt = await count_chats()
        if cnt >= 150:
            await m.reply("Достигнут лимит 150 чатов.")
            await state.clear()
            return
        ok = await add_chat(parsed)
        await m.reply("Добавлен" if ok else "Уже есть")
        await state.clear()
        return
    # ручной ввод для newtask
    parts = [p.strip() for p in m.text.split(",")]
    parsed_list = []
    for p in parts:
        pp = parse_chat_identifier(p)
        if pp:
            parsed_list.append(pp)
    if not parsed_list:
        return await m.reply("Не распознал чаты. Попробуй ещё раз.")
    if len(parsed_list) > 150:
        return await m.reply("Нельзя более 150 чатов.")
    await state.update_data(chats=parsed_list)
    await m.reply("Теперь пришли текст сообщения или отправь медиа. Подпись к медиа станет текстом.")
    await state.set_state(NewTask.entering_content)

@dp.message(Command("chats"))
async def cmd_chats(m: types.Message):
    if not await is_admin(m.from_user):
        return await m.reply("Только администратор.")
    chats = await list_chats()
    if not chats:
        return await m.reply("Список чатов пуст.")
    await m.reply("Список чатов:\n\n" + "\n".join(f"{i+1}. {c}" for i, c in enumerate(chats)))

@dp.message(Command("removechat"))
async def cmd_removechat(m: types.Message, state: FSMContext):
    if not await is_admin(m.from_user):
        return await m.reply("Только администратор.")
    await m.reply("Отправь идентификатор чата для удаления.")
    await state.set_state(NewTask.entering_manual)
    await state.update_data(mode="removechat")

@dp.message(NewTask.entering_manual)
async def handle_removechat_or_manual(m: types.Message, state: FSMContext):
    data = await state.get_data()
    mode = data.get("mode")
    if mode == "removechat":
        parsed = parse_chat_identifier(m.text.strip())
        if not parsed:
            return await m.reply("Не распознал идентификатор.")
        await remove_chat(parsed)
        await m.reply(f"Удалён: {parsed}")
        await state.clear()
        return
    return

# -------------------- New task creation flow (FSM) --------------------
@dp.message(Command("newtask"))
async def cmd_newtask(m: types.Message, state: FSMContext):
    if not await is_admin(m.from_user):
        return await m.reply("Только администратор.")
    kb = types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="Выбрать из добавленных")],
            [types.KeyboardButton(text="Ввести вручную")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await m.reply("Создание новой задачи. Выбери способ:", reply_markup=kb)
    await state.set_state(NewTask.choosing_source)

@dp.message(NewTask.choosing_source)
async def newtask_choose_source(m: types.Message, state: FSMContext):
    await m.answer("Ок", reply_markup=types.ReplyKeyboardRemove())
    if m.text == "Выбрать из добавленных":
        chats = await list_chats()
        if not chats:
            await m.reply("Список добавленных пуст. Сначала добавь чаты через /addchat.")
            await state.clear()
            return
        await state.update_data(all_chats=chats)
        text = "Список чатов:\n" + "\n".join(f"{i+1}. {c}" for i, c in enumerate(chats))
        text += "\nОтправь номера через запятую (например: 1,2,5) или 'all'."
        await m.reply(text)
        await state.set_state(NewTask.choosing_from_list)
    else:
        await m.reply("Введи идентификаторы через запятую (например: @a, -100123, https://t.me/b)")
        await state.set_state(NewTask.entering_manual)
        await state.update_data(mode="newtask_manual")

@dp.message(NewTask.choosing_from_list)
async def newtask_chats_selected_from_list(m: types.Message, state: FSMContext):
    raw = m.text.strip()
    data = await state.get_data()
    chats = data.get("all_chats", [])
    selected = []

    if raw.lower() == "all":
        selected = chats
    else:
        parts = [p.strip() for p in raw.split(",")]
        for p in parts:
            if p.isdigit():
                idx = int(p) - 1
                if 0 <= idx < len(chats):
                    selected.append(chats[idx])
            elif "-" in p:
                try:
                    a, b = map(int, p.split("-", 1))
                    for i in range(a - 1, b):
                        if 0 <= i < len(chats):
                            selected.append(chats[i])
                except:
                    pass

    if not selected:
        await m.reply("Не выбрано ни одного чата. Отмена.")
        await state.clear()
        return

    await state.update_data(chats=selected)
    await m.reply("Теперь пришли текст сообщения или отправь медиа. Подпись к медиа станет текстом.")
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

    await state.update_data(text=text, file_id=file_id, file_type=file_type)

    kb = types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="Разово (один раз)")],
            [types.KeyboardButton(text="Ежедневно")],
            [types.KeyboardButton(text="Несколько раз в день")],
            [types.KeyboardButton(text="Еженедельно")],
            [types.KeyboardButton(text="Ежемесячно")],
            [types.KeyboardButton(text="Будни / Выходные")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await m.reply("Выбери режим отправки:", reply_markup=kb)
    await state.set_state(NewTask.choosing_schedule_type)

@dp.message(NewTask.choosing_schedule_type)
async def newtask_choose_schedule_type(m: types.Message, state: FSMContext):
    await m.answer("Ок", reply_markup=types.ReplyKeyboardRemove())
    t = m.text

    if t == "Разово (один раз)":
        await m.reply("Введите дату и время: YYYY-MM-DD HH:MM (например: 2025-10-30 18:30)")
        await state.set_state(NewTask.entering_once)

    elif t == "Ежедневно":
        await m.reply("Введите время: HH:MM (например: 09:00)")
        await state.set_state(NewTask.entering_daily)

    elif t == "Несколько раз в день":
        await m.reply("Введите времена через запятую: HH:MM,HH:MM (например: 09:00,13:30,20:00)")
        await state.set_state(NewTask.entering_multiple_daily)

    elif t == "Еженедельно":
        await m.reply("Введите дни недели и время. Пример: mon,wed,fri 09:00\nДни: mon,tue,wed,thu,fri,sat,sun")
        await state.set_state(NewTask.entering_weekly)

    elif t == "Ежемесячно":
        await m.reply("Введите числа месяца и время. Пример: 1,15 09:00")
        await state.set_state(NewTask.entering_monthly)

    elif t == "Будни / Выходные":
        kb = types.ReplyKeyboardMarkup(
            keyboard=[
                [types.KeyboardButton(text="Будни (Mon-Fri)")],
                [types.KeyboardButton(text="Выходные (Sat-Sun)")],
            ],
            resize_keyboard=True,
            one_time_keyboard=True
        )
        await m.reply("Выбери вариант:", reply_markup=kb)
        await state.set_state(NewTask.choosing_weekmode)

    else:
        await m.reply("Неверный выбор. Отмена.")
        await state.clear()

@dp.message(NewTask.entering_once)
async def newtask_schedule_once(m: types.Message, state: FSMContext):
    txt = m.text.strip()
    try:
        dt = datetime.strptime(txt, "%Y-%m-%d %H:%M")
    except:
        await m.reply("Неверный формат. Попробуйте ещё раз.")
        return
    schedule = {"type": "once", "datetime": dt.strftime("%Y-%m-%d %H:%M")}
    await finalize_newtask(m, state, schedule)

@dp.message(NewTask.entering_daily)
async def newtask_schedule_daily(m: types.Message, state: FSMContext):
    txt = m.text.strip()
    try:
        _ = time_str_to_time(txt)
    except:
        await m.reply("Неверный формат времени.")
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
        await m.reply("Неверный формат времен.")
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
        await m.reply("Неверный формат. Пример: mon,wed,fri 09:00")

@dp.message(NewTask.entering_monthly)
async def newtask_schedule_monthly(m: types.Message, state: FSMContext):
    try:
        days_part, time_part = m.text.split()
        days = [int(x.strip()) for x in days_part.split(",") if x.strip()]
        _ = time_str_to_time(time_part.strip())
        schedule = {"type": "monthly", "days": days, "times": [time_part.strip()]}
        await finalize_newtask(m, state, schedule)
    except:
        await m.reply("Неверный формат. Пример: 1,15 09:00")

@dp.message(NewTask.choosing_weekmode)
async def newtask_schedule_weekdays_weekends(m: types.Message, state: FSMContext):
    choice = m.text.strip()
    if choice == "Будни (Mon-Fri)":
        await state.update_data(weekmode="weekdays")
        await m.reply("Введите время: HH:MM")
        await state.set_state(NewTask.entering_weekmode_time)
    elif choice == "Выходные (Sat-Sun)":
        await state.update_data(weekmode="weekends")
        await m.reply("Введите время: HH:MM")
        await state.set_state(NewTask.entering_weekmode_time)
    else:
        await m.reply("Неверный выбор. Отмена.")
        await state.clear()

@dp.message(NewTask.entering_weekmode_time)
async def finalize_newtask_from_weekmode(m: types.Message, state: FSMContext):
    txt = m.text.strip()
    try:
        _ = time_str_to_time(txt)
    except:
        await m.reply("Неверный формат времени.")
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
        await m.reply("Чаты для рассылки не выбраны. Отмена.")
        await state.clear()
        return

    task_id = await add_task(chats, text, file_id, file_type, schedule, created_by)
    next_run = compute_next_run_from_schedule(schedule)
    await m.reply(
        f"Задача #{task_id} создана.\n"
        f"Расписание: {schedule_to_str(schedule)}\n"
        f"Следующий запуск: {next_run}"
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
    for t in tasks:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"edit:{t['id']}"),
             InlineKeyboardButton(text="🗑 Удалить", callback_data=f"delete:{t['id']}")],
            [InlineKeyboardButton(text="🚀 Отправить сейчас", callback_data=f"sendnow:{t['id']}"),
             InlineKeyboardButton(text=("⏸ Включено" if t['enabled'] else "▶️ Включить"), callback_data=f"toggle:{t['id']}")]
        ])
        info = (
            f"ID: {t['id']}\n"
            f"Чаты: {', '.join(t['chats'])}\n"
            f"Текст: {t['text'][:200] + ('...' if len(t['text'])>200 else '')}\n"
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
async def cb_edit(call: types.CallbackQuery):
    task_id = int(call.data.split(":", 1)[1])
    task = await get_task(task_id)
    if not task:
        return await call.answer("Не найдено", show_alert=True)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить текст/подпись", callback_data=f"edittext:{task_id}")],
        [InlineKeyboardButton(text="📎 Заменить/удалить медиа", callback_data=f"editmedia:{task_id}")],
        [InlineKeyboardButton(text="📌 Изменить чаты", callback_data=f"editchats:{task_id}")],
        [InlineKeyboardButton(text="⏰ Изменить расписание", callback_data=f"editschedule:{task_id}")],
    ])
    await call.message.answer(f"Редактирование задачи {task_id}:", reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data.startswith("edittext:"))
async def cb_edittext(call: types.CallbackQuery):
    task_id = int(call.data.split(":", 1)[1])
    await call.message.answer("Отправь новый текст/подпись для задачи.")
    async def handle_edit_text(msg: types.Message):
        new_text = msg.text or ""
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE tasks SET text = ? WHERE id = ?", (new_text, task_id))
            await db.commit()
        await msg.reply("Текст обновлён.")
    dp.message.register(handle_edit_text, F.chat.id == call.message.chat.id)
    await call.answer()

@dp.callback_query(F.data.startswith("editmedia:"))
async def cb_editmedia(call: types.CallbackQuery):
    task_id = int(call.data.split(":", 1)[1])
    await call.message.answer("Пришли новое медиа (фото/видео/документ/аудио/голос) или напиши 'delete' чтобы удалить медиа.")
    async def handle_edit_media(msg: types.Message):
        if msg.text and msg.text.lower().strip() == "delete":
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("UPDATE tasks SET file_id = NULL, file_type = NULL WHERE id = ?", (task_id,))
                await db.commit()
            await msg.reply("Медиа удалено.")
            return
        file_id, file_type = None, None
        if msg.photo:
            file_id = msg.photo[-1].file_id; file_type="photo"
        elif msg.video:
            file_id = msg.video.file_id; file_type="video"
        elif msg.document:
            file_id = msg.document.file_id; file_type="document"
        elif msg.audio:
            file_id = msg.audio.file_id; file_type="audio"
        elif msg.voice:
            file_id = msg.voice.file_id; file_type="voice"
        if not file_id:
            await msg.reply("Не распознал медиа. Отправь корректный файл или 'delete'.")
            return
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE tasks SET file_id = ?, file_type = ? WHERE id = ?", (file_id, file_type, task_id))
            await db.commit()
        await msg.reply("Медиа обновлено.")
    dp.message.register(handle_edit_media, F.chat.id == call.message.chat.id)
    await call.answer()

@dp.callback_query(F.data.startswith("editchats:"))
async def cb_editchats(call: types.CallbackQuery):
    task_id = int(call.data.split(":", 1)[1])
    await call.message.answer("Введи новые чаты через запятую (идентификаторы) или 'list' чтобы выбрать из добавленных.")
    async def handle_edit_chats(msg: types.Message):
        raw = msg.text.strip()
        if raw.lower() == "list":
            chats = await list_chats()
            if not chats:
                await msg.reply("Добавленных чатов нет.")
                return
            text = "Добавленные чаты:\n" + "\n".join(f"{i+1}. {c}" for i, c in enumerate(chats))
            text += "\nОтправь номера через запятую или 'all'."
            await msg.reply(text)
            async def handle_edit_chats_list(mm: types.Message):
                r = mm.text.strip()
                selected = []
                if r.lower() == "all":
                    selected = chats
                else:
                    for p in [x.strip() for x in r.split(",")]:
                        if p.isdigit():
                            idx = int(p)-1
                            if 0 <= idx < len(chats):
                                selected.append(chats[idx])
                if not selected:
                    await mm.reply("Ничего не выбрано.")
                    return
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute("UPDATE tasks SET chats = ? WHERE id = ?", (json.dumps(selected), task_id))
                    await db.commit()
                await mm.reply("Чаты обновлены.")
            dp.message.register(handle_edit_chats_list, F.chat.id == msg.chat.id)
            return
        parts = [p.strip() for p in raw.split(",")]
        parsed = []
        for p in parts:
            pp = parse_chat_identifier(p)
            if pp:
                parsed.append(pp)
        if not parsed:
            await msg.reply("Не распознал чаты.")
            return
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE tasks SET chats = ? WHERE id = ?", (json.dumps(parsed), task_id))
            await db.commit()
        await msg.reply("Чаты обновлены.")
    dp.message.register(handle_edit_chats, F.chat.id == call.message.chat.id)
    await call.answer()

@dp.callback_query(F.data.startswith("editschedule:"))
async def cb_editschedule(call: types.CallbackQuery):
    task_id = int(call.data.split(":", 1)[1])
    await call.message.answer("Чтобы изменить расписание, удалите задачу и создайте новую. (Редактор расписания — в разработке.)")
    await call.answer()

# -------------------- Scheduler loop --------------------
async def scheduler_loop():
    while True:
        try:
            now = now_trunc_min()
            async with aiosqlite.connect(DB_PATH) as db:
                cur = await db.execute(
                    "SELECT id, chats, text, file_id, file_type, schedule, next_run FROM tasks WHERE enabled = 1 AND next_run IS NOT NULL"
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
                        next_run_dt = datetime.strptime(next_run_str, "%Y-%m-%d %H:%M")
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
                            nxt = compute_next_run_from_schedule(schedule, base_dt=next_run_dt + timedelta(minutes=1))
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
