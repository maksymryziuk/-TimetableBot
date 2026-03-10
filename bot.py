import logging
import asyncio
import os
import re
import time
import pickle
from datetime import datetime, timedelta
import random
from redis import asyncio as aioredis
from aiogram.fsm.storage.base import StorageKey
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.storage.redis import RedisStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from dotenv import load_dotenv

# Налаштування
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
REDIS_URL = os.getenv("REDIS_URL", "")
URL = ""

# Логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Ініціалізація Redis
redis = aioredis.from_url(REDIS_URL)
storage = RedisStorage(redis)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=storage)

class Form(StatesGroup):
    waiting_for_group = State()
    group_set = State()

# Кеш
cache = {}
cache_lock = asyncio.Lock()
CACHE_TTL = 3 * 86400

# Керування Playwright
class PlaywrightManager:
    def __init__(self):
        self._init_lock = asyncio.Lock() # Лок для захисту від одночасної ініціалізаці
        self.browser = None
        self.context = None
        self._playwright = None

    async def initialize(self):
        async with self._init_lock: # Блокування, щоб уникнути одночасного запуску
            if self.browser:
                return
             # Запуск плейварт і хроміум   
            self._playwright = await async_playwright().start()
            self.browser = await self._playwright.chromium.launch(headless=True)
            self.context = await self.browser.new_context()

    async def close(self):
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self._playwright:
            await self._playwright.stop()

    # ініціалізація браузера
    async def fetch(self, group: str) -> str:
        try:
            if not self.browser:
                await self.initialize()

            page = await self.context.new_page()
            await page.goto(URL, wait_until="domcontentloaded", timeout=10000)

            await page.fill("#group", ''.join(c for c in group if c.isalnum() or c in "-_ "))
            async with page.expect_response(
                lambda response: response.url == URL and response.request.method == "POST",
                timeout=5000
            ):
                await page.keyboard.press("Enter")

            await page.wait_for_selector("table", state="attached", timeout=5000)
            content = await page.content()
            await page.close()
            return content

        except Exception as e:
            logger.error(f"Playwright error: {e}")
            return ""

playwright_mgr = PlaywrightManager()

# генерація дат і днів
def get_next_day_of_week(target_weekday: int) -> str:
    today = datetime.today()
    days_ahead = target_weekday - today.weekday()
    if days_ahead < 0:
        days_ahead += 7
    return (today + timedelta(days=days_ahead)).strftime("%d.%m.%Y")
# для кнопок
def generate_days_keyboard():
    today = datetime.today()
    days = ["Понеділок", "Вівторок", "Середа", "Четвер", "П'ятниця"]
    
    buttons = []
    for i, day in enumerate(days):
        days_ahead = i - today.weekday()
        if days_ahead < 0:
            days_ahead += 7
        target_date = today + timedelta(days=days_ahead)
        date_str = target_date.strftime("%d.%m")
        
        buttons.append(
            InlineKeyboardButton(
                text=f"{day} {date_str}",
                callback_data=f"day_{i}"
            )
        )

    return InlineKeyboardMarkup(inline_keyboard=[
        buttons[:2],
        buttons[2:4],
        [buttons[4]],
        [InlineKeyboardButton(text="Змінити групу", callback_data="change_group")]
    ])
# щоб п'ятниця виводилась нормально
def normalize(text: str) -> str:
    return re.sub(r"[’'`ʻʹʽ]", "", text.lower())
# керування кешом, щоб не стукало на ті групи і дні які вже тикались користувачем
async def get_cached_html(group: str, day_idx: int) -> str | None:
    today = datetime.today()
    target_date = (today + timedelta(days=(day_idx - today.weekday()) % 7)).strftime("%d.%m")
    cache_key = f"{group}_{target_date}"
    
    async with cache_lock:
        if cache_key in cache:
            timestamp, html = cache[cache_key]
            if time.time() - timestamp < CACHE_TTL:
                return html
        return None

async def set_cache(group: str, day_idx: int, html: str):
    today = datetime.today()
    target_date = (today + timedelta(days=(day_idx - today.weekday()) % 7)).strftime("%d.%m")
    cache_key = f"{group}_{target_date}"
    
    async with cache_lock:
        cache[cache_key] = (time.time(), html)

# сам парс сайту
def parse_timetable(html: str, day_idx: int) -> str:
    today = datetime.today()
    days_ahead = (day_idx - today.weekday()) % 7
    target_date = (today + timedelta(days=days_ahead)).strftime("%d.%m.%Y")
    day_names = ["Понеділок", "Вівторок", "Середа", "Четвер", "П'ятниця"]
    target_day_name = day_names[day_idx]

    soup = BeautifulSoup(html, "html.parser")
    
    # Шукаємо точну дату
    exact_day_block = None
    for block in soup.find_all("h4"):
        block_text = block.get_text(strip=True)
        if (target_day_name.lower() in block_text.lower() and 
            target_date in block_text):
            exact_day_block = block
            break
    
    # Якщо не пішло по даті, йдем по днях
    if not exact_day_block:
        for block in soup.find_all("h4"):
            block_text = block.get_text(strip=True)
            if target_day_name.lower() in block_text.lower():
                exact_day_block = block
                break

    if not exact_day_block:
        return f"❌ Розклад на {target_day_name} ({target_date}) не знайдено"

    table = exact_day_block.find_next("table")
    if not table:
        return f"❌ Розклад на {target_day_name} ({target_date}) відсутній"

    heading_raw = exact_day_block.get_text(strip=True)
    heading_fixed = re.sub(r'(\d{2}\.\d{2}\.\d{4})([А-ЯҐЄІЇа-яґєії])', r'\1 \2', heading_raw)
    heading_fixed = ' '.join(heading_fixed.split())
    result = [f"<b>{heading_fixed}</b>"]

    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        time_parts = cells[1].get_text(" ", strip=True).split()
        time = f"{time_parts[0]}-{time_parts[1]}" if len(time_parts) >= 2 else ""

        is_remote = bool(cells[2].find("span", class_="remote_work") or
                         cells[2].find("img", src=lambda x: "home-16" in x if x else False))

        meet_link = ""
        link_tag = cells[2].find("a", href=True)
        if link_tag and is_remote:
            if "meet.google.com" in link_tag["href"]:
                meet_link = f"🔗 <a href='{link_tag['href']}'>Google Meet</a>"
            elif "zoom.us" in link_tag["href"]:
                meet_link = f"🔗 <a href='{link_tag['href']}'>Zoom</a>"

        content = [line.strip() for line in cells[2].get_text("\n").split("\n") if line.strip()]

        if not content:
            result.append(f"<b>{cells[0].get_text()} пара {time}</b>\n🔹 Пари немає")
            continue

        lessons = []
        current_lesson = []
        for line in content:
            if any(x in line for x in ["(Лаб)", "(Л)", "(Пр)"]):
                if current_lesson:
                    lessons.append(current_lesson)
                current_lesson = [line]
            else:
                if not any(x in line.lower() for x in ["дистанційно", "google meet", "zoom", "http"]):
                    current_lesson.append(line)
        if current_lesson:
            lessons.append(current_lesson)

        pairs = []
        for lesson in lessons:
            if not lesson:
                continue

            lesson_type = "📚"
            if "(Лаб)" in lesson[0]:
                lesson_type = "🔬 Лабораторна |"
                lesson[0] = lesson[0].replace("(Лаб)", "").strip()
            elif "(Л)" in lesson[0]:
                lesson_type = "📖 Лекція |"
                lesson[0] = lesson[0].replace("(Л)", "").strip()
            elif "(Пр)" in lesson[0]:
                lesson_type = "✏️ Практика |"
                lesson[0] = lesson[0].replace("(Пр)", "").strip()

            details = []
            for line in lesson[1:]:
                if 'ауд.' in line:
                    details.append(f"🏫 {line}")
                elif 'підгр.' in line or 'Потік' in line or 'група' in line:
                    details.append(f"👥 {line}")
                elif line.strip():
                    details.append(f"👨‍🏫 {line}")

            remote_prefix = '💻 Дистанційно\n' if is_remote else ''
            pair_info = [
                        f"{remote_prefix}{lesson_type} {lesson[0]}",
                        *details,
                        f"{meet_link}" if is_remote and meet_link else ""
            ]
            pair_info = [x for x in pair_info if x.strip()]
            pairs.append("\n".join(pair_info))

        if not pairs:
            result.append(f"<b>{cells[0].get_text()} пара {time}</b>\n🔹 Пари немає")
        else:
            combined_pairs = "\n\n".join(pairs)
            result.append(f"<b>{cells[0].get_text()} пара {time}</b>\n{combined_pairs}")

    if len(result) == 1:
        return f"❌ Розклад на {target_day_name} ({target_date}) відсутній"

    return "\n\n➖➖➖➖➖➖\n\n".join(result)
    
# збереження сесії користувача
async def save_user_session(user_id: int, group: str, message: types.Message = None, state: FSMContext = None):
    await storage.redis.set(f"user:{user_id}:group", group)
    
    if state:
        current_state = await state.get_state()
        await storage.redis.set(f"user:{user_id}:fsm_state", current_state if current_state else "group_set")
    
    if message:
        await storage.redis.set(f"user:{user_id}:last_msg_id", message.message_id)

# отримання сесії
async def get_user_session(user_id: int):
    group = await storage.redis.get(f"user:{user_id}:group")
    fsm_state = await storage.redis.get(f"user:{user_id}:fsm_state")
    
    return {
        "group": group.decode() if group else None,
        "fsm_state": fsm_state.decode() if fsm_state else None
    }

# відновленння сесії
async def restore_user_sessions():
    try:
        keys = await storage.redis.keys("user:*:fsm_state")
        total_users = len(keys)
        if not total_users:
            logger.info("Немає активних сесій для відновлення")
            return

        logger.info(f"Відновлення сесій для {total_users} користувачів...")
        
        restored = 0
        for i, key in enumerate(keys):
            try:
                user_id = int(key.decode().split(":")[1])
                session = await get_user_session(user_id)
                
                if not session["group"] or not session["fsm_state"]:
                    continue
                
                # Видалення останнього повідомлення
                last_msg_id = await storage.redis.get(f"user:{user_id}:last_msg_id")
                if last_msg_id:
                    try:
                        await bot.delete_message(chat_id=user_id, message_id=int(last_msg_id))
                    except Exception as e:
                        logger.error(f"Не вдалося видалити повідомлення для {user_id}: {str(e)}")
                
                # Відновлення стану
                storage_key = StorageKey(
                    chat_id=user_id,
                    user_id=user_id,
                    bot_id=bot.id
                )
                state = FSMContext(storage=storage, key=storage_key)
                await state.set_state(session["fsm_state"])
                await state.set_data({"group": session["group"]})
                
                # Нове повідомлення
                await bot.send_message(
                    user_id,
                    "🔄 Бот був оновлений, Баги пофікшені! Для продовження роботи відправте команду /go\n Знайшли баг? Пиши сюди -> @sky_ei ",
                    reply_markup=types.ReplyKeyboardRemove()
                )
                
                restored += 1
                if i % 10 == 0:
                    await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Помилка відновлення сесії для {user_id}: {str(e)}")
        
        logger.info(f"Успішно відновлено {restored}/{total_users} сесій")
        
    except Exception as e:
        logger.error(f"Помилка у restore_user_sessions: {str(e)}")


# Обробники команд
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await message.answer(
        "👋 Введіть назву вашої групи (наприклад, КІ-22-1): \n Бот працює досить повільно через безкоштовний хост, вибач😢",
        reply_markup=types.ReplyKeyboardRemove()
    )
    await state.set_state(Form.waiting_for_group)

@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    help_text = (
        "👋 Привіт! Я — твій бот-помічник, який допоможе не пропустити пари.\n\n"
        "Щоб дізнатися свій розклад, просто введи назву своєї групи (наприклад, КІ-22-1), "
        "і я відправлю тобі розклад на вибраний день.\n\n"
        "Ось що ти можеш зробити:\n"
        "1. Введи назву своєї групи, і я надам тобі можливість вибрати день тижня.\n"
        "2. Обери день, і я покажу тобі розклад на цей день.\n"
        "3. Якщо потрібно, можеш змінити групу чи вибір дня.\n\n"
        "Ти завжди можеш звернутися до мене, і я надам актуальний розклад."
    )
    await message.answer(help_text, parse_mode="HTML")

@dp.message(Command("go"))
async def cmd_go(message: types.Message, state: FSMContext):
    user_data = await state.get_data()
    group = user_data.get("group")
    
    if not group:
        await message.answer("ℹ️ У вас ще не обрана група. Введіть /start для початку.")
        return
    
    msg = await message.answer(
        f"✅ Обрана група: <b>{group}</b>\nОберіть день:",
        reply_markup=generate_days_keyboard(),
        parse_mode="HTML"
    )
    await save_user_session(message.from_user.id, group, msg, state)
    await state.set_state(Form.group_set)

# Анекдоти
jokes = [
    "Було два пси. Один втопився, а інший згорів. Це були бульдог і хот-дог.",
    "Сидить сліпий і безногий.\nСліпий: Ну шо, як життя іде?\nБезногий: як бачиш.",
    "Мій дід - електрик, сьогодні йому стукнуло 220.",
    "-Тату, ти можеш перестати жартувати про мою сліпоту?\n-Побачимо…",
    "Вася дуже любив жартувати, і коли його друга переїхав поїзд, на похороні друга він привітав його з переїздом.",
    "Грали з дідом в шахи, а дід двинув коня.",
    "Сьогодні помер працівник моргу, але вже завтра він знов буде на роботі.",
    "Штірліц напоїв кішку бензином. Кішка пройшла 2 метри та померла. 'Бензин скінчився', - подумав Штірліц.",
    "Йде оптиміст по кладовищу і бачить одні плюси.",
    "У бабусі було 2 котики і щоб їх не плутати, одного вона назвала Барсік, а другого втопила.."
]

@dp.message(Form.group_set)
async def handle_unexpected_message(message: types.Message):
    joke = random.choice(jokes)
    await message.answer(
        f"🤔 Я не зрозумів, що ви маєте на увазі...\nАле ось тобі анекдот:\n\n<b>{joke}</b>",
        parse_mode="HTML"
    )

@dp.message(Form.waiting_for_group)
async def set_group(message: types.Message, state: FSMContext):
    group = message.text.strip().upper()
    pattern = r'^[A-ZА-ЯҐЄІЇ]{1,5}-\d{1,3}-\d[A-ZА-ЯҐЄІЇ]{0,3}$'
    
    if not re.fullmatch(pattern, group):
        await message.answer(
            "❌ Неправильний формат групи. Введіть у форматі: КІ-22-1 або КІ-22-1к\n"
            "Приклади:\n• КІ-22-1\n• ПМ-21-2\n• АВ-23-1к\n• МТ-20-2м"
        )
        return

    await state.update_data(group=group)
    msg = await message.answer(
        f"✅ Обрана група: <b>{group}</b>\nОберіть день:",
        reply_markup=generate_days_keyboard(),
        parse_mode="HTML"
    )
    await save_user_session(message.from_user.id, group, msg, state)
    await state.set_state(Form.group_set)

# обробка кнопок
@dp.callback_query(Form.group_set)
async def process_day_selection(callback: types.CallbackQuery, state: FSMContext):
    # отримання даних юзера
    user_data = await state.get_data()
    group = user_data.get("group")

    # збереження сесії
    await save_user_session(callback.from_user.id, group, callback.message, state)
    
    if callback.data == "change_group":
        await callback.message.edit_text("Введіть нову назву групи:")
        await state.set_state(Form.waiting_for_group)
        return

    if callback.data.startswith("day_"):
        day_idx = int(callback.data.split("_")[1])
        await callback.message.delete()

        loading_message = await callback.message.answer_animation(
            "https://media.giphy.com/media/3o7bu3XilJ5BOiSGic/giphy.gif",
            caption=f"⏳ Завантажуємо розклад для групи {group}..."
        )
        # Перевіряємо кеш
        try:
            html = await get_cached_html(group, day_idx)
            if not html:
                html = await playwright_mgr.fetch(group)
                if not html:
                    raise Exception("Не вдалося отримати розклад")
                await set_cache(group, day_idx, html)
             # Парс
            timetable = parse_timetable(html, day_idx)
            await loading_message.delete()
            
            msg = await callback.message.answer(
                f"📅 <b>Розклад для {group}</b>\n\n{timetable}",
                reply_markup=generate_days_keyboard(),
                parse_mode="HTML"
            )
            await save_user_session(callback.from_user.id, group, msg, state)
            
        except Exception as e:
            logger.error(f"Помилка: {e}")
            await loading_message.delete()
            error_msg = await callback.message.answer("⚠️ Не вдалося завантажити розклад")
            await asyncio.sleep(3)
            await error_msg.delete()
            
            msg = await callback.message.answer(
                f"✅ Обрана група: <b>{group}</b>\nОберіть день:",
                reply_markup=generate_days_keyboard(),
                parse_mode="HTML"
            )
            await save_user_session(callback.from_user.id, group, msg, state)
            
        finally:
            await callback.answer()

# Запуск бота
async def on_startup():
    await playwright_mgr.initialize()
    await asyncio.sleep(5) 
    await restore_user_sessions()

async def on_shutdown():
    await playwright_mgr.close()

async def keep_alive():
    while True:
        await asyncio.sleep(60)
        logger.info("⏳ Ping: бот активний")

async def run_bot():
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    try:
        await asyncio.gather(
            dp.start_polling(bot),
            keep_alive()
        )
    except Exception as e:
        logger.exception("Критична помилка в main:")
