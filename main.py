import discord
import os
import asyncio
import aiohttp
import json
import base64
import random
import certifi
import secrets
import string
import time
import signal
import sys
import pytz
from datetime import datetime, timedelta, timezone

# Pycord specific UI imports
from discord import InputTextStyle, Interaction, SelectOption
from discord.ui import View, Button, Modal, InputText, Select 
from discord.ext import commands, tasks
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

# --- CONFIGURATION ---
load_dotenv()

TOKEN = os.getenv('DISCORD_TOKEN')
MONGODB_URI = os.getenv('MONGODB_URI')

# REPLACE WITH YOUR REAL DISCORD ID
ADMIN_ID = 1122147581187866660 

# CHANNELS
FARM_LOG_CHANNEL_ID = 1452166255363489917
SAVER_LOG_CHANNEL_ID = 1452339685555834941

# --- STYLING CONSTANTS & EMOJIS ---
# Duo Colors
DUO_GREEN = 0x58CC02
DUO_RED = 0xFF4B4B
DUO_BLUE = 0x1CB0F6
DUO_ORANGE = 0xFF9600
DUO_PURPLE = 0xCE82FF
DUO_DARK = 0x0F172A
DUO_GOLD = 0xF1C40F

# Custom Emojis (Ensure these IDs are correct in your server)
EMOJI_XP = "<:XP:1452179297300250749>"
EMOJI_GEM = "<:Gem:1452195859780603924>"
EMOJI_STREAK = "<:Streak:1452177768707260576>" 
EMOJI_HEALTH = "<:health:1452209178943950980>"
EMOJI_POTION = "<:XPpotion:1452197476323954793>"
EMOJI_FREEZE = "<:StreakFreeze:1452178832747659327>"
EMOJI_OUTFIT = "<:outfit:1452208583642054698>"
EMOJI_TASTE = "<:freetaste:1452210193000697991>"
EMOJI_MISC = "<:misc:1452211033375641766>"
EMOJI_DUO_RAIN = "<:DuoRain:1452268882302599269>"

# Standard UI Emojis
EMOJI_CHECK = "✅"
EMOJI_CROSS = "❌"
EMOJI_TRASH = "🗑️"
EMOJI_FARM = "🚜"
EMOJI_LOADING = "⏳"
EMOJI_REFRESH = "🔄"
EMOJI_SETTINGS = "⚙️"
EMOJI_GLOBE = "🌎"
EMOJI_LOCK = "🔒"
EMOJI_TROPHY = "🏆"
EMOJI_RUN = "▶️"
EMOJI_ADMIN = "🛠️"
EMOJI_DB = "💾"
EMOJI_PING = "📶"
EMOJI_PREV = "⬅️"
EMOJI_NEXT = "➡️"
EMOJI_USERS = "👥"
EMOJI_ACCOUNTS = "📊"
EMOJI_CHART = "📈"
EMOJI_CLOCK = "⏱️"
EMOJI_INFO = "ℹ️"
EMOJI_WARNING = "⚠️"
EMOJI_STOP = "🛑"
EMOJI_SHOP = "🛒"
EMOJI_QUEST = "📜"
EMOJI_CHEST = "🎁"

# --- DUOLINGO CONSTANTS ---
BASE_URL_V1 = "https://www.duolingo.com/2017-06-30"
BASE_URL_V2 = "https://www.duolingo.com/2023-05-23"
SESSIONS_URL = f"{BASE_URL_V1}/sessions"
STORIES_URL = "https://stories.duolingo.com/api2/stories"
LEADERBOARDS_URL = "https://duolingo-leaderboards-prod.duolingo.com/leaderboards/7d9f5dd1-8423-491a-91f2-2532052038ce"
GOALS_URL = "https://goals-api.duolingo.com"
STORY_SLUG = "fr-en-le-passeport"

GEM_REWARDS = [
    "SKILL_COMPLETION_BALANCED-3cc66443_c14d_3965_a68b_e4eb1cfae15e-2-GEMS",
    "SKILL_COMPLETION_BALANCED-110f61a1_f8bc_350f_ac25_1ded90c1d2ed-2-GEMS"
]

CHALLENGE_TYPES = [
    "assist", "characterIntro", "characterMatch", "characterPuzzle", "characterSelect",
    "characterTrace", "characterWrite", "completeReverseTranslation", "definition", "dialogue",
    "extendedMatch", "extendedListenMatch", "form", "freeResponse", "gapFill", "judge", "listen",
    "listenComplete", "listenMatch", "match", "name", "listenComprehension", "listenIsolation",
    "listenSpeak", "listenTap", "orderTapComplete", "partialListen", "partialReverseTranslate",
    "patternTapComplete", "radioBinary", "radioImageSelect", "radioListenMatch",
    "radioListenRecognize", "radioSelect", "readComprehension", "reverseAssist", "sameDifferent",
    "select", "selectPronunciation", "selectTranscription", "svgPuzzle", "syllableTap",
    "syllableListenTap", "speak", "tapCloze", "tapClozeTable", "tapComplete", "tapCompleteTable",
    "tapDescribe", "translate", "transliterate", "transliterationAssist", "typeCloze",
    "typeClozeTable", "typeComplete", "typeCompleteTable", "writeComprehension"
]

# --- SHOP DATA ---
RAW_SHOP_ITEMS = [
    {"id": "streak_freeze", "name": "Streak Freeze", "type": "misc", "price": 200, "currencyType": "XGM"},
    {"id": "streak_freeze_gift", "name": "Streak Freeze Gift", "type": "misc", "price": 20, "currencyType": "XGM"},
    {"id": "formal_outfit", "name": "Formal Attire", "type": "outfit", "price": 400, "currencyType": "XGM"},
    {"id": "luxury_outfit", "name": "Luxury Tracksuit", "type": "outfit", "price": 600, "currencyType": "XGM"},
    {"id": "health_shield", "name": "Health Shield", "type": "misc", "price": 500, "currencyType": "XGM"},
    {"id": "xp_boost_refill", "name": "XP Boost Refill", "type": "misc", "price": 100, "currencyType": "XGM"},
    {"id": "xp_boost_15_gift", "name": "XP Boost 15 Gift", "type": "misc", "price": 20, "currencyType": "XGM"},
    {"id": "general_xp_boost", "name": "General XP Boost", "type": "misc", "price": 750, "currencyType": "XGM"},
    {"id": "xp_boost_15", "name": "XP Boost 15m", "type": "misc", "price": 750, "currencyType": "XGM"},
    {"id": "unlimited_hearts_boost", "name": "Unlimited Hearts", "type": "misc", "price": 750, "currencyType": "XGM"},
    {"id": "early_bird_xp_boost", "name": "Early Bird XP", "type": "misc", "price": 750, "currencyType": "XGM"},
    {"id": "health_refill", "name": "Health Refill", "type": "misc", "price": 350, "currencyType": "XGM"}
]

# --- GLOBAL STATE MANAGEMENT ---
active_farms = {}
stop_reasons = {}
start_time = time.time()
bot_is_stopping = False

# Initialize Bot (Pycord)
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# Initialize Database
try:
    mongo_client = AsyncIOMotorClient(MONGODB_URI, tlsCAFile=certifi.where())
    db = mongo_client["duo_streak_saver"]
    users_collection = db["users"]
    print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Connected to MongoDB.")
except Exception as e:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ MongoDB Connection Error: {e}")

# --- SIGNAL HANDLING (SHUTDOWN) ---
def signal_handler(sig, frame):
    global bot_is_stopping
    if bot_is_stopping: return
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] [!] Ctrl+C detected. Initiating graceful shutdown...")
    bot_is_stopping = True
    asyncio.create_task(shutdown_sequence())

async def shutdown_sequence():
    tasks_to_wait = []
    for key, data in list(active_farms.items()):
        if not data['task'].done():
            stop_reasons[key] = "Bot Shutdown"
            data['task'].cancel()
            tasks_to_wait.append(data['task'])
    if tasks_to_wait:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Waiting for {len(tasks_to_wait)} farms to clean up...")
        await asyncio.gather(*tasks_to_wait, return_exceptions=True)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Cleanup complete. Closing bot connection.")
    await bot.close()

# Note: signal.signal might interfere with loop in some envs, but standard for standalone scripts
signal.signal(signal.SIGINT, signal_handler)

# --- HELPER FUNCTIONS ---

def build_embed(title, description=None, color=DUO_BLUE, thumbnail=None):
    """Creates a consistent, beautiful embed."""
    embed = discord.Embed(title=title, description=description, color=color, timestamp=datetime.now())
    if thumbnail:
        if thumbnail.startswith("//"):
            thumbnail = f"https:{thumbnail}"
        if thumbnail.startswith("http"):
            embed.set_thumbnail(url=thumbnail)
    embed.set_footer(text="DuoRain • Automations")
    return embed

def get_headers(jwt=None, user_id=None):
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "device-platform": "web",
        "x-duolingo-device-platform": "web",
        "x-duolingo-app-version": "1.0.0",
        "x-duolingo-application": "chrome",
        "x-duolingo-client-version": "web",
        "Connection": "Keep-Alive",
    }
    if jwt:
        headers["Authorization"] = f"Bearer {jwt}"
        headers["Cookie"] = f"jwt_token={jwt}"
    if user_id:
        headers["x-amzn-trace-id"] = f"User={user_id}"
    return headers

async def get_duo_profile(session, jwt, user_id):
    try:
        url = f"{BASE_URL_V2}/users/{user_id}?fields=username,totalXp,streak,gems,fromLanguage,learningLanguage,streakData,timezone,picture"
        async with session.get(url, headers=get_headers(jwt, user_id), timeout=15) as resp:
            if resp.status == 200:
                return await resp.json()
    except:
        pass
    return None

async def get_privacy_settings(session, jwt, user_id):
    try:
        url = f"{BASE_URL_V2}/users/{user_id}/privacy-settings?fields=privacySettings"
        async with session.get(url, headers=get_headers(jwt, user_id)) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get('privacySettings', [])
    except:
        pass
    return []

async def set_privacy_settings(session, jwt, user_id, disable_social_bool):
    try:
        url = f"{BASE_URL_V2}/users/{user_id}/privacy-settings?fields=privacySettings"
        headers = get_headers(jwt, user_id)
        payload = {"DISABLE_SOCIAL": disable_social_bool}
        async with session.patch(url, headers=headers, json=payload) as resp:
            return resp.status == 200
    except:
        return False

async def extract_user_id(jwt):
    try:
        payload = jwt.split('.')[1]
        padded = payload + '=' * (-len(payload) % 4)
        decoded = base64.urlsafe_b64decode(padded)
        return json.loads(decoded).get('sub')
    except:
        return None

def create_progress_bar(current, total, length=10):
    if total == 0: total = 1
    percent = min(1.0, current / total)
    filled_length = int(length * percent)
    bar = "🟩" * filled_length + "⬛" * (length - filled_length)
    return f"`{bar}` **{int(percent * 100)}%**"

def format_time(seconds):
    if seconds < 0: seconds = 0
    if seconds < 60: return f"{int(seconds)}s"
    m = int(seconds // 60)
    s = int(seconds % 60)
    if m < 60: return f"{m}m {s}s"
    h = int(m // 60)
    m = int(m % 60)
    return f"{h}h {m}m"

def get_uptime():
    uptime_seconds = int(time.time() - start_time)
    days, remainder = divmod(uptime_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{days}d {hours}h {minutes}m {seconds}s"

async def update_account_delay(user_id, duo_id, delay_type, delay_ms):
    await users_collection.update_one(
        {"_id": user_id, "accounts.duo_id": duo_id},
        {"$set": {f"accounts.$.delays.{delay_type}": delay_ms}}
    )

def is_social_disabled(privacy_settings):
    if not privacy_settings: return False
    for s in privacy_settings:
        if isinstance(s, dict) and s.get('id') == 'disable_social':
            return s.get('enabled', False)
    return False

# --- QUEST SYSTEM ---
async def get_goals_schema(session, jwt):
    """Fetches the goals schema which contains badge definitions."""
    try:
        url = f"{GOALS_URL}/schema?ui_language=en"
        async with session.get(url, headers=get_headers(jwt)) as resp:
            if resp.status == 200:
                return await resp.json()
    except Exception as e:
        print(f"Goal Schema Error: {e}")
    return None

async def brute_force_metrics(session, jwt, user_id, metrics, timestamp_str):
    """Sends a batch update to force complete specific metrics."""
    try:
        updates = [{"metric": m, "quantity": 2000} for m in metrics]
        updates.append({"metric": "QUESTS", "quantity": 1}) # Ensure QUESTS metric is bumped
        
        payload = {
            "metric_updates": updates,
            "timezone": "UTC",
            "timestamp": timestamp_str
        }
        
        url = f"{GOALS_URL}/users/{user_id}/progress/batch"
        headers = get_headers(jwt)
        # Goals API typically requires x-requested-with
        headers["x-requested-with"] = "XMLHttpRequest" 
        
        async with session.post(url, headers=headers, json=payload) as resp:
            return resp.status == 200
    except Exception as e:
        print(f"Brute Force Error: {e}")
        return False

async def process_quests(session, jwt, user_id, mode="daily"):
    """
    mode can be:
    - 'daily': Completes current daily/friends quests
    - 'monthly_current': Completes only this month's badge
    - 'all_previous': Completes all previous monthly badges
    """
    schema = await get_goals_schema(session, jwt)
    if not schema:
        return "Failed to fetch quest data."

    goals = schema.get('goals', [])
    unique_metrics = set()
    timestamp_map = {} # Map metric to timestamp if special handling needed

    now = datetime.now(timezone.utc)
    current_year = now.year
    current_month = now.month
    
    # Regex to identify monthly badges: YYYY_MM_monthly
    
    count = 0

    if mode == "daily":
        for g in goals:
            cat = g.get('category', [])
            if "DAILY" in cat or "FRIENDS" in cat:
                if g.get('metric'):
                    unique_metrics.add(g.get('metric'))
                    count += 1
        ts_str = now.isoformat()

    elif mode == "monthly_current":
        target_id = f"{current_year}_{current_month:02d}_monthly"
        for g in goals:
            if target_id in g.get('goalId', ''):
                if g.get('metric'):
                    unique_metrics.add(g.get('metric'))
                    count += 1
        # Set timestamp to middle of current month to be safe
        ts_date = datetime(current_year, current_month, 15, 12, 0, 0, tzinfo=timezone.utc)
        ts_str = ts_date.isoformat()

    elif mode == "all_previous":
        processed_months = 0
        for g in goals:
            gid = g.get('goalId', '')
            # Check if it looks like a monthly badge
            parts = gid.split('_')
            if len(parts) >= 3 and parts[2] == 'monthly':
                try:
                    y = int(parts[0])
                    m = int(parts[1])
                    # Check if it's in the past
                    if y < current_year or (y == current_year and m < current_month):
                        if g.get('metric'):
                            # For batching previous months, we technically need separate requests per month
                            # to get the timestamp right, OR we can try sending them all.
                            # Best practice: Send one request per month found.
                            
                            # We will store them to process individually below
                            m_ts = datetime(y, m, 15, 12, 0, 0, tzinfo=timezone.utc).isoformat()
                            if m_ts not in timestamp_map: timestamp_map[m_ts] = []
                            timestamp_map[m_ts].append(g.get('metric'))
                            processed_months += 1
                except: continue
        
        # If all_previous, we process the timestamp map
        if not timestamp_map: return "No previous monthly badges found."
        
        success_count = 0
        for ts, metrics in timestamp_map.items():
            if await brute_force_metrics(session, jwt, user_id, list(set(metrics)), ts):
                success_count += 1
            await asyncio.sleep(0.5)
        return f"Processed {success_count} past months."

    # Process Daily or Current Monthly (Single Request)
    if unique_metrics:
        success = await brute_force_metrics(session, jwt, user_id, list(unique_metrics), ts_str)
        if success: return f"Successfully completed {mode} quests."
        else: return "Request failed."
    
    if mode != "all_previous" and count == 0:
        return f"No active {mode} quests found."
        
    return "Unknown error."

# --- CORE DUOLINGO ACTIONS ---

async def perform_one_lesson(session, jwt, user_id, from_lang, learning_lang):
    try:
        headers = get_headers(jwt, user_id)
        session_payload = {
            "challengeTypes": CHALLENGE_TYPES, "fromLanguage": from_lang, "learningLanguage": learning_lang,
            "type": "GLOBAL_PRACTICE", "isFinalLevel": False, "isV2": True, "juicy": True, "smartTipsVersion": 2
        }
        async with session.post(SESSIONS_URL, json=session_payload, headers=headers, timeout=10) as resp:
            if resp.status != 200: return False
            sess_data = await resp.json()
            
        session_id = sess_data.get('id')
        if not session_id: return False

        await asyncio.sleep(2) 

        now_ts = datetime.now(timezone.utc).timestamp()
        update_payload = {
            **sess_data, "heartsLeft": 5, "startTime": now_ts - 60, "endTime": now_ts,
            "failed": False, "maxInLessonStreak": 9, "shouldLearnThings": True, "enableBonusPoints": False
        }
        async with session.put(f"{SESSIONS_URL}/{session_id}", json=update_payload, headers=headers, timeout=10) as resp:
            return resp.status == 200
    except:
        return False

async def run_xp_story(session, jwt, from_lang, to_lang):
    try:
        headers = get_headers(jwt)
        now_ts = int(time.time())
        payload = {
            "awardXp": True, "completedBonusChallenge": True, "fromLanguage": from_lang, 
            "learningLanguage": to_lang, "hasXpBoost": False, "illustrationFormat": "svg",
            "isFeaturedStoryInPracticeHub": True, "isLegendaryMode": True, "isV2Redo": False, 
            "isV2Story": False, "masterVersion": True, "maxScore": 0, "score": 0, 
            "happyHourBonusXp": 469, "startTime": now_ts, "endTime": now_ts + random.randint(40, 60)
        }
        async with session.post(f"{STORIES_URL}/{STORY_SLUG}/complete", headers=headers, json=payload, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("awardedXp", 0)
    except:
        pass
    return 0

# --- SHOP LOGIC ---
def categorize_items():
    cats = {"XP Boosts": [], "Health/Hearts": [], "Outfits": [], "Misc": []}
    for item in RAW_SHOP_ITEMS:
        if item.get("currencyType") != "XGM": continue
        iid = item['id']
        name = item.get('name', iid.replace('_', ' ').title())
        item['name'] = name 
        if "xp_boost" in iid or "general_xp" in iid: cats["XP Boosts"].append(item)
        elif "health" in iid or "heart" in iid: cats["Health/Hearts"].append(item)
        elif "outfit" in iid: cats["Outfits"].append(item)
        else: cats["Misc"].append(item)
    return cats

async def purchase_shop_item(session, jwt, user_id, item_id, from_lang, to_lang):
    headers = get_headers(jwt, user_id)
    if item_id == "xp_boost_refill":
        url = "https://ios-api-2.duolingo.com/2023-05-23/batch"
        headers["host"] = "ios-api-2.duolingo.com"
        inner_body = {"isFree": False, "learningLanguage": to_lang, "subscriptionFeatureGroupId": 0, "xpBoostSource": "REFILL", "xpBoostMinutes": 15, "xpBoostMultiplier": 3, "id": item_id}
        payload = {"includeHeaders": True, "requests": [{"url": f"/2023-05-23/users/{user_id}/shop-items", "extraHeaders": {}, "method": "POST", "body": json.dumps(inner_body)}]}
    else:
        url = f"{BASE_URL_V1}/users/{user_id}/shop-items"
        payload = {"itemName": item_id, "isFree": True, "consumed": True, "fromLanguage": from_lang, "learningLanguage": to_lang}
    try:
        async with session.post(url, headers=headers, json=payload) as resp:
            return resp.status == 200
    except:
        return False

# --- LEAGUE LOGIC ---
async def get_league_position(session, jwt, user_id):
    try:
        headers = get_headers(jwt, user_id)
        url = f"{LEADERBOARDS_URL}/users/{user_id}?client_unlocked=true&get_reactions=true&_={int(time.time() * 1000)}"
        async with session.get(url, headers=headers, timeout=10) as resp:
            if resp.status != 200: return None
            data = await resp.json()
        
        active = data.get('active')
        if active is None: return {"banned": True}
            
        cohort = active.get('cohort', {})
        rankings = cohort.get('rankings', [])
        
        my_data = next((u for u in rankings if str(u.get('user_id')) == str(user_id)), None)
        if not my_data: return None

        my_rank = next((i + 1 for i, u in enumerate(rankings) if str(u.get('user_id')) == str(user_id)), None)
        return {"rank": my_rank, "score": my_data.get('score'), "rankings": rankings, "banned": False}
    except Exception as e:
        print(f"League Fetch Error: {e}")
    return None

async def league_saver_logic(session, jwt, user_id, target_rank, from_lang, to_lang, update_msg=None):
    farmed_session_xp = 0
    try:
        data = await get_league_position(session, jwt, user_id)
        if not data: return "Could not fetch leaderboard data."

        if data.get("banned"):
            return "❌ User is BANNED from Leagues."
        
        current_rank = data['rank']
        if current_rank <= target_rank:
            return f"Safe at Rank #{current_rank}."

        rankings = data['rankings']
        target_idx = min(len(rankings)-1, target_rank - 1)
        target_user = rankings[target_idx]
        target_score = target_user['score']
        my_score = data['score']
        
        xp_needed = (target_score - my_score) + 100
        if xp_needed <= 0: xp_needed = 40 

        if update_msg: 
            await update_msg.edit(embed=build_embed(
                f"{EMOJI_TROPHY} League Saver Active", 
                f"**Current:** #{current_rank} ({my_score} XP)\n**Target:** #{target_rank} ({target_score} XP)\n**Need:** ~{xp_needed} XP", 
                DUO_BLUE))

        last_edit = time.time()
        while farmed_session_xp < xp_needed:
            xp = await run_xp_story(session, jwt, from_lang, to_lang)
            if xp == 0: 
                await asyncio.sleep(2)
                xp_retry = await run_xp_story(session, jwt, from_lang, to_lang)
                if xp_retry == 0: break
                xp = xp_retry

            farmed_session_xp += xp
            if update_msg and (time.time() - last_edit > 8):
                 try:
                     await update_msg.edit(embed=build_embed(
                         f"{EMOJI_TROPHY} League Saver", 
                         f"Farming XP...\n**Gained:** {farmed_session_xp}/{xp_needed}", 
                         DUO_BLUE))
                     last_edit = time.time()
                 except: pass 
            await asyncio.sleep(0.5)

        return f"Finished. Farmed {farmed_session_xp} XP to secure rank."
    except Exception as e:
        return f"Error: {e}"

# --- VIEWS & MODALS ---

class ProtectedView(View):
    def __init__(self, author_id, timeout=180):
        super().__init__(timeout=timeout)
        self.author_id = author_id

    async def interaction_check(self, interaction: Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(embed=build_embed(f"{EMOJI_CROSS} Access Denied", "This menu is not for you.", DUO_RED), ephemeral=True)
            return False
        return True

class JWTModal(Modal):
    def __init__(self, *args, **kwargs):
        super().__init__(title="Link with Token", *args, **kwargs)
        self.add_item(InputText(label="JWT Token", style=InputTextStyle.paragraph, placeholder="Paste token...", required=True, min_length=20))

    async def callback(self, interaction: Interaction):
        await interaction.followup.defer(ephemeral=True)
        jwt = self.children[0].value.strip().replace('"', '')
        if not len(jwt.split('.')) == 3:
            await interaction.followup.send(embed=build_embed(f"{EMOJI_CROSS} Invalid Token", "Invalid JWT structure.", DUO_RED), ephemeral=True)
            return
        await process_login(interaction, jwt)

class EmailLoginModal(Modal):
    def __init__(self, *args, **kwargs):
        super().__init__(title="Login with Credentials", *args, **kwargs)
        self.add_item(InputText(label="Email/Username", placeholder="duo_fan_123", required=True))
        self.add_item(InputText(label="Password", placeholder="Password...", required=True))

    async def callback(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        email = self.children[0].value
        password = self.children[1].value
        distinct_id = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(32))
        login_data = {"distinctId": distinct_id, "identifier": email, "password": password}
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post("https://www.duolingo.com/2017-06-30/login?fields=", json=login_data, headers=get_headers()) as response:
                    if response.status != 200:
                        await interaction.followup.send(embed=build_embed(f"{EMOJI_CROSS} Login Failed", "Check credentials.", DUO_RED), ephemeral=True)
                        return
                    jwt = response.headers.get('jwt')
                    if not jwt: return
                    await process_login(interaction, jwt)
            except Exception as e:
                await interaction.followup.send(embed=build_embed(f"{EMOJI_WARNING} Error", str(e), DUO_RED), ephemeral=True)

class DelayModal(Modal):
    def __init__(self, account, delay_type, *args, **kwargs):
        super().__init__(title=f"Set {delay_type} Delay", *args, **kwargs)
        self.account = account
        self.delay_type = delay_type
        self.add_item(InputText(label="Delay (ms)", placeholder="100", required=True, max_length=5))

    async def callback(self, interaction: Interaction):
        try:
            val = int(self.children[0].value)
            if val < 1: val = 1
            await update_account_delay(interaction.user.id, self.account['duo_id'], self.delay_type, val)
            await interaction.response.send_message(embed=build_embed(f"{EMOJI_CHECK} Delay Updated", f"**{self.delay_type}** delay set to **{val}ms** for {self.account['username']}.", DUO_GREEN), ephemeral=True)
        except ValueError:
            await interaction.response.send_message(embed=build_embed(f"{EMOJI_CROSS} Invalid Input", "Please enter a valid number.", DUO_RED), ephemeral=True)

class FarmModal(Modal):
    def __init__(self, farm_type, jwt, duo_id, username, delay_ms, *args, **kwargs):
        super().__init__(title=f"{farm_type} Farming", *args, **kwargs)
        self.farm_type = farm_type
        self.jwt = jwt
        self.duo_id = str(duo_id)
        self.username = username
        self.delay_ms = delay_ms
        label_map = {"XP": "Amount of XP", "Gems": "Loops (60 gems/loop)", "Streak": "Days to add"}
        label_text = label_map.get(farm_type, "Amount")
        self.add_item(InputText(label=label_text, placeholder="e.g. 100", required=True))
    
    async def callback(self, interaction: Interaction):
        farm_key = f"{self.duo_id}_{self.farm_type}"
        if self.farm_type == "Gems": farm_key = f"{self.duo_id}_Gem"
        
        if farm_key in active_farms:
            await interaction.response.send_message(embed=build_embed(f"{EMOJI_CROSS} Farm Active", f"**{self.username}** is already running a {self.farm_type} farm!", DUO_RED), ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        try:
            amount = int(self.children[0].value)
            if amount <= 0: raise ValueError
            if interaction.user.id != ADMIN_ID:
                if self.farm_type == "XP" and amount > 1000000:
                    await interaction.followup.send(embed=build_embed("Limit Reached", "Max XP per run is **1,000,000**.", DUO_ORANGE), ephemeral=True)
                    return
                elif (self.farm_type in ["Gems", "Gem", "Streak"]) and amount > 10000:
                    await interaction.followup.send(embed=build_embed("Limit Reached", "Max per run is **10,000**.", DUO_ORANGE), ephemeral=True)
                    return
        except:
            await interaction.followup.send(embed=build_embed(f"{EMOJI_CROSS} Error", "Invalid amount entered.", DUO_RED), ephemeral=True)
            return
        
        async with aiohttp.ClientSession() as temp_sess:
            p = await get_duo_profile(temp_sess, self.jwt, self.duo_id)
            if not p: 
                await interaction.followup.send(embed=build_embed(f"{EMOJI_CROSS} Connection Error", "Could not connect to Duolingo.", DUO_RED), ephemeral=True)
                return
            from_lang = p.get('fromLanguage', 'en')
            to_lang = p.get('learningLanguage', 'fr')
        
        if self.farm_type == "XP":
            task = asyncio.create_task(farm_xp_logic(interaction.user.id, self.jwt, self.duo_id, self.username, amount, from_lang, to_lang, self.delay_ms))
            final_key = f"{self.duo_id}_XP"
        elif self.farm_type == "Gems" or self.farm_type == "Gem": 
            task = asyncio.create_task(farm_gems_logic(interaction.user.id, self.jwt, self.duo_id, self.username, amount, from_lang, to_lang, self.delay_ms))
            final_key = f"{self.duo_id}_Gem"
        elif self.farm_type == "Streak":
            task = asyncio.create_task(farm_streak_logic(interaction.user.id, self.jwt, self.duo_id, self.username, amount, from_lang, to_lang, self.delay_ms))
            final_key = f"{self.duo_id}_Streak"
        else:
             await interaction.followup.send(embed=build_embed("Error", "Invalid farm type.", DUO_RED), ephemeral=True)
             return
        
        active_farms[final_key] = {
            "type": self.farm_type,
            "task": task,
            "user_id": interaction.user.id,
            "username": self.username,
            "duo_id": self.duo_id,
            "target": amount,
            "progress": 0,
            "delay": self.delay_ms # Store delay for ETA calculation
        }

        await interaction.followup.send(embed=build_embed(f"{EMOJI_CHECK} Started {self.farm_type}", f"Farm started for `{self.username}`.\nCheck <#{FARM_LOG_CHANNEL_ID}> for logs.", DUO_GREEN), ephemeral=True)

# --- FARMING & LOGGING LOGIC ---

def get_farm_log_channel():
    return bot.get_channel(FARM_LOG_CHANNEL_ID)

def get_saver_log_channel():
    return bot.get_channel(SAVER_LOG_CHANNEL_ID)

async def farm_xp_logic(discord_user_id, jwt, duo_id, username, amount, from_lang, to_lang, delay_ms):
    channel = get_farm_log_channel()
    discord_user = await bot.fetch_user(discord_user_id)
    farm_key = f"{duo_id}_XP"
    
    if channel: 
        start_embed = build_embed(f"{EMOJI_XP} XP Farm Started", f"**User:** {discord_user.mention}\n**Account:** `{username}`\n**Target:** {amount} XP", DUO_BLUE)
        await channel.send(embed=start_embed)

    try:
        start_clock = time.time()
        async with aiohttp.ClientSession() as session:
            max_xp_req = 499
            min_xp_req = 30
            if amount < min_xp_req: amount = min_xp_req
            max_req = amount // max_xp_req
            remain_xp = amount % max_xp_req
            if remain_xp > 0 and remain_xp < min_xp_req:
                if max_req > 0:
                    max_req -= 1
                    remain_xp += max_xp_req
            
            total_reqs = max_req + (1 if remain_xp >= min_xp_req else 0)
            total_xp_farmed = 0
            sleep_time = delay_ms / 1000.0

            for i in range(max_req):
                if farm_key in active_farms: active_farms[farm_key]['progress'] = total_xp_farmed
                now_ts = int(time.time())
                payload = {"awardXp": True, "completedBonusChallenge": True, "fromLanguage": from_lang, "learningLanguage": to_lang, "hasXpBoost": False, "illustrationFormat": "svg", "isFeaturedStoryInPracticeHub": True, "isLegendaryMode": True, "isV2Redo": False, "isV2Story": False, "masterVersion": True, "maxScore": 0, "score": 0, "happyHourBonusXp": 469, "startTime": now_ts, "endTime": now_ts + random.randint(300, 420)}
                async with session.post(f"{STORIES_URL}/{STORY_SLUG}/complete", headers=get_headers(jwt), json=payload, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        total_xp_farmed += data.get("awardedXp", 0)
                await asyncio.sleep(sleep_time)

            if remain_xp >= min_xp_req:
                now_ts = int(time.time())
                bonus_xp = min(max(0, remain_xp - min_xp_req), 469)
                payload = {"awardXp": True, "completedBonusChallenge": True, "fromLanguage": from_lang, "learningLanguage": to_lang, "hasXpBoost": False, "illustrationFormat": "svg", "isFeaturedStoryInPracticeHub": True, "isLegendaryMode": True, "isV2Redo": False, "isV2Story": False, "masterVersion": True, "maxScore": 0, "score": 0, "happyHourBonusXp": bonus_xp, "startTime": now_ts, "endTime": now_ts + random.randint(40, 60)}
                async with session.post(f"{STORIES_URL}/{STORY_SLUG}/complete", headers=get_headers(jwt), json=payload, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        total_xp_farmed += data.get("awardedXp", 0)
            
            if farm_key in active_farms: active_farms[farm_key]['progress'] = total_xp_farmed

            end_clock = time.time()
            elapsed_str = format_time(end_clock - start_clock)
            
            if channel: 
                finish_embed = build_embed(f"{EMOJI_CHECK} XP Farm Finished", f"**User:** {discord_user.mention}\n**Account:** `{username}`", DUO_GREEN)
                finish_embed.add_field(name="Gained", value=f"**+{total_xp_farmed} XP**", inline=True)
                finish_embed.add_field(name="Time", value=f"`{elapsed_str}`", inline=True)
                await channel.send(embed=finish_embed)

    except asyncio.CancelledError:
        reason = stop_reasons.get(farm_key, "User Request")
        if channel and reason == "User Request":
             await channel.send(embed=build_embed(f"{EMOJI_STOP} Farm Stopped", f"XP Farm for `{username}` stopped manually by {discord_user.mention}.", DUO_RED))
    except Exception as e:
        if channel:
            await channel.send(embed=build_embed(f"{EMOJI_WARNING} Farm Error", f"**Error:** {str(e)}\n**Account:** `{username}`", DUO_RED))
    finally:
        if farm_key in active_farms: del active_farms[farm_key]
        if farm_key in stop_reasons: del stop_reasons[farm_key]

async def farm_gems_logic(discord_user_id, jwt, duo_id, username, loops, from_lang, to_lang, delay_ms):
    channel = get_farm_log_channel()
    discord_user = await bot.fetch_user(discord_user_id)
    farm_key = f"{duo_id}_Gem"

    if channel: 
        await channel.send(embed=build_embed(f"{EMOJI_GEM} Gem Farm Started", f"**User:** {discord_user.mention}\n**Account:** `{username}`\n**Target:** {loops} Loops", DUO_PURPLE))

    try:
        start_clock = time.time()
        async with aiohttp.ClientSession() as session:
            headers = get_headers(jwt, duo_id)
            total_gems = 0
            sleep_time = delay_ms / 1000.0

            for i in range(loops):
                if farm_key in active_farms: active_farms[farm_key]['progress'] = i + 1
                rewards_copy = list(GEM_REWARDS)
                random.shuffle(rewards_copy)
                for _ in range(2):
                    for reward in rewards_copy:
                        url = f"{BASE_URL_V1}/users/{duo_id}/rewards/{reward}"
                        payload = {"consumed": True, "fromLanguage": from_lang, "learningLanguage": to_lang}
                        try:
                            async with session.patch(url, headers=headers, json=payload, timeout=5) as resp: pass
                        except asyncio.CancelledError:
                            raise 
                        except Exception:
                            pass
                total_gems += 120
                await asyncio.sleep(sleep_time)

            end_clock = time.time()
            elapsed_str = format_time(end_clock - start_clock)
            if channel: 
                finish_embed = build_embed(f"{EMOJI_CHECK} Gem Farm Finished", f"**User:** {discord_user.mention}\n**Account:** `{username}`", DUO_GREEN)
                finish_embed.add_field(name="Earned", value=f"**~{total_gems} Gems**", inline=True)
                finish_embed.add_field(name="Time", value=f"`{elapsed_str}`", inline=True)
                await channel.send(embed=finish_embed)

    except asyncio.CancelledError:
        reason = stop_reasons.get(farm_key, "User Request")
        if channel and reason == "User Request":
             await channel.send(embed=build_embed(f"{EMOJI_STOP} Farm Stopped", f"Gem Farm for `{username}` stopped by {discord_user.mention}.", DUO_RED))
    except Exception as e:
        if channel:
            await channel.send(embed=build_embed(f"{EMOJI_WARNING} Farm Error", f"**Error:** {str(e)}\n**Account:** `{username}`", DUO_RED))
    finally:
        if farm_key in active_farms: del active_farms[farm_key]
        if farm_key in stop_reasons: del stop_reasons[farm_key]

async def farm_streak_logic(discord_user_id, jwt, duo_id, username, amount, from_lang, to_lang, delay_ms):
    channel = get_farm_log_channel()
    discord_user = await bot.fetch_user(discord_user_id)
    farm_key = f"{duo_id}_Streak"

    if channel: 
        await channel.send(embed=build_embed(f"{EMOJI_STREAK} Streak Farm Started", f"**User:** {discord_user.mention}\n**Account:** `{username}`\n**Target:** {amount} Days", DUO_ORANGE))

    try:
        start_clock = time.time()
        timeout_settings = aiohttp.ClientTimeout(total=300, connect=10, sock_read=10)
        
        async with aiohttp.ClientSession(timeout=timeout_settings) as session:
            try:
                profile = await get_duo_profile(session, jwt, duo_id)
            except Exception:
                profile = None

            if not profile:
                if channel: await channel.send(f"❌ Could not fetch profile for `{username}`. Aborting.")
                return
            
            s_data = profile.get('streakData') or {}
            curr_streak = s_data.get('currentStreak')
            streak_start_str = curr_streak.get('startDate') if curr_streak else None
            
            if not streak_start_str: 
                farm_start = datetime.now(timezone.utc) - timedelta(days=1)
            else:
                try:
                    streak_start_dt = datetime.strptime(streak_start_str, "%Y-%m-%d")
                    streak_start_dt = streak_start_dt.replace(tzinfo=timezone.utc)
                    farm_start = streak_start_dt - timedelta(days=1)
                except:
                    farm_start = datetime.now(timezone.utc) - timedelta(days=1)

            success_cnt = 0
            sleep_time = delay_ms / 1000.0
            headers = get_headers(jwt, duo_id)
            
            for i in range(amount):
                if farm_key not in active_farms: raise asyncio.CancelledError("Farm removed")
                if farm_key in active_farms: active_farms[farm_key]['progress'] = success_cnt

                sim_day = farm_start - timedelta(days=i)
                session_payload = {"challengeTypes": CHALLENGE_TYPES, "fromLanguage": from_lang, "isFinalLevel": False, "isV2": True, "juicy": True, "learningLanguage": to_lang, "smartTipsVersion": 2, "type": "GLOBAL_PRACTICE"}
                
                sess_id = None
                sess_data = None
                try:
                    async with session.post(SESSIONS_URL, headers=headers, json=session_payload, timeout=10) as resp:
                        if resp.status == 200:
                            sess_data = await resp.json()
                            sess_id = sess_data.get('id')
                        elif resp.status == 429:
                            await asyncio.sleep(5)
                            continue
                except (asyncio.TimeoutError, aiohttp.ClientError):
                    continue
                
                if not sess_id: continue

                start_ts = int((sim_day - timedelta(seconds=60)).timestamp())
                end_ts = int(sim_day.timestamp())
                update_payload = {**sess_data, "heartsLeft": 5, "startTime": start_ts, "endTime": end_ts, "enableBonusPoints": False, "failed": False, "maxInLessonStreak": 9, "shouldLearnThings": True}
                
                await asyncio.sleep(0.5)

                try:
                    async with session.put(f"{SESSIONS_URL}/{sess_id}", headers=headers, json=update_payload, timeout=10) as resp:
                        if resp.status == 200: success_cnt += 1
                except (asyncio.TimeoutError, aiohttp.ClientError):
                    continue
                
                await asyncio.sleep(sleep_time + random.uniform(0.1, 0.3))

            if farm_key in active_farms: active_farms[farm_key]['progress'] = success_cnt

            end_clock = time.time()
            elapsed_str = format_time(end_clock - start_clock)
            if channel: 
                finish_embed = build_embed(f"{EMOJI_CHECK} Streak Farm Finished", f"**User:** {discord_user.mention}\n**Account:** `{username}`", DUO_GREEN)
                finish_embed.add_field(name="Restored", value=f"**{success_cnt} Days**", inline=True)
                finish_embed.add_field(name="Time", value=f"`{elapsed_str}`", inline=True)
                await channel.send(embed=finish_embed)

    except asyncio.CancelledError:
        reason = stop_reasons.get(farm_key, "User Request")
        if channel and reason == "User Request":
             await channel.send(embed=build_embed(f"{EMOJI_STOP} Farm Stopped", f"Streak Farm for `{username}` stopped by {discord_user.mention}.", DUO_RED))
    except Exception as e:
        if channel:
            await channel.send(embed=build_embed(f"{EMOJI_WARNING} Farm Error", f"**Error:** {str(e)}\n**Account:** `{username}`", DUO_RED))
    finally:
        if farm_key in active_farms: del active_farms[farm_key]
        if farm_key in stop_reasons: del stop_reasons[farm_key]

async def process_login(interaction, jwt):
    duo_id = await extract_user_id(jwt)
    if not duo_id:
        await interaction.followup.send(embed=build_embed("Invalid Token", "Could not decode JWT.", DUO_RED), ephemeral=True)
        return

    user_doc = await users_collection.find_one({"_id": interaction.user.id})
    current_accounts = user_doc.get("accounts", []) if user_doc else []
    
    if len(current_accounts) >= 5 and interaction.user.id != ADMIN_ID:
        await interaction.followup.send(embed=build_embed("Limit Reached", "You can only link up to 5 accounts.", DUO_RED), ephemeral=True)
        return

    existing_owner = await users_collection.find_one({"accounts.duo_id": duo_id})
    if existing_owner:
        if existing_owner['_id'] == interaction.user.id:
            await interaction.followup.send(embed=build_embed("Already Linked", "You have already linked this account!", DUO_ORANGE), ephemeral=True)
        else:
            await interaction.followup.send(embed=build_embed("Account Taken", "This account is linked by another user.", DUO_RED), ephemeral=True)
        return
        
    async with aiohttp.ClientSession() as session:
        profile = await get_duo_profile(session, jwt, duo_id)
        if not profile: 
            username, xp, streak, gems, pic = f"User_{duo_id}", 0, 0, 0, None
        else: 
            username, xp, streak, gems = profile.get('username'), profile.get('totalXp'), profile.get('streak'), profile.get('gems')
            pic = profile.get('picture')
        
        await users_collection.update_one(
            {"_id": interaction.user.id}, 
            {"$push": {"accounts": {
                "duo_id": duo_id, 
                "jwt": jwt, 
                "username": username, 
                "delays": {"XP": 100, "Gem": 100, "Streak": 100},
                "streak_saver": False, 
                "league_saver": False,
                "quest_saver": False
            }}},
            upsert=True
        )
        
        embed = build_embed(f"🎉 Welcome, {username}!", "Account linked successfully.", DUO_GREEN, thumbnail=pic)
        embed.add_field(name=f"{EMOJI_XP} Total XP", value=f"**{xp:,}**", inline=True)
        embed.add_field(name=f"{EMOJI_GEM} Gems", value=f"**{gems:,}**", inline=True)
        embed.add_field(name=f"{EMOJI_STREAK} Streak", value=f"**{streak:,}**", inline=True)
        await interaction.followup.send(embed=embed, ephemeral=True)

# --- BACKGROUND TASKS (MONITORS) ---

@tasks.loop(hours=4)
async def streak_monitor():
    channel = get_saver_log_channel()
    pipeline = [{"$match": {"accounts.streak_saver": True}}, {"$project": {"count": {"$size": {"$filter": {"input": "$accounts", "as": "acc", "cond": "$$acc.streak_saver"}}}}}, {"$group": {"_id": None, "total": {"$sum": "$count"}}}]
    users_to_check = 0
    async for doc in users_collection.aggregate(pipeline):
        users_to_check = doc.get("total", 0)
    
    if users_to_check == 0: return

    if channel:
        await channel.send(embed=build_embed(f"{EMOJI_STREAK} Streak Saver Routine", f"Checking **{users_to_check}** accounts...", DUO_ORANGE))

    saved_users = []
    checked_safe_users = []

    async with aiohttp.ClientSession() as session:
        cursor = users_collection.find({})
        async for user_doc in cursor:
            discord_id = user_doc["_id"]
            for acc in user_doc.get("accounts", []):
                if not acc.get("streak_saver", False): continue
                try:
                    p = await get_duo_profile(session, acc['jwt'], acc['duo_id'])
                    if not p: continue
                    
                    timezone_str = p.get("timezone", "Asia/Saigon")
                    try:
                        user_tz = pytz.timezone(timezone_str)
                    except pytz.exceptions.UnknownTimeZoneError:
                        user_tz = pytz.timezone("Asia/Saigon")
                        
                    now = datetime.now(user_tz)
                    streak_data = p.get('streakData', {})
                    current_streak = streak_data.get('currentStreak', {})
                    should_do_lesson = True
                    
                    if current_streak:
                        last_extended = current_streak.get('lastExtendedDate')
                        if last_extended:
                            last_extended_dt = datetime.strptime(last_extended, "%Y-%m-%d")
                            last_extended_dt = user_tz.localize(last_extended_dt)
                            should_do_lesson = last_extended_dt.date() < now.date()
                    
                    if not should_do_lesson:
                        checked_safe_users.append(discord_id)
                        continue 
                        
                    success = await perform_one_lesson(session, acc['jwt'], acc['duo_id'], p['fromLanguage'], p['learningLanguage'])
                    if success: saved_users.append(discord_id)
                    await asyncio.sleep(1.5) 
                except: continue

    if channel:
        unique_saved = list(set(saved_users)) 
        mentions = " ".join([f"<@{uid}>" for uid in unique_saved]) if unique_saved else "None"
        embed = build_embed(f"{EMOJI_CHECK} Streak Saver Complete", None, DUO_GREEN)
        embed.add_field(name="Checked", value=f"{users_to_check}", inline=True)
        embed.add_field(name="Saved", value=f"{len(saved_users)}", inline=True)
        if unique_saved: embed.add_field(name="Users Saved", value=mentions, inline=False)
        else: embed.add_field(name="Status", value="All other users were safe.", inline=False)
        await channel.send(embed=embed)

@tasks.loop(hours=3)
async def league_monitor():
    channel = get_saver_log_channel()
    pipeline = [{"$match": {"accounts.league_saver": True}}, {"$project": {"count": {"$size": {"$filter": {"input": "$accounts", "as": "acc", "cond": "$$acc.league_saver"}}}}}, {"$group": {"_id": None, "total": {"$sum": "$count"}}}]
    users_to_check = 0
    async for doc in users_collection.aggregate(pipeline):
        users_to_check = doc.get("total", 0)
    
    if users_to_check == 0: return

    if channel:
        await channel.send(embed=build_embed(f"{EMOJI_TROPHY} League Saver Routine", f"Checking **{users_to_check}** accounts...", DUO_BLUE))

    saved_users = []
    async with aiohttp.ClientSession() as session:
        cursor = users_collection.find({})
        async for user_doc in cursor:
            discord_id = user_doc["_id"]
            for acc in user_doc.get("accounts", []):
                if not acc.get("league_saver", False): continue
                if f"{acc['duo_id']}_XP" in active_farms: continue
                target = acc.get("target_league_pos", 10)
                try:
                    p = await get_duo_profile(session, acc['jwt'], acc['duo_id'])
                    if not p: continue
                    result = await league_saver_logic(session, acc['jwt'], acc['duo_id'], target, p['fromLanguage'], p['learningLanguage'], None)
                    if "Finished" in str(result): saved_users.append(discord_id)
                    await asyncio.sleep(3)
                except: pass

    if channel:
        unique_saved = list(set(saved_users))
        mentions = " ".join([f"<@{uid}>" for uid in unique_saved]) if unique_saved else "None"
        embed = build_embed(f"{EMOJI_CHECK} League Saver Complete", None, DUO_GREEN)
        embed.add_field(name="Checked", value=f"{users_to_check}", inline=True)
        embed.add_field(name="Farmed", value=f"{len(saved_users)}", inline=True)
        if unique_saved: embed.add_field(name="Secured Users", value=mentions, inline=False)
        else: embed.add_field(name="Status", value="Everyone is safe.", inline=False)
        await channel.send(embed=embed)

@tasks.loop(hours=6)
async def quest_monitor():
    """Checks for active Quest Savers and completes Daily & Current Month quests."""
    channel = get_saver_log_channel()
    pipeline = [{"$match": {"accounts.quest_saver": True}}, {"$project": {"count": {"$size": {"$filter": {"input": "$accounts", "as": "acc", "cond": "$$acc.quest_saver"}}}}}, {"$group": {"_id": None, "total": {"$sum": "$count"}}}]
    
    users_to_check = 0
    async for doc in users_collection.aggregate(pipeline):
        users_to_check = doc.get("total", 0)
    
    if users_to_check == 0: return

    if channel:
        await channel.send(embed=build_embed(f"{EMOJI_QUEST} Quest Saver Routine", f"Checking **{users_to_check}** accounts...", DUO_GOLD))

    completed_users = []
    async with aiohttp.ClientSession() as session:
        cursor = users_collection.find({})
        async for user_doc in cursor:
            discord_id = user_doc["_id"]
            for acc in user_doc.get("accounts", []):
                if not acc.get("quest_saver", False): continue
                try:
                    # Complete Daily Quests
                    res_daily = await process_quests(session, acc['jwt'], acc['duo_id'], "daily")
                    # Complete Monthly Badge (Current)
                    res_monthly = await process_quests(session, acc['jwt'], acc['duo_id'], "monthly_current")
                    
                    if "Successfully" in res_daily or "Successfully" in res_monthly:
                        completed_users.append(discord_id)
                    await asyncio.sleep(1.5)
                except: pass

    if channel:
        unique = list(set(completed_users))
        mentions = " ".join([f"<@{uid}>" for uid in unique]) if unique else "None"
        embed = build_embed(f"{EMOJI_CHECK} Quest Saver Complete", None, DUO_GREEN)
        embed.add_field(name="Checked", value=f"{users_to_check}", inline=True)
        embed.add_field(name="Updated", value=f"{len(completed_users)}", inline=True)
        if unique: embed.add_field(name="Users Updated", value=mentions, inline=False)
        else: embed.add_field(name="Status", value="All quests were already up to date.", inline=False)
        await channel.send(embed=embed)

@bot.event
async def on_ready():
    print(f'[{datetime.now().strftime("%H:%M:%S")}] Logged in as {bot.user}')
    if not streak_monitor.is_running(): streak_monitor.start()
    if not league_monitor.is_running(): league_monitor.start()
    if not quest_monitor.is_running(): quest_monitor.start()

# --- VIEWS ---

class FarmStatusView(View):
    def __init__(self, user_active_farms):
        super().__init__(timeout=None)
        self.user_active_farms = user_active_farms 
        for farm in self.user_active_farms:
            f_type = farm['type']
            d_id = farm['duo_id']
            key_suffix = f"{d_id}_{f_type}"
            if f_type == "Gems": key_suffix = f"{d_id}_Gem"
            btn = Button(label=f"{farm['username']} ({f_type})", style=discord.ButtonStyle.secondary, emoji=EMOJI_CHART, row=0)
            btn.callback = self.make_progress_callback(key_suffix)
            self.add_item(btn)

        has_xp = any(f['type'] == 'XP' for f in self.user_active_farms)
        has_gem = any(f['type'] in ['Gems', 'Gem'] for f in self.user_active_farms)
        has_streak = any(f['type'] == 'Streak' for f in self.user_active_farms)

        if has_xp:
            btn = Button(label="Stop XP", style=discord.ButtonStyle.danger, emoji=EMOJI_XP, row=1)
            btn.callback = self.stop_type_callback("XP")
            self.add_item(btn)
        if has_gem:
            btn = Button(label="Stop Gems", style=discord.ButtonStyle.danger, emoji=EMOJI_GEM, row=1)
            btn.callback = self.stop_type_callback("Gem") 
            self.add_item(btn)
        if has_streak:
            btn = Button(label="Stop Streak", style=discord.ButtonStyle.danger, emoji=EMOJI_STREAK, row=1)
            btn.callback = self.stop_type_callback("Streak")
            self.add_item(btn)

        btn_all = Button(label="Stop ALL", style=discord.ButtonStyle.danger, emoji=EMOJI_STOP, row=2)
        btn_all.callback = self.stop_all_callback
        self.add_item(btn_all)

    def make_progress_callback(self, key_suffix):
        async def callback(interaction: Interaction):
            target_key = key_suffix 
            if target_key not in active_farms:
                 await interaction.response.send_message(embed=build_embed(f"{EMOJI_CROSS} Not Found", "Farm is no longer running.", DUO_RED), ephemeral=True)
                 return
            
            data = active_farms[target_key]
            current = data['progress']
            total = data['target']
            bar = create_progress_bar(current, total)
            
            # ETA Calculation
            delay = data.get('delay', 100)
            remaining = total - current
            
            if data['type'] == "XP":
                loops_left = remaining / 480
                seconds_left = loops_left * ((delay/1000) + 1.5)
            elif data['type'] in ["Gems", "Gem"]:
                remaining_loops = total - current
                seconds_left = remaining_loops * ((delay/1000) + 1.0)
            else:
                seconds_left = remaining * ((delay/1000) + 1.5)
            
            eta_str = format_time(seconds_left)

            color = DUO_BLUE
            icon = EMOJI_XP
            if data['type'] in ["Gems", "Gem"]: 
                color = DUO_PURPLE
                icon = EMOJI_GEM
            elif data['type'] == "Streak": 
                color = DUO_ORANGE
                icon = EMOJI_STREAK

            embed = build_embed(f"{icon} {data['type']} Farm Status", None, color)
            embed.add_field(name="Account", value=f"`{data['username']}`", inline=True)
            embed.add_field(name="Progress", value=f"{current} / {total}", inline=True)
            embed.add_field(name="Estimated Time Left", value=f"**{eta_str}**", inline=True)
            embed.add_field(name="Visual", value=bar, inline=False)
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
        return callback

    def stop_type_callback(self, f_type):
        async def callback(interaction: Interaction):
            stopped_names = []
            keys_to_stop = []
            for key, data in active_farms.items():
                is_correct_type = (data['type'] == f_type) or (f_type == "Gem" and data['type'] == "Gems")
                if data['user_id'] == interaction.user.id and is_correct_type:
                    keys_to_stop.append(key)
                    stopped_names.append(data['username'])
            
            if not keys_to_stop:
                await interaction.response.send_message(embed=build_embed(f"{EMOJI_CROSS} No Active Farms", f"No active **{f_type}** farms found.", DUO_RED), ephemeral=True)
                return

            await interaction.response.send_message(embed=build_embed(f"{EMOJI_STOP} Stopping...", f"Stopping **{f_type}** farms for: {', '.join(stopped_names)}...", DUO_ORANGE), ephemeral=True)
            for key in keys_to_stop:
                if key in active_farms:
                    data = active_farms[key]
                    stop_reasons[key] = "User Request"
                    data['task'].cancel()
        return callback

    async def stop_all_callback(self, interaction: Interaction):
        count = 0
        keys_to_stop = []
        for key, data in active_farms.items():
            if data['user_id'] == interaction.user.id:
                keys_to_stop.append(key)
                count += 1
        
        if count == 0:
             await interaction.response.send_message(embed=build_embed(f"{EMOJI_CROSS} No Farms", "You have no active farms.", DUO_RED), ephemeral=True)
             return

        await interaction.response.send_message(embed=build_embed(f"{EMOJI_STOP} Stopping All", f"Stopping **{count}** active farms...", DUO_RED), ephemeral=True)
        for key in keys_to_stop:
            if key in active_farms:
                active_farms[key]['task'].cancel()
                stop_reasons[key] = "User Request"

# --- COMMANDS ---

@bot.slash_command(name="guide", description="Learn how to get your Duolingo JWT Token")
async def guide_cmd(ctx: discord.ApplicationContext):
    embed = build_embed("🎟️ How to get your Login Token", "Steps to get your JWT Token from Duolingo web:", DUO_BLUE)
    embed.add_field(name="1️⃣ Log In", value="Go to [Duolingo.com](https://www.duolingo.com) on PC.", inline=False)
    embed.add_field(name="2️⃣ Console", value="Press **F12** -> **Console** tab.", inline=False)
    js_code = "document.cookie.match(new RegExp('(^| )jwt_token=([^;]+)'))[0].slice(11)"
    embed.add_field(name="3️⃣ Code", value=f"Paste this:\n```javascript\n{js_code}\n```", inline=False)
    await ctx.respond(embed=embed, ephemeral=True)

@bot.slash_command(name="dashboard", description="View profile and settings")
async def dashboard(ctx: discord.ApplicationContext):
    await ctx.defer(ephemeral=True)
    user_doc = await users_collection.find_one({"_id": ctx.author.id})
    if user_doc is None: user_doc = {"_id": ctx.author.id, "accounts": []}
    accounts = user_doc.get("accounts", [])
    view = DashboardView(user_doc, accounts)
    embed = await view.generate_embed()
    await ctx.respond(embed=embed, view=view, ephemeral=True)

@bot.slash_command(name="farm", description="Start or manage a farming session")
async def farm_cmd(ctx: discord.ApplicationContext):
    await ctx.defer(ephemeral=True)
    user_doc = await users_collection.find_one({"_id": ctx.author.id})
    accounts = user_doc.get("accounts", []) if user_doc else []
    
    if not accounts: 
        return await ctx.respond(embed=build_embed("No Accounts", "No linked accounts. Use `/dashboard` to add one.", DUO_RED), ephemeral=True)

    my_active_farms = []
    for key, data in active_farms.items():
        if data['user_id'] == ctx.author.id:
            my_active_farms.append(data)

    view = ProtectedView(ctx.author.id)
    if len(accounts) == 1:
        view.add_item(FarmTypeSelect(accounts[0], has_active_farms=bool(my_active_farms)))
        if my_active_farms:
            btn = Button(label="View Active Tasks", style=discord.ButtonStyle.primary, emoji=EMOJI_CHART, row=1)
            async def show_status_callback(inter: Interaction):
                current_active = [v for k, v in active_farms.items() if v['user_id'] == inter.user.id]
                if current_active:
                    s_view = FarmStatusView(current_active)
                    embed = build_embed(f"{EMOJI_FARM} Active Farms", "Manage running farms below.", DUO_GREEN)
                    await inter.response.send_message(embed=embed, view=s_view, ephemeral=True)
                else:
                    await inter.response.send_message(embed=build_embed("No Active Farms", None, DUO_ORANGE), ephemeral=True)
            btn.callback = show_status_callback
            view.add_item(btn)
        await ctx.respond(embed=build_embed(f"{EMOJI_FARM} Select Farm", None, DUO_BLUE), view=view, ephemeral=True)
    else:
        view.add_item(FarmAccountSelect(accounts, ctx.author.id, has_active_farms=bool(my_active_farms)))
        await ctx.respond(embed=build_embed(f"{EMOJI_DUO_RAIN} Select Account", None, DUO_BLUE), view=view, ephemeral=True)

@bot.slash_command(name="quests", description="Complete Daily, Monthly, and Past Quests")
async def quests_cmd(ctx: discord.ApplicationContext):
    await ctx.defer(ephemeral=True)
    user_doc = await users_collection.find_one({"_id": ctx.author.id})
    accounts = user_doc.get("accounts", []) if user_doc else []
    
    if not accounts: 
        return await ctx.respond(embed=build_embed("No Accounts", "No linked accounts.", DUO_RED), ephemeral=True)
    
    if len(accounts) == 1:
        view = QuestsView(accounts[0], ctx.author.id)
        embed = build_embed(f"{EMOJI_QUEST} Quests: {accounts[0]['username']}", "Select a quest action below.", DUO_GOLD)
        await ctx.respond(embed=embed, view=view, ephemeral=True)
    else:
        view = ProtectedView(ctx.author.id)
        view.add_item(QuestAccountSelect(accounts, ctx.author.id))
        await ctx.respond(embed=build_embed(f"{EMOJI_DUO_RAIN} Select Account", None, DUO_BLUE), view=view, ephemeral=True)

@bot.slash_command(name="saver", description="Manage Streak, League, and Quest Savers")
async def saver_cmd(ctx: discord.ApplicationContext):
    await ctx.defer(ephemeral=True)
    user_doc = await users_collection.find_one({"_id": ctx.author.id})
    if not user_doc or not user_doc.get("accounts"):
        await ctx.respond(embed=build_embed("No Accounts", "No accounts linked.", DUO_RED), ephemeral=True)
        return
    view = SaverView(user_doc, ctx.author.id)
    embed = await view.generate_embed()
    await ctx.respond(embed=embed, view=view, ephemeral=True)

@bot.slash_command(name="shop", description="Get items from the Duolingo Shop for Free")
async def shop_cmd(ctx: discord.ApplicationContext):
    await ctx.defer(ephemeral=True)
    user_doc = await users_collection.find_one({"_id": ctx.author.id})
    accounts = user_doc.get("accounts", []) if user_doc else []
    
    if not accounts: 
        return await ctx.respond(embed=build_embed("No Accounts", "No linked accounts.", DUO_RED), ephemeral=True)
    
    if len(accounts) == 1:
        view = ShopView(accounts[0], ctx.author.id)
        embed = build_embed(f"{EMOJI_SHOP} Duolingo Shop: {accounts[0]['username']}", "Select a category below.", DUO_GOLD)
        await ctx.respond(embed=embed, view=view, ephemeral=True)
    else:
        view = ProtectedView(ctx.author.id)
        view.add_item(ShopAccountSelect(accounts, ctx.author.id))
        await ctx.respond(embed=build_embed(f"{EMOJI_DUO_RAIN} Select Account", None, DUO_BLUE), view=view, ephemeral=True)

@bot.slash_command(name="admin", description="Admin Dashboard")
async def admin_cmd(ctx: discord.ApplicationContext):
    if ctx.author.id != ADMIN_ID: 
        return await ctx.respond(embed=build_embed(f"{EMOJI_CROSS} Unauthorized", "Access Denied.", DUO_RED), ephemeral=True)
    await ctx.defer(ephemeral=True)
    view = AdminView()
    embed = await view.get_stats_embed()
    await ctx.respond(embed=embed, view=view, ephemeral=True)

# --- QUEST VIEWS ---

class QuestAccountSelect(Select):
    def __init__(self, accounts, author_id):
        self.accounts = accounts
        self.author_id = author_id
        options = [SelectOption(label=acc['username'], value=str(i), emoji=EMOJI_DUO_RAIN) for i, acc in enumerate(accounts)]
        super().__init__(placeholder="Select Account for Quests...", min_values=1, max_values=1, options=options)
    
    async def callback(self, interaction: Interaction):
        acc = self.accounts[int(self.values[0])]
        view = QuestsView(acc, self.author_id)
        await interaction.response.edit_message(embed=build_embed(f"{EMOJI_QUEST} Quests: {acc['username']}", "Select a quest action below.", DUO_GOLD), view=view)

class QuestsView(ProtectedView):
    def __init__(self, account, author_id):
        super().__init__(author_id)
        self.account = account
        self.add_item(QuestActionSelect(account))

class QuestActionSelect(Select):
    def __init__(self, account):
        self.account = account
        options = [
            SelectOption(label="Complete Daily Quests", value="daily", emoji=EMOJI_CHECK, description="Finish today's 3 daily quests"),
            SelectOption(label="Complete Current Month", value="monthly_current", emoji=EMOJI_TROPHY, description="Finish this month's badge"),
            SelectOption(label="Complete ALL Previous", value="all_previous", emoji=EMOJI_CHEST, description="Unlock badges from past months")
        ]
        super().__init__(placeholder="Choose an Action...", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        mode = self.values[0]
        
        async with aiohttp.ClientSession() as session:
            result = await process_quests(session, self.account['jwt'], self.account['duo_id'], mode)
        
        color = DUO_GREEN if "Successfully" in result or "Processed" in result else DUO_RED
        await interaction.followup.send(embed=build_embed("Quest Result", result, color), ephemeral=True)

# --- SHOP VIEWS ---

class ShopAccountSelect(Select):
    def __init__(self, accounts, author_id):
        self.accounts = accounts
        self.author_id = author_id
        options = [SelectOption(label=acc['username'], value=str(i), emoji=EMOJI_DUO_RAIN) for i, acc in enumerate(accounts)]
        super().__init__(placeholder="Select Account for Shop...", min_values=1, max_values=1, options=options)
    async def callback(self, interaction: Interaction):
        acc = self.accounts[int(self.values[0])]
        view = ShopView(acc, self.author_id)
        await interaction.response.edit_message(embed=build_embed(f"{EMOJI_SHOP} Duolingo Shop: {acc['username']}", "Select a category below.", DUO_GOLD), view=view)

class ShopView(ProtectedView):
    def __init__(self, account, author_id):
        super().__init__(author_id)
        self.add_item(ShopCategorySelect(account))

class ShopCategorySelect(Select):
    def __init__(self, account):
        self.account = account
        self.cats = categorize_items()
        options = [
            SelectOption(label="XP Boosts", emoji=EMOJI_POTION, description="Refills and 15min boosts"),
            SelectOption(label="Health/Hearts", emoji=EMOJI_HEALTH, description="Shields and Unlimited Hearts"),
            SelectOption(label="Outfits", emoji=EMOJI_OUTFIT, description="Champagne and Formal suits"),
            SelectOption(label="Misc", emoji=EMOJI_MISC, description="Streak Freezes and more")
        ]
        super().__init__(placeholder="Choose a Category...", min_values=1, max_values=1, options=options)
    async def callback(self, interaction: Interaction):
        cat_name = self.values[0]
        items = self.cats.get(cat_name, [])
        if not items: return await interaction.response.send_message(embed=build_embed("No Items", "No items in this category.", DUO_ORANGE), ephemeral=True)
        view = View()
        view.add_item(ShopItemSelect(self.account, items, cat_name))
        await interaction.response.edit_message(embed=build_embed(f"{EMOJI_SHOP} {cat_name}", "Select an item to purchase.", DUO_BLUE), view=view)

class ShopItemSelect(Select):
    def __init__(self, account, items, category_name):
        self.account = account
        self.items = items
        options = []
        for item in items[:25]:
            options.append(SelectOption(label=item['name'][:100], value=item['id'], description=f"Cost: {item['price']} Gems"))
        super().__init__(placeholder=f"Select {category_name} Item...", min_values=1, max_values=1, options=options)
    async def callback(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        item_id = self.values[0]
        item_name = next((i['name'] for i in self.items if i['id'] == item_id), item_id)
        async with aiohttp.ClientSession() as session:
            p = await get_duo_profile(session, self.account['jwt'], self.account['duo_id'])
            if not p: return await interaction.followup.send(embed=build_embed("Error", "Could not fetch profile data.", DUO_RED), ephemeral=True)
            success = await purchase_shop_item(session, self.account['jwt'], self.account['duo_id'], item_id, p['fromLanguage'], p['learningLanguage'])
            if success: await interaction.followup.send(embed=build_embed(f"{EMOJI_CHECK} Purchase Successful", f"Bought **{item_name}**.", DUO_GREEN), ephemeral=True)
            else: await interaction.followup.send(embed=build_embed(f"{EMOJI_CROSS} Purchase Failed", f"Could not buy **{item_name}**.", DUO_RED), ephemeral=True)

class AccountSelect(Select):
    def __init__(self, accounts, parent_view):
        self.parent_view = parent_view
        self.accounts = accounts
        options = []
        for i, acc in enumerate(accounts):
            options.append(SelectOption(label=acc.get('username', 'Unknown'), value=str(i), emoji=EMOJI_DUO_RAIN, default=(i == parent_view.current_index)))
        super().__init__(placeholder="Switch Account...", min_values=1, max_values=1, options=options, row=0)
    async def callback(self, interaction: Interaction):
        await interaction.response.defer()
        idx = int(self.values[0])
        self.parent_view.current_index = idx
        await self.parent_view.refresh_dashboard(interaction)

class DashboardView(View):
    def __init__(self, user_doc, accounts):
        super().__init__(timeout=None)
        self.user_doc = user_doc
        self.accounts = accounts
        self.current_index = 0
        self.setup_items()

    def setup_items(self, disable_refresh=False):
        self.clear_items()
        if self.accounts: 
            self.add_item(AccountSelect(self.accounts, self))
        self.add_item(self.open_settings)
        self.add_item(self.add_account_btn)
        self.add_item(self.remove_account_btn)
        self.refresh_btn.disabled = disable_refresh
        self.add_item(self.refresh_btn)

    async def refresh_dashboard(self, interaction: Interaction, temp_disable=False):
        embed = await self.generate_embed()
        self.setup_items(disable_refresh=temp_disable)
        try:
            if interaction.response.is_done(): await interaction.edit_original_response(embed=embed, view=self)
            else: await interaction.response.edit_message(embed=embed, view=self)
        except: pass

    @discord.ui.button(label="Settings", style=discord.ButtonStyle.secondary, emoji=EMOJI_SETTINGS, row=1)
    async def open_settings(self, button: Button, interaction: Interaction):
        if not self.accounts: return await interaction.response.send_message(embed=build_embed("Error", "No accounts linked.", DUO_RED), ephemeral=True)
        current_acc = self.accounts[self.current_index]
        await interaction.response.send_message(embed=build_embed(f"{EMOJI_SETTINGS} Settings", f"Settings for **{current_acc.get('username')}**", DUO_BLUE), view=SettingsView(current_acc), ephemeral=True)
    
    @discord.ui.button(label="Add Account", style=discord.ButtonStyle.success, emoji="➕", row=1)
    async def add_account_btn(self, button: Button, interaction: Interaction):
        view = View()
        view.add_item(Button(label="Login (Email)", style=discord.ButtonStyle.green, custom_id="login_email"))
        view.add_item(Button(label="Login (Token)", style=discord.ButtonStyle.blurple, custom_id="login_token"))
        async def email_cb(inter): await inter.response.send_modal(EmailLoginModal())
        async def token_cb(inter): await inter.response.send_modal(JWTModal())
        view.children[0].callback = email_cb
        view.children[1].callback = token_cb
        await interaction.response.send_message("Select login method:", view=view, ephemeral=True)

    @discord.ui.button(label="Remove", style=discord.ButtonStyle.danger, emoji=EMOJI_TRASH, row=1)
    async def remove_account_btn(self, button: Button, interaction: Interaction):
        if not self.accounts: return await interaction.response.send_message(embed=build_embed("Error", "No accounts to remove.", DUO_RED), ephemeral=True)
        current_acc = self.accounts[self.current_index]
        await users_collection.update_one({"_id": interaction.user.id}, {"$pull": {"accounts": {"duo_id": current_acc['duo_id']}}})
        await interaction.response.send_message(embed=build_embed(f"{EMOJI_TRASH} Removed", f"Removed **{current_acc.get('username')}**.", DUO_ORANGE), ephemeral=True)

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.primary, emoji=EMOJI_REFRESH, row=1)
    async def refresh_btn(self, button: Button, interaction: Interaction):
        await interaction.response.defer()
        await self.refresh_dashboard(interaction, temp_disable=True)
        await asyncio.sleep(3)
        self.setup_items(disable_refresh=False)
        try: await interaction.edit_original_response(view=self)
        except: pass

    async def generate_embed(self):
        if not self.accounts: return build_embed("Dashboard", "No accounts linked. Click 'Add Account' to start.", DUO_DARK)
        acc = self.accounts[self.current_index]
        async with aiohttp.ClientSession() as session:
            profile = await get_duo_profile(session, acc['jwt'], acc['duo_id'])
            privacy_settings = await get_privacy_settings(session, acc['jwt'], acc['duo_id'])
        if not profile: return build_embed("Connection Issue", "Could not fetch data (Check headers/token).", DUO_ORANGE)
        username, xp, streak, gems = profile.get('username'), profile.get('totalXp'), profile.get('streak'), profile.get('gems')
        is_private = is_social_disabled(privacy_settings)
        privacy_str = f"{EMOJI_LOCK} Private" if is_private else f"{EMOJI_GLOBE} Public"
        embed = build_embed(f"{username} | Dashboard", None, DUO_GREEN)
        embed.add_field(name=f"{EMOJI_XP} Total XP", value=f"**{xp:,}**", inline=True)
        embed.add_field(name=f"{EMOJI_STREAK} Streak", value=f"**{streak:,}**", inline=True)
        embed.add_field(name=f"{EMOJI_GEM} Gems", value=f"**{gems:,}**", inline=True)
        embed.add_field(name="Status", value=f"**Privacy:** {privacy_str}", inline=False)
        return embed

class SettingsView(View):
    def __init__(self, account):
        super().__init__()
        self.account = account
        self.add_item(DelaySelect(account))

    @discord.ui.button(label="Toggle Privacy", style=discord.ButtonStyle.gray, emoji="👁️", row=1)
    async def toggle_privacy(self, button: Button, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        async with aiohttp.ClientSession() as session:
            privacy_settings = await get_privacy_settings(session, self.account['jwt'], self.account['duo_id'])
            is_private = is_social_disabled(privacy_settings)
            new_disable_social = not is_private
            success = await set_privacy_settings(session, self.account['jwt'], self.account['duo_id'], new_disable_social)
            if success:
                status_str = "Private 🔒" if new_disable_social else "Public 🌎"
                await interaction.followup.send(embed=build_embed("Privacy Updated", f"Profile is now **{status_str}**.", DUO_GREEN), ephemeral=True)
            else:
                await interaction.followup.send(embed=build_embed("Error", "Failed to update privacy settings.", DUO_RED), ephemeral=True)

class DelaySelect(Select):
    def __init__(self, account):
        self.account = account
        delays = account.get("delays", {})
        d_xp = delays.get("XP", 100)
        d_gem = delays.get("Gem", 100)
        d_str = delays.get("Streak", 100)
        
        options = [
            SelectOption(label=f"XP Delay ({d_xp}ms)", value="XP", emoji=EMOJI_XP),
            SelectOption(label=f"Gem Delay ({d_gem}ms)", value="Gem", emoji=EMOJI_GEM),
            SelectOption(label=f"Streak Delay ({d_str}ms)", value="Streak", emoji=EMOJI_STREAK)
        ]
        super().__init__(placeholder="Select Delay Type to Edit...", min_values=1, max_values=1, options=options, row=0)

    async def callback(self, interaction: Interaction):
        delay_type = self.values[0]
        await interaction.response.send_modal(DelayModal(self.account, delay_type))

class FarmTypeSelect(Select):
    def __init__(self, account, has_active_farms=False):
        self.account = account
        self.has_active_farms = has_active_farms
        delays = account.get("delays", {})
        xp_delay = delays.get("XP", 100)
        gem_delay = delays.get("Gem", 100)
        streak_delay = delays.get("Streak", 100)

        options = [
            SelectOption(label=f"XP Farm ({xp_delay} ms)", value="XP Farm", emoji=EMOJI_XP, description="Farm XP using High-Yield method"),
            SelectOption(label=f"Gem Farm ({gem_delay} ms)", value="Gem Farm", emoji=EMOJI_GEM, description="Farm Gems (60 per loop)"),
            SelectOption(label=f"Streak Farm ({streak_delay} ms)", value="Streak Farm", emoji=EMOJI_STREAK, description="Restore or increase streak")
        ]
        super().__init__(placeholder="Select a Farm Type...", min_values=1, max_values=1, options=options, row=0)
    
    async def callback(self, interaction: Interaction):
        farm_type_clean = self.values[0].replace(" Farm", "")
        type_key = "Gem" if "Gem" in farm_type_clean else farm_type_clean
        if type_key == "Gems": type_key = "Gem"
        current_delays = self.account.get("delays", {})
        delay = current_delays.get(type_key, 100)
        await interaction.response.send_modal(FarmModal(farm_type_clean, self.account['jwt'], self.account['duo_id'], self.account['username'], delay))

class FarmAccountSelect(Select):
    def __init__(self, accounts, author_id, has_active_farms=False):
        self.accounts = accounts
        self.author_id = author_id
        self.has_active_farms = has_active_farms
        options = [SelectOption(label=acc['username'], value=str(i), emoji=EMOJI_DUO_RAIN) for i, acc in enumerate(accounts)]
        super().__init__(placeholder="Select Account to Farm on...", min_values=1, max_values=1, options=options, row=0)
    async def callback(self, interaction: Interaction):
        acc = self.accounts[int(self.values[0])]
        view = ProtectedView(self.author_id)
        view.add_item(FarmTypeSelect(acc, self.has_active_farms))
        if self.has_active_farms:
            btn = Button(label="View Active Tasks", style=discord.ButtonStyle.primary, emoji=EMOJI_CHART, row=1)
            async def show_status_callback(inter: Interaction):
                current_active = [v for k, v in active_farms.items() if v['user_id'] == inter.user.id]
                if current_active:
                    s_view = FarmStatusView(current_active)
                    embed = build_embed(f"{EMOJI_FARM} Active Farms", "Manage running farms below.", DUO_GREEN)
                    await inter.response.send_message(embed=embed, view=s_view, ephemeral=True)
                else:
                    await inter.response.send_message(embed=build_embed("No Active Farms", None, DUO_ORANGE), ephemeral=True)
            btn.callback = show_status_callback
            view.add_item(btn)
        await interaction.response.edit_message(embed=build_embed(f"{EMOJI_FARM} Select Farm for {acc['username']}", None, DUO_BLUE), view=view)

class SaverView(View):
    def __init__(self, user_doc, author_id):
        super().__init__(timeout=None)
        self.user_doc = user_doc
        self.author_id = author_id
        self.accounts = user_doc.get("accounts", [])
        self.current_acc_idx = 0
        self.selected_mode = "streak"
        self.update_components()

    async def interaction_check(self, interaction: Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(embed=build_embed(f"{EMOJI_CROSS} Access Denied", "This menu is not for you.", DUO_RED), ephemeral=True)
            return False
        return True

    def get_current_acc(self):
        if not self.accounts: return None
        return self.accounts[self.current_acc_idx]

    def update_components(self):
        self.clear_items()
        acc = self.get_current_acc()
        if not acc: return

        acc_opts = [SelectOption(label=a['username'], value=str(i), default=(i==self.current_acc_idx)) for i, a in enumerate(self.accounts)]
        acc_sel = Select(placeholder="Select Account", options=acc_opts, row=0, custom_id="saver_acc")
        acc_sel.callback = self.on_acc_change
        self.add_item(acc_sel)

        mode_opts = [
            SelectOption(label="Streak Saver", value="streak", emoji=EMOJI_STREAK, default=(self.selected_mode=="streak"), description="Auto-repair streaks"),
            SelectOption(label="League Saver", value="league", emoji=EMOJI_TROPHY, default=(self.selected_mode=="league"), description="Maintain league position"),
            SelectOption(label="Quest Saver", value="quest", emoji=EMOJI_QUEST, default=(self.selected_mode=="quest"), description="Auto-complete daily quests")
        ]
        mode_sel = Select(placeholder="Saver Mode", options=mode_opts, row=1, custom_id="saver_mode")
        mode_sel.callback = self.on_mode_change
        self.add_item(mode_sel)

        if self.selected_mode == "streak":
            is_on = acc.get("streak_saver", False)
            style = discord.ButtonStyle.green if is_on else discord.ButtonStyle.danger
            label = "Streak Saver: ON" if is_on else "Streak Saver: OFF"
            btn = Button(label=label, style=style, row=2, custom_id="tog_streak")
            btn.callback = self.toggle_streak
            self.add_item(btn)
            btn_run = Button(label="Force Run Lesson", style=discord.ButtonStyle.secondary, emoji=EMOJI_RUN, row=2, custom_id="run_streak")
            btn_run.callback = self.force_streak
            self.add_item(btn_run)

        elif self.selected_mode == "league":
            is_on = acc.get("league_saver", False)
            style = discord.ButtonStyle.green if is_on else discord.ButtonStyle.danger
            label = "League Saver: ON" if is_on else "League Saver: OFF"
            btn = Button(label=label, style=style, row=2, custom_id="tog_league")
            btn.callback = self.toggle_league
            self.add_item(btn)
            if is_on:
                curr_target = acc.get("target_league_pos", 10)
                rank_opts = [SelectOption(label=f"Rank #{i}", value=str(i), default=(i==curr_target)) for i in range(1, 16)]
                rank_sel = Select(placeholder="Target Rank", options=rank_opts, row=3, custom_id="league_rank")
                rank_sel.callback = self.set_rank
                self.add_item(rank_sel)
                btn_run = Button(label="Force Reach Rank", style=discord.ButtonStyle.secondary, emoji=EMOJI_RUN, row=4, custom_id="run_league")
                btn_run.callback = self.force_league
                self.add_item(btn_run)
        
        elif self.selected_mode == "quest":
            is_on = acc.get("quest_saver", False)
            style = discord.ButtonStyle.green if is_on else discord.ButtonStyle.danger
            label = "Quest Saver: ON" if is_on else "Quest Saver: OFF"
            btn = Button(label=label, style=style, row=2, custom_id="tog_quest")
            btn.callback = self.toggle_quest
            self.add_item(btn)
            btn_run = Button(label="Force Complete Quests", style=discord.ButtonStyle.secondary, emoji=EMOJI_RUN, row=2, custom_id="run_quest")
            btn_run.callback = self.force_quest
            self.add_item(btn_run)

    async def generate_embed(self):
        acc = self.get_current_acc()
        if not acc: return build_embed("Saver Menu", "No accounts.", DUO_DARK)
        embed = build_embed(f"🛡️ Saver Menu: {acc['username']}", None, DUO_GREEN)
        s_stat = "✅ Active" if acc.get("streak_saver") else "❌ Inactive"
        l_stat = "✅ Active" if acc.get("league_saver") else "❌ Inactive"
        q_stat = "✅ Active" if acc.get("quest_saver") else "❌ Inactive"
        embed.add_field(name=f"{EMOJI_STREAK} Streak Saver", value=s_stat, inline=True)
        embed.add_field(name=f"{EMOJI_TROPHY} League Saver", value=l_stat, inline=True)
        embed.add_field(name=f"{EMOJI_QUEST} Quest Saver", value=q_stat, inline=True)
        if self.selected_mode == "league" and acc.get("league_saver"):
            embed.add_field(name="Target Rank", value=f"#{acc.get('target_league_pos', 10)}", inline=False)
        return embed

    async def on_acc_change(self, interaction: Interaction):
        self.current_acc_idx = int(interaction.data["values"][0])
        self.update_components()
        await interaction.response.edit_message(embed=await self.generate_embed(), view=self)

    async def on_mode_change(self, interaction: Interaction):
        self.selected_mode = interaction.data["values"][0]
        self.update_components()
        await interaction.response.edit_message(embed=await self.generate_embed(), view=self)

    async def toggle_streak(self, interaction: Interaction):
        acc = self.get_current_acc()
        new_val = not acc.get("streak_saver", False)
        await users_collection.update_one({"_id": self.author_id, "accounts.duo_id": acc["duo_id"]}, {"$set": {"accounts.$.streak_saver": new_val}})
        self.accounts[self.current_acc_idx]["streak_saver"] = new_val
        self.update_components()
        await interaction.response.edit_message(embed=await self.generate_embed(), view=self)

    async def toggle_league(self, interaction: Interaction):
        acc = self.get_current_acc()
        new_val = not acc.get("league_saver", False)
        await users_collection.update_one({"_id": self.author_id, "accounts.duo_id": acc["duo_id"]}, {"$set": {"accounts.$.league_saver": new_val}})
        self.accounts[self.current_acc_idx]["league_saver"] = new_val
        self.update_components()
        await interaction.response.edit_message(embed=await self.generate_embed(), view=self)
    
    async def toggle_quest(self, interaction: Interaction):
        acc = self.get_current_acc()
        new_val = not acc.get("quest_saver", False)
        await users_collection.update_one({"_id": self.author_id, "accounts.duo_id": acc["duo_id"]}, {"$set": {"accounts.$.quest_saver": new_val}})
        self.accounts[self.current_acc_idx]["quest_saver"] = new_val
        self.update_components()
        await interaction.response.edit_message(embed=await self.generate_embed(), view=self)

    async def set_rank(self, interaction: Interaction):
        val = int(interaction.data["values"][0])
        acc = self.get_current_acc()
        await users_collection.update_one({"_id": self.author_id, "accounts.duo_id": acc["duo_id"]}, {"$set": {"accounts.$.target_league_pos": val}})
        self.accounts[self.current_acc_idx]["target_league_pos"] = val
        self.update_components()
        await interaction.response.edit_message(embed=await self.generate_embed(), view=self)

    async def force_streak(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        acc = self.get_current_acc()
        async with aiohttp.ClientSession() as session:
            p = await get_duo_profile(session, acc['jwt'], acc['duo_id'])
            if p:
                if await perform_one_lesson(session, acc['jwt'], acc['duo_id'], p['fromLanguage'], p['learningLanguage']):
                     await interaction.followup.send(embed=build_embed(f"{EMOJI_CHECK} Success", "Streak Lesson Completed!", DUO_GREEN), ephemeral=True)
                else:
                     await interaction.followup.send(embed=build_embed(f"{EMOJI_CROSS} Failed", "Lesson Failed.", DUO_RED), ephemeral=True)

    async def force_league(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        acc = self.get_current_acc()
        target = acc.get("target_league_pos", 10)
        msg = await interaction.followup.send(embed=build_embed(f"{EMOJI_LOADING} Checking...", None, DUO_BLUE), ephemeral=True)
        async with aiohttp.ClientSession() as session:
            p = await get_duo_profile(session, acc['jwt'], acc['duo_id'])
            if not p: return
            res = await league_saver_logic(session, acc['jwt'], acc['duo_id'], target, p['fromLanguage'], p['learningLanguage'], msg)
            await msg.edit(embed=build_embed(f"{EMOJI_CHECK} Result", res, DUO_GREEN))
    
    async def force_quest(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        acc = self.get_current_acc()
        async with aiohttp.ClientSession() as session:
            # Completing daily and current monthly
            res1 = await process_quests(session, acc['jwt'], acc['duo_id'], "daily")
            res2 = await process_quests(session, acc['jwt'], acc['duo_id'], "monthly_current")
            
        color = DUO_GREEN if ("Successfully" in res1 or "Successfully" in res2) else DUO_RED
        await interaction.followup.send(embed=build_embed("Quest Result", f"**Daily:** {res1}\n**Monthly:** {res2}", color), ephemeral=True)

class AdminView(View):
    def __init__(self):
        super().__init__(timeout=None)

    async def get_stats_embed(self):
        total_users = await users_collection.count_documents({})
        pipeline = [{"$project": {"count": {"$size": {"$ifNull": ["$accounts", []]}}}}, {"$group": {"_id": None, "total": {"$sum": "$count"}}}]
        async for doc in users_collection.aggregate(pipeline):
            total_accounts = doc.get("total", 0)
            break
        else:
            total_accounts = 0
            
        active_streak = await self.get_active_saver_count("streak_saver")
        active_league = await self.get_active_saver_count("league_saver")
        active_quest = await self.get_active_saver_count("quest_saver")
        active_farm_count = len(active_farms)

        embed = build_embed(f"{EMOJI_ADMIN} Admin Control Panel", None, DUO_GOLD)
        embed.add_field(name=f"{EMOJI_PING} Latency", value=f"`{round(bot.latency * 1000)}ms`", inline=True)
        embed.add_field(name="⏱️ Uptime", value=f"`{get_uptime()}`", inline=True)
        embed.add_field(name=f"{EMOJI_WARNING} DB Docs", value=f"**{await users_collection.estimated_document_count():,}**", inline=True)
        embed.add_field(name="👥 Users", value=f"**{total_users}**", inline=True)
        embed.add_field(name=f"{EMOJI_ACCOUNTS} Accounts", value=f"**{total_accounts}**", inline=True)
        embed.add_field(name=f"{EMOJI_FARM} Active Farms", value=f"**{active_farm_count}**", inline=True)
        embed.add_field(name=f"{EMOJI_STREAK} Streak Savers", value=f"**{active_streak}**", inline=True)
        embed.add_field(name=f"{EMOJI_TROPHY} League Savers", value=f"**{active_league}**", inline=True)
        embed.add_field(name=f"{EMOJI_QUEST} Quest Savers", value=f"**{active_quest}**", inline=True)
        return embed

    async def get_active_saver_count(self, key):
        pipeline = [{"$match": {f"accounts.{key}": True}}, {"$project": {"count": {"$size": {"$filter": {"input": "$accounts", "as": "acc", "cond": f"$$acc.{key}"}}}}}, {"$group": {"_id": None, "total": {"$sum": "$count"}}}]
        async for doc in users_collection.aggregate(pipeline):
            return doc.get("total", 0)
        return 0

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, emoji=EMOJI_REFRESH, row=0)
    async def refresh_btn(self, button: Button, interaction: Interaction):
        await interaction.response.edit_message(embed=await self.get_stats_embed(), view=self)

    @discord.ui.button(label="View Users", style=discord.ButtonStyle.primary, emoji=EMOJI_USERS, row=0)
    async def view_users_btn(self, button: Button, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        cursor = users_collection.find({})
        users = await cursor.to_list(length=None)
        view = DBUserListView(users)
        await interaction.followup.send(embed=view.get_embed(), view=view, ephemeral=True)

    @discord.ui.button(label="Delete User", style=discord.ButtonStyle.danger, emoji="🚫", row=0)
    async def del_user_btn(self, button: Button, interaction: Interaction):
        await interaction.response.send_modal(AdminDeleteUserModal())

    @discord.ui.button(label="Run Streak Task", style=discord.ButtonStyle.success, emoji=EMOJI_STREAK, row=1)
    async def force_streak_task(self, button: Button, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        if not streak_monitor.is_running(): streak_monitor.start()
        else: streak_monitor.restart()
        await interaction.followup.send(embed=build_embed(f"{EMOJI_CHECK} Forced", "Streak Monitor routine forced.", DUO_GREEN), ephemeral=True)

    @discord.ui.button(label="Nuke DB", style=discord.ButtonStyle.danger, emoji=EMOJI_TRASH, row=1)
    async def nuke_btn(self, button: Button, interaction: Interaction):
        await interaction.response.send_modal(AdminNukeConfirm())

    @discord.ui.button(label="🛑 Disable ALL League", style=discord.ButtonStyle.danger, row=2)
    async def disable_all_league(self, button: Button, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        await users_collection.update_many({}, {"$set": {"accounts.$[].league_saver": False}})
        await interaction.followup.send(embed=build_embed(f"{EMOJI_CHECK} Disabled", "Turned OFF League Saver for everyone.", DUO_GREEN), ephemeral=True)

    @discord.ui.button(label="🧯 Disable ALL Streak", style=discord.ButtonStyle.danger, row=2)
    async def disable_all_streak(self, button: Button, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        await users_collection.update_many({}, {"$set": {"accounts.$[].streak_saver": False}})
        await interaction.followup.send(embed=build_embed(f"{EMOJI_CHECK} Disabled", "Turned OFF Streak Saver for everyone.", DUO_GREEN), ephemeral=True)

class AdminDeleteUserModal(Modal):
    def __init__(self, *args, **kwargs):
        super().__init__(title="DELETE SPECIFIC USER", *args, **kwargs)
        self.add_item(InputText(label="Discord User ID", placeholder="123456789...", required=True))

    async def callback(self, interaction: Interaction):
        try:
            uid = int(self.children[0].value)
            res = await users_collection.delete_one({"_id": uid})
            if res.deleted_count > 0:
                await interaction.response.send_message(embed=build_embed(f"{EMOJI_TRASH} Deleted", f"User `{uid}` deleted from database.", DUO_GREEN), ephemeral=True)
            else:
                await interaction.response.send_message(embed=build_embed(f"{EMOJI_CROSS} Not Found", f"User `{uid}` not found.", DUO_RED), ephemeral=True)
        except:
            await interaction.response.send_message(embed=build_embed(f"{EMOJI_CROSS} Error", "Invalid ID format.", DUO_RED), ephemeral=True)

class AdminNukeConfirm(Modal):
    def __init__(self, *args, **kwargs):
        super().__init__(title="CONFIRM DATABASE DELETION", *args, **kwargs)
        self.add_item(InputText(label="Type 'DELETE' to confirm", placeholder="DELETE", required=True))

    async def callback(self, interaction: Interaction):
        if self.children[0].value == "DELETE":
            await users_collection.delete_many({})
            await interaction.response.send_message(embed=build_embed(f"{EMOJI_TRASH} NUKED", "DATABASE CLEARED SUCCESSFULLY.", DUO_RED), ephemeral=True)
        else:
            await interaction.response.send_message(embed=build_embed(f"{EMOJI_CROSS} Aborted", "Verification failed.", DUO_RED), ephemeral=True)

class DBUserListView(View):
    def __init__(self, users_list):
        super().__init__()
        self.users = users_list
        self.page = 0
        self.per_page = 5
        self.max_page = max(0, (len(users_list) - 1) // self.per_page)
        self.update_buttons()

    def update_buttons(self):
        self.prev_btn.disabled = (self.page == 0)
        self.next_btn.disabled = (self.page == self.max_page)

    @discord.ui.button(emoji=EMOJI_PREV, style=discord.ButtonStyle.secondary)
    async def prev_btn(self, button: Button, interaction: Interaction):
        self.page -= 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

    @discord.ui.button(emoji=EMOJI_NEXT, style=discord.ButtonStyle.secondary)
    async def next_btn(self, button: Button, interaction: Interaction):
        self.page += 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

    def get_embed(self):
        start = self.page * self.per_page
        end = start + self.per_page
        subset = self.users[start:end]
        embed = build_embed(f"{EMOJI_DB} User Database ({len(self.users)})", None, DUO_GOLD)
        if not subset: embed.description = "No users found."
        else:
            desc = ""
            for u in subset:
                uid = u["_id"]
                accs = len(u.get("accounts", []))
                desc += f"**ID:** `{uid}`\n**Accounts:** {accs}\n`----------------`\n"
            embed.description = desc
        embed.set_footer(text=f"Page {self.page+1}/{self.max_page+1}")
        return embed

if __name__ == "__main__":
    if TOKEN: bot.run(TOKEN)
