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
import traceback
import signal
import sys
from datetime import datetime, timedelta, timezone
from discord import app_commands, TextStyle, Interaction, SelectOption
from discord.ui import View, Button, Modal, TextInput, Select
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
DUO_GREEN = 0x58CC02
DUO_RED = 0xFF4B4B
DUO_BLUE = 0x1CB0F6
DUO_ORANGE = 0xFF9600
DUO_PURPLE = 0xCE82FF
DUO_DARK = 0x0F172A
DUO_GOLD = 0xF1C40F

# Custom Emojis
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

# --- DUOLINGO CONSTANTS ---
BASE_URL_V1 = "https://www.duolingo.com/2017-06-30"
BASE_URL_V2 = "https://www.duolingo.com/2023-05-23"
SESSIONS_URL = f"{BASE_URL_V1}/sessions"
STORIES_URL = "https://stories.duolingo.com/api2/stories"
LEADERBOARDS_URL = "https://duolingo-leaderboards-prod.duolingo.com/leaderboards/7d9f5dd1-8423-491a-91f2-2532052038ce"
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

# --- GLOBAL STATE MANAGEMENT ---
active_farms = {}
stop_reasons = {}

# --- SHOP DATA ---
RAW_SHOP_ITEMS = [
    {"id": "streak_freeze", "name": "Streak Freeze", "type": "misc", "price": 200, "currencyType": "XGM"},
    {"id": "streak_freeze_gift", "name": "Streak Freeze Gift", "type": "misc", "price": 20, "currencyType": "XGM"},
    {"id": "society_streak_freeze", "name": "Society Streak Freeze", "type": "misc", "price": 0, "currencyType": "XGM"},
    {"id": "duo_streak_freeze", "name": "Duo Streak Freeze", "type": "misc", "price": 0, "currencyType": "XGM"},
    {"id": "formal_outfit", "name": "Formal Attire", "type": "outfit", "price": 400, "currencyType": "XGM"},
    {"id": "luxury_outfit", "name": "Luxury Tracksuit", "type": "outfit", "price": 600, "currencyType": "XGM"},
    {"id": "health_shield", "name": "Health Shield", "type": "misc", "price": 500, "currencyType": "XGM"},
    {"id": "mastery_test", "name": "Mastery Test", "type": "misc", "price": 200, "currencyType": "XGM"},
    {"id": "skill_test", "name": "Skill Test", "type": "misc", "price": 200, "currencyType": "XGM"},
    {"id": "heart_segment", "name": "Heart Segment", "type": "misc", "price": 50, "currencyType": "XGM"},
    {"id": "heart_segment_reactive", "name": "Heart Segment Reactive", "type": "misc", "price": 100, "currencyType": "XGM"},
    {"id": "skill_test_gems", "name": "Skill Test Gems", "type": "misc", "price": 200, "currencyType": "XGM"},
    {"id": "skill_test_gems_200", "name": "Skill Test Gems 200", "type": "misc", "price": 200, "currencyType": "XGM"},
    {"id": "levels_pacing_gems", "name": "Levels Pacing", "type": "misc", "price": 400, "currencyType": "XGM"},
    {"id": "hard_mode_gems_5", "name": "Hard Mode (5)", "type": "misc", "price": 5, "currencyType": "XGM"},
    {"id": "hard_mode_gems_20", "name": "Hard Mode (20)", "type": "misc", "price": 20, "currencyType": "XGM"},
    {"id": "hard_mode_gems_50", "name": "Hard Mode (50)", "type": "misc", "price": 50, "currencyType": "XGM"},
    {"id": "mistakes_practice_gems_20", "name": "Mistakes Practice (20)", "type": "misc", "price": 20, "currencyType": "XGM"},
    {"id": "mistakes_practice_gems_200", "name": "Mistakes Practice (200)", "type": "misc", "price": 200, "currencyType": "XGM"},
    {"id": "mistakes_practice_gems_5", "name": "Mistakes Practice (5)", "type": "misc", "price": 5, "currencyType": "XGM"},
    {"id": "mistakes_practice_gems_50", "name": "Mistakes Practice (50)", "type": "misc", "price": 50, "currencyType": "XGM"},
    {"id": "pronunciation_review_10_pack", "name": "Pronunciation Review (10)", "type": "misc", "price": 300, "currencyType": "XGM"},
    {"id": "pronunciation_review_5_pack", "name": "Pronunciation Review (5)", "type": "misc", "price": 200, "currencyType": "XGM"},
    {"id": "row_blaster_150", "name": "Row Blaster 150", "type": "misc", "price": 150, "currencyType": "XGM"},
    {"id": "row_blaster_250", "name": "Row Blaster 250", "type": "misc", "price": 250, "currencyType": "XGM"},
    {"id": "legendary_keep_going", "name": "Legendary Keep Going", "type": "misc", "price": 50, "currencyType": "XGM"},
    {"id": "side_quest_entry", "name": "Side Quest Entry", "type": "misc", "price": 100, "currencyType": "XGM"},
    {"id": "daily_quest_reroll", "name": "Daily Quest Reroll", "type": "misc", "price": 20, "currencyType": "XGM"},
    {"id": "bookshelf_chapter_unlock", "name": "Bookshelf Unlock", "type": "misc", "price": 50, "currencyType": "XGM"},
    {"id": "final_level_attempt", "name": "Final Level Attempt", "type": "misc", "price": 100, "currencyType": "XGM"},
    {"id": "ramp_up_entry", "name": "Ramp Up Entry", "type": "misc", "price": 100, "currencyType": "XGM"},
    {"id": "gem_timer_boosts_1_450", "name": "Gem Timer Boost (1)", "type": "misc", "price": 450, "currencyType": "XGM"},
    {"id": "gem_timer_boosts_5_1800", "name": "Gem Timer Boost (5)", "type": "misc", "price": 1800, "currencyType": "XGM"},
    {"id": "gem_timer_boosts_15_4500", "name": "Gem Timer Boost (15)", "type": "misc", "price": 4500, "currencyType": "XGM"},
    {"id": "health_refill", "name": "Health Refill", "type": "misc", "price": 350, "currencyType": "XGM"},
    {"id": "health_refill_reactive", "name": "Health Refill Reactive", "type": "misc", "price": 450, "currencyType": "XGM"},
    {"id": "health_refill_partial_1", "name": "Partial Health (1)", "type": "misc", "price": 70, "currencyType": "XGM"},
    {"id": "health_refill_partial_2", "name": "Partial Health (2)", "type": "misc", "price": 140, "currencyType": "XGM"},
    {"id": "health_refill_partial_3", "name": "Partial Health (3)", "type": "misc", "price": 210, "currencyType": "XGM"},
    {"id": "health_refill_partial_4", "name": "Partial Health (4)", "type": "misc", "price": 280, "currencyType": "XGM"},
    {"id": "health_refill_discounted", "name": "Health Discounted", "type": "misc", "price": 50, "currencyType": "XGM"},
    {"id": "leaderboard_gem_wager_100", "name": "Gem Wager 100", "type": "misc", "price": 100, "currencyType": "XGM"},
    {"id": "leaderboard_gem_wager_50", "name": "Gem Wager 50", "type": "misc", "price": 50, "currencyType": "XGM"},
    {"id": "gem_wager", "name": "Double or Nothing", "type": "misc", "price": 50, "currencyType": "XGM"},
    {"id": "roleplay_path_free_taste", "name": "Roleplay Path Free Taste", "type": "misc", "price": 0, "currencyType": "XGM"},
    {"id": "video_call_path_free_taste", "name": "Video Call Path Free Taste", "type": "misc", "price": 0, "currencyType": "XGM"},
    {"id": "video_call_chest_free_taste", "name": "Video Call Chest Free", "type": "misc", "price": 0, "currencyType": "XGM"},
    {"id": "xp_boost_refill", "name": "XP Boost Refill", "type": "misc", "price": 100, "currencyType": "XGM"},
    {"id": "xp_boost_15_gift", "name": "XP Boost 15 Gift", "type": "misc", "price": 20, "currencyType": "XGM"},
    {"id": "xp_boost_gift", "name": "XP Boost Gift", "type": "misc", "price": 20, "currencyType": "XGM"},
    {"id": "xp_boost_stackable", "name": "XP Boost Stackable", "type": "misc", "price": 0, "currencyType": "XGM"},
    {"id": "general_xp_boost", "name": "General XP Boost", "type": "misc", "price": 750, "currencyType": "XGM"},
    {"id": "xp_boost_15", "name": "XP Boost 15m", "type": "misc", "price": 750, "currencyType": "XGM"},
    {"id": "xp_boost_60", "name": "XP Boost 60m", "type": "misc", "price": 750, "currencyType": "XGM"},
    {"id": "unlimited_hearts_boost", "name": "Unlimited Hearts", "type": "misc", "price": 750, "currencyType": "XGM"},
    {"id": "early_bird_xp_boost", "name": "Early Bird XP", "type": "misc", "price": 750, "currencyType": "XGM"}
]

# Initialize Bot
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)
start_time = time.time()
bot_is_stopping = False

# Initialize Database
try:
    mongo_client = AsyncIOMotorClient(MONGODB_URI, tlsCAFile=certifi.where())
    db = mongo_client["duo_streak_saver"]
    users_collection = db["users"]
    print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Connected to MongoDB.")
except Exception as e:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ MongoDB Connection Error: {e}")

# --- SIGNAL HANDLING (SHUTDOWN) ---
# We use a flag and a shutdown sequence task to ensure network operations complete.
def signal_handler(sig, frame):
    global bot_is_stopping
    if bot_is_stopping: return
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] [!] Ctrl+C detected. Initiating graceful shutdown...")
    bot_is_stopping = True
    asyncio.create_task(shutdown_sequence())

async def shutdown_sequence():
    """Cancels active farms and waits for them to send 'Stopped' logs before closing bot."""
    tasks_to_wait = []
    
    # Cancel all farms
    for key, data in list(active_farms.items()):
        if not data['task'].done():
            stop_reasons[key] = "Bot Shutdown"
            data['task'].cancel()
            tasks_to_wait.append(data['task'])
    
    # Wait for the cancellation blocks (finally/except CancelledError) to execute
    if tasks_to_wait:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Waiting for {len(tasks_to_wait)} farms to clean up...")
        # return_exceptions=True prevents one error from stopping the wait
        await asyncio.gather(*tasks_to_wait, return_exceptions=True)
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Cleanup complete. Closing bot connection.")
    await bot.close()

signal.signal(signal.SIGINT, signal_handler)

# --- HELPER FUNCTIONS ---

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
    if seconds < 60: return f"{int(seconds)}s"
    m = int(seconds // 60)
    s = int(seconds % 60)
    return f"{m}m {s}s"

def get_uptime():
    uptime_seconds = int(time.time() - start_time)
    days, remainder = divmod(uptime_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{days}d {hours}h {minutes}m {seconds}s"

async def update_account_delay(user_id, duo_id, delay_ms):
    await users_collection.update_one(
        {"_id": user_id, "accounts.duo_id": duo_id},
        {"$set": {"accounts.$.loop_delay": delay_ms}}
    )

def is_social_disabled(privacy_settings):
    if not privacy_settings: return False
    for s in privacy_settings:
        if isinstance(s, dict) and s.get('id') == 'disable_social':
            return s.get('enabled', False)
    return False

# --- CORE DUOLINGO ACTIONS ---

async def perform_one_lesson(session, jwt, user_id, from_lang, learning_lang):
    try:
        headers = get_headers(jwt, user_id)
        session_payload = {
            "challengeTypes": CHALLENGE_TYPES, "fromLanguage": from_lang, "learningLanguage": learning_lang,
            "type": "GLOBAL_PRACTICE", "isFinalLevel": False, "isV2": True, "juicy": True, "smartTipsVersion": 2
        }
        async with session.post(SESSIONS_URL, json=session_payload, headers=headers) as resp:
            if resp.status != 200: return False
            sess_data = await resp.json()
            
        session_id = sess_data.get('id')
        if not session_id: return False

        await asyncio.sleep(2) 

        now_ts = datetime.now(timezone.utc).timestamp()
        update_payload = {
            **sess_data, "heartsLeft": 5, "startTime": now_ts - 60, "endTime": now_ts,
            "failed": False, "maxInLessonStreak": 10, "shouldLearnThings": True, "enableBonusPoints": False
        }
        async with session.put(f"{SESSIONS_URL}/{session_id}", json=update_payload, headers=headers) as resp:
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
    cats = {"XP Boosts": [], "Health/Hearts": [], "Outfits": [], "Free Taste": [], "Misc": []}
    for item in RAW_SHOP_ITEMS:
        if item.get("currencyType") != "XGM": continue
        
        iid = item['id']
        name = item.get('name', iid.replace('_', ' ').title())
        item['name'] = name 
        
        if "xp_boost" in iid or "general_xp" in iid or "early_bird" in iid or "unlimited_hearts" in iid:
            cats["XP Boosts"].append(item)
        elif "health" in iid or "heart" in iid:
            cats["Health/Hearts"].append(item)
        elif "outfit" in iid or item['type'] == "outfit":
            cats["Outfits"].append(item)
        elif "free_taste" in iid:
            cats["Free Taste"].append(item)
        else:
            cats["Misc"].append(item)
    return cats

async def purchase_shop_item(session, jwt, user_id, item_id, from_lang, to_lang):
    headers = get_headers(jwt, user_id)
    
    if item_id == "xp_boost_refill":
        url = "https://ios-api-2.duolingo.com/2023-05-23/batch"
        headers["host"] = "ios-api-2.duolingo.com"
        inner_body = {
            "isFree": False, "learningLanguage": to_lang, "subscriptionFeatureGroupId": 0,
            "xpBoostSource": "REFILL", "xpBoostMinutes": 15, "xpBoostMultiplier": 3, "id": item_id
        }
        payload = {
            "includeHeaders": True,
            "requests": [{"url": f"/2023-05-23/users/{user_id}/shop-items", "extraHeaders": {}, "method": "POST", "body": json.dumps(inner_body)}]
        }
    else:
        url = f"{BASE_URL_V1}/users/{user_id}/shop-items"
        payload = {
            "itemName": item_id,
            "isFree": True, 
            "consumed": True,
            "fromLanguage": from_lang,
            "learningLanguage": to_lang
        }
    
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
        async with session.get(url, headers=headers) as resp:
            if resp.status != 200: return None
            data = await resp.json()
        
        active = data.get('active')
        if active is None:
            return {"banned": True}
            
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
            await users_collection.update_one(
                {"accounts.duo_id": user_id},
                {"$set": {"accounts.$.league_saver": False}}
            )
            return "❌ User is BANNED from Leagues. Saver disabled automatically."
        
        current_rank = data['rank']
        if current_rank <= target_rank:
            return f"Already at safe rank #{current_rank} (Target: #{target_rank}). No action needed."

        rankings = data['rankings']
        target_user = rankings[target_rank - 1] 
        target_score = target_user['score']
        my_score = data['score']
        
        xp_needed = (target_score - my_score) + 60
        if xp_needed <= 0: xp_needed = 40 

        if update_msg: 
            await update_msg.edit(embed=discord.Embed(
                title=f"{EMOJI_TROPHY} League Saver Active", 
                description=f"**Current:** #{current_rank} ({my_score} XP)\n**Target:** #{target_rank} ({target_score} XP)\n**Need:** ~{xp_needed} XP", 
                color=DUO_BLUE))

        last_edit = time.time()
        while farmed_session_xp < xp_needed:
            xp = await run_xp_story(session, jwt, from_lang, to_lang)
            if xp == 0: break 
            farmed_session_xp += xp
            
            if update_msg and (time.time() - last_edit > 8):
                 try:
                     await update_msg.edit(embed=discord.Embed(
                         title=f"{EMOJI_TROPHY} League Saver", 
                         description=f"Farming XP...\n**Gained:** {farmed_session_xp}/{xp_needed}", 
                         color=DUO_BLUE))
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
            await interaction.response.send_message(f"{EMOJI_CROSS} This menu is not for you.", ephemeral=True)
            return False
        return True

class JWTModal(Modal, title="Link with Token"):
    jwt_input = TextInput(label="JWT Token", style=TextStyle.paragraph, placeholder="Paste token...", required=True, min_length=20)
    async def on_submit(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        jwt = self.jwt_input.value.strip().replace('"', '')
        if not len(jwt.split('.')) == 3:
            await interaction.followup.send(embed=discord.Embed(title=f"{EMOJI_CROSS} Invalid Token", description="Invalid JWT structure.", color=DUO_RED), ephemeral=True)
            return
        await process_login(interaction, jwt)

class EmailLoginModal(Modal, title="Login with Credentials"):
    email = TextInput(label="Email/Username", placeholder="duo_fan_123", required=True)
    password = TextInput(label="Password", placeholder="Password...", required=True)
    async def on_submit(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        distinct_id = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(32))
        login_data = {"distinctId": distinct_id, "identifier": self.email.value, "password": self.password.value}
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post("https://www.duolingo.com/2017-06-30/login?fields=", json=login_data, headers=get_headers()) as response:
                    if response.status != 200:
                        await interaction.followup.send(embed=discord.Embed(title=f"{EMOJI_CROSS} Login Failed", color=DUO_RED), ephemeral=True)
                        return
                    jwt = response.headers.get('jwt')
                    if not jwt: return
                    await process_login(interaction, jwt)
            except Exception as e:
                await interaction.followup.send(f"Error: {e}", ephemeral=True)

class DelayModal(Modal, title="Set Farming Delay"):
    delay_input = TextInput(label="Delay (ms)", placeholder="100 (Default is 100ms)", required=True, max_length=5)
    def __init__(self, account):
        super().__init__()
        self.account = account
    async def on_submit(self, interaction: Interaction):
        try:
            val = int(self.delay_input.value)
            if val < 1: val = 1
            await update_account_delay(interaction.user.id, self.account['duo_id'], val)
            await interaction.response.send_message(f"✅ Delay updated to **{val}ms** for {self.account['username']}.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("❌ Invalid number.", ephemeral=True)

class FarmModal(Modal):
    def __init__(self, farm_type, jwt, duo_id, username, delay_ms):
        super().__init__(title=f"{farm_type} Farming")
        self.farm_type = farm_type
        self.jwt = jwt
        self.duo_id = str(duo_id)
        self.username = username
        self.delay_ms = delay_ms
        label_map = {"XP": "Amount of XP", "Gems": "Loops (60 gems/loop)", "Streak": "Days to add"}
        label_text = label_map.get(farm_type, "Amount")
        self.amount_input = TextInput(label=label_text, placeholder="e.g. 100", required=True)
        self.add_item(self.amount_input)
    
    async def on_submit(self, interaction: Interaction):
        farm_key = f"{self.duo_id}_{self.farm_type}"
        if self.farm_type == "Gems": farm_key = f"{self.duo_id}_Gem"
        
        if farm_key in active_farms:
            await interaction.response.send_message(f"❌ **{self.username}** is already running a {self.farm_type} farm! Wait for it to finish.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        try:
            amount = int(self.amount_input.value)
            if amount <= 0: raise ValueError
            if interaction.user.id != ADMIN_ID:
                if self.farm_type == "XP" and amount > 1000000:
                    await interaction.followup.send(f"{EMOJI_CROSS} Limit Reached! Max XP per run is **1,000,000**.", ephemeral=True)
                    return
                elif (self.farm_type in ["Gems", "Gem", "Streak"]) and amount > 10000:
                    await interaction.followup.send(f"{EMOJI_CROSS} Limit Reached! Max per run is **10,000**.", ephemeral=True)
                    return
        except:
            await interaction.followup.send("Invalid amount.", ephemeral=True)
            return
        
        async with aiohttp.ClientSession() as temp_sess:
            p = await get_duo_profile(temp_sess, self.jwt, self.duo_id)
            if not p: 
                await interaction.followup.send("❌ Could not connect to Duolingo.", ephemeral=True)
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
             await interaction.followup.send("Invalid farm type.", ephemeral=True)
             return
        
        active_farms[final_key] = {
            "type": self.farm_type,
            "task": task,
            "user_id": interaction.user.id,
            "username": self.username,
            "duo_id": self.duo_id,
            "target": amount,
            "progress": 0
        }

        await interaction.followup.send(f"{EMOJI_CHECK} **Started {self.farm_type} Farm** for `{self.username}`.\nCheck <#{FARM_LOG_CHANNEL_ID}> for logs.", ephemeral=True)

# --- FARMING & LOGGING LOGIC ---

def get_farm_log_channel():
    return bot.get_channel(FARM_LOG_CHANNEL_ID)

def get_saver_log_channel():
    return bot.get_channel(SAVER_LOG_CHANNEL_ID)

async def farm_xp_logic(discord_user_id, jwt, duo_id, username, amount, from_lang, to_lang, delay_ms):
    channel = get_farm_log_channel()
    discord_user = await bot.fetch_user(discord_user_id)
    farm_key = f"{duo_id}_XP"
    
    start_time_gmt = datetime.now(timezone.utc)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [XP Farm] STARTED for {username} (Target: {amount})")
    
    start_embed = discord.Embed(title=f"{EMOJI_XP} XP Farm Started", color=DUO_BLUE)
    start_embed.description = f"**User:** {discord_user.mention}\n**Account:** `{username}`\n**Target:** {amount} XP"
    start_embed.add_field(name="Time (GMT)", value=f"`{start_time_gmt.strftime('%Y-%m-%d %H:%M:%S')}`")
    start_embed.set_footer(text="Farming in progress...")
    if channel: await channel.send(embed=start_embed)

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
            finish_embed = discord.Embed(title=f"{EMOJI_CHECK} XP Farm Finished", color=DUO_GREEN)
            finish_embed.description = f"**User:** {discord_user.mention}\n**Account:** `{username}`"
            finish_embed.add_field(name="Gained", value=f"**+{total_xp_farmed} XP**", inline=True)
            finish_embed.add_field(name="Time Taken", value=f"`{elapsed_str}`", inline=True)
            finish_embed.set_footer(text=f"Completed at {datetime.now(timezone.utc).strftime('%H:%M GMT')}")
            
            if channel: await channel.send(embed=finish_embed)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [XP Farm] FINISHED for {username}")

    except asyncio.CancelledError:
        reason = stop_reasons.get(farm_key, "User Request")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [XP Farm] STOPPED for {username}. Reason: {reason}")
        if reason == "User Request":
             if channel: await channel.send(embed=discord.Embed(title=f"{EMOJI_STOP} Farm Stopped", description=f"XP Farm for `{username}` stopped manually by {discord_user.mention}.", color=DUO_RED))
        else:
            int_embed = discord.Embed(title=f"{EMOJI_STOP} Farm Interrupted", color=DUO_RED)
            int_embed.description = f"**Reason:** {reason}\n**Account:** `{username}`\n**User:** {discord_user.mention}"
            if channel: await channel.send(embed=int_embed)
            
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [XP Farm] ERROR for {username}: {e}")
        traceback.print_exc()
        int_embed = discord.Embed(title=f"{EMOJI_WARNING} Farm Error", color=DUO_RED)
        int_embed.description = f"**Error:** {str(e)}\n**Account:** `{username}`\n**User:** {discord_user.mention}"
        if channel: await channel.send(embed=int_embed)
    finally:
        if farm_key in active_farms: del active_farms[farm_key]
        if farm_key in stop_reasons: del stop_reasons[farm_key]

async def farm_gems_logic(discord_user_id, jwt, duo_id, username, loops, from_lang, to_lang, delay_ms):
    channel = get_farm_log_channel()
    discord_user = await bot.fetch_user(discord_user_id)
    farm_key = f"{duo_id}_Gem"

    start_time_gmt = datetime.now(timezone.utc)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [Gem Farm] STARTED for {username}")

    start_embed = discord.Embed(title=f"{EMOJI_GEM} Gem Farm Started", color=DUO_PURPLE)
    start_embed.description = f"**User:** {discord_user.mention}\n**Account:** `{username}`\n**Target:** {loops} Loops"
    start_embed.add_field(name="Time (GMT)", value=f"`{start_time_gmt.strftime('%Y-%m-%d %H:%M:%S')}`")
    if channel: await channel.send(embed=start_embed)

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
                            raise # This ensures the task stops immediately when cancelled
                        except Exception:
                            pass
                
                total_gems += (60 * 2) 
                await asyncio.sleep(sleep_time)

            end_clock = time.time()
            elapsed_str = format_time(end_clock - start_clock)
            finish_embed = discord.Embed(title=f"{EMOJI_CHECK} Gem Farm Finished", color=DUO_GREEN)
            finish_embed.description = f"**User:** {discord_user.mention}\n**Account:** `{username}`"
            finish_embed.add_field(name="Earned", value=f"**~{total_gems} Gems**", inline=True)
            finish_embed.add_field(name="Loops", value=f"{loops}", inline=True)
            finish_embed.add_field(name="Time Taken", value=f"`{elapsed_str}`", inline=True)
            finish_embed.set_footer(text=f"Completed at {datetime.now(timezone.utc).strftime('%H:%M GMT')}")
            
            if channel: await channel.send(embed=finish_embed)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [Gem Farm] FINISHED for {username}")

    except asyncio.CancelledError:
        reason = stop_reasons.get(farm_key, "User Request")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [Gem Farm] STOPPED for {username}. Reason: {reason}")
        if reason == "User Request":
             if channel: await channel.send(embed=discord.Embed(title=f"{EMOJI_STOP} Farm Stopped", description=f"Gem Farm for `{username}` stopped manually by {discord_user.mention}.", color=DUO_RED))
        else:
            int_embed = discord.Embed(title=f"{EMOJI_STOP} Farm Interrupted", color=DUO_RED)
            int_embed.description = f"**Reason:** {reason}\n**Account:** `{username}`\n**User:** {discord_user.mention}"
            if channel: await channel.send(embed=int_embed)
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [Gem Farm] ERROR for {username}: {e}")
        traceback.print_exc()
        int_embed = discord.Embed(title=f"{EMOJI_WARNING} Farm Error", color=DUO_RED)
        int_embed.description = f"**Error:** {str(e)}\n**Account:** `{username}`\n**User:** {discord_user.mention}"
        if channel: await channel.send(embed=int_embed)
    finally:
        if farm_key in active_farms: del active_farms[farm_key]
        if farm_key in stop_reasons: del stop_reasons[farm_key]

async def farm_streak_logic(discord_user_id, jwt, duo_id, username, amount, from_lang, to_lang, delay_ms):
    channel = get_farm_log_channel()
    discord_user = await bot.fetch_user(discord_user_id)
    farm_key = f"{duo_id}_Streak"

    start_time_gmt = datetime.now(timezone.utc)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [Streak Farm] STARTED for {username}")

    start_embed = discord.Embed(title=f"{EMOJI_STREAK} Streak Farm Started", color=DUO_ORANGE)
    start_embed.description = f"**User:** {discord_user.mention}\n**Account:** `{username}`\n**Target:** {amount} Days"
    start_embed.add_field(name="Time (GMT)", value=f"`{start_time_gmt.strftime('%Y-%m-%d %H:%M:%S')}`")
    if channel: await channel.send(embed=start_embed)

    try:
        start_clock = time.time()
        async with aiohttp.ClientSession() as session:
            profile = await get_duo_profile(session, jwt, duo_id)
            if not profile: return
            
            s_data = profile.get('streakData') or {}
            curr_streak = s_data.get('currentStreak')
            streak_start_str = curr_streak.get('startDate') if curr_streak else None
            
            if not streak_start_str: farm_start = datetime.now(timezone.utc) - timedelta(days=1)
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
                if farm_key in active_farms: active_farms[farm_key]['progress'] = success_cnt

                sim_day = farm_start - timedelta(days=i)
                session_payload = {"challengeTypes": CHALLENGE_TYPES, "fromLanguage": from_lang, "isFinalLevel": False, "isV2": True, "juicy": True, "learningLanguage": to_lang, "smartTipsVersion": 2, "type": "GLOBAL_PRACTICE"}
                
                async with session.post(SESSIONS_URL, headers=headers, json=session_payload, timeout=10) as resp:
                    if resp.status != 200: continue
                    sess_data = await resp.json()
                
                sess_id = sess_data.get('id')
                if not sess_id: continue

                start_ts = int((sim_day - timedelta(seconds=60)).timestamp())
                end_ts = int(sim_day.timestamp())
                update_payload = {**sess_data, "heartsLeft": 5, "startTime": start_ts, "endTime": end_ts, "enableBonusPoints": False, "failed": False, "maxInLessonStreak": 9, "shouldLearnThings": True}
                
                async with session.put(f"{SESSIONS_URL}/{sess_id}", headers=headers, json=update_payload, timeout=10) as resp:
                    if resp.status == 200: success_cnt += 1
                
                await asyncio.sleep(sleep_time)

            if farm_key in active_farms: active_farms[farm_key]['progress'] = success_cnt

            end_clock = time.time()
            elapsed_str = format_time(end_clock - start_clock)
            finish_embed = discord.Embed(title=f"{EMOJI_CHECK} Streak Farm Finished", color=DUO_GREEN)
            finish_embed.description = f"**User:** {discord_user.mention}\n**Account:** `{username}`"
            finish_embed.add_field(name="Restored", value=f"**{success_cnt} Days**", inline=True)
            finish_embed.add_field(name="Time Taken", value=f"`{elapsed_str}`", inline=True)
            finish_embed.set_footer(text=f"Completed at {datetime.now(timezone.utc).strftime('%H:%M GMT')}")
            
            if channel: await channel.send(embed=finish_embed)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [Streak Farm] FINISHED for {username}")

    except asyncio.CancelledError:
        reason = stop_reasons.get(farm_key, "User Request")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [Streak Farm] STOPPED for {username}. Reason: {reason}")
        if reason == "User Request":
             if channel: await channel.send(embed=discord.Embed(title=f"{EMOJI_STOP} Farm Stopped", description=f"Streak Farm for `{username}` stopped manually by {discord_user.mention}.", color=DUO_RED))
        else:
            int_embed = discord.Embed(title=f"{EMOJI_STOP} Farm Interrupted", color=DUO_RED)
            int_embed.description = f"**Reason:** {reason}\n**Account:** `{username}`\n**User:** {discord_user.mention}"
            if channel: await channel.send(embed=int_embed)
    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [Streak Farm] ERROR for {username}: {e}")
        traceback.print_exc()
        int_embed = discord.Embed(title=f"{EMOJI_WARNING} Farm Error", color=DUO_RED)
        int_embed.description = f"**Error:** {str(e)}\n**Account:** `{username}`\n**User:** {discord_user.mention}"
        if channel: await channel.send(embed=int_embed)
    finally:
        if farm_key in active_farms: del active_farms[farm_key]
        if farm_key in stop_reasons: del stop_reasons[farm_key]

async def process_login(interaction, jwt):
    duo_id = await extract_user_id(jwt)
    if not duo_id:
        await interaction.followup.send(embed=discord.Embed(description="Invalid Token.", color=DUO_RED), ephemeral=True)
        return

    user_doc = await users_collection.find_one({"_id": interaction.user.id})
    current_accounts = user_doc.get("accounts", []) if user_doc else []
    
    if len(current_accounts) >= 5 and interaction.user.id != ADMIN_ID:
        await interaction.followup.send(embed=discord.Embed(title="Limit Reached", description="You can only link up to 5 accounts.", color=DUO_RED), ephemeral=True)
        return

    existing_owner = await users_collection.find_one({"accounts.duo_id": duo_id})
    if existing_owner:
        if existing_owner['_id'] == interaction.user.id:
            await interaction.followup.send(embed=discord.Embed(description="You have already linked this account!", color=DUO_RED), ephemeral=True)
        else:
            await interaction.followup.send(embed=discord.Embed(title="❌ Account Taken", description="This account is already linked by another user.", color=DUO_RED), ephemeral=True)
        return
        
    async with aiohttp.ClientSession() as session:
        profile = await get_duo_profile(session, jwt, duo_id)
        if not profile: 
            username, xp, streak, gems = f"User_{duo_id}", 0, 0, 0
        else: 
            username, xp, streak, gems = profile.get('username'), profile.get('totalXp'), profile.get('streak'), profile.get('gems')
        
        await users_collection.update_one(
            {"_id": interaction.user.id}, 
            {"$push": {"accounts": {"duo_id": duo_id, "jwt": jwt, "username": username, "loop_delay": 100, "streak_saver": False, "league_saver": False}}},
            upsert=True
        )
        
        embed = discord.Embed(title=f"🎉 Welcome aboard, {username}!", description="Account linked successfully.", color=DUO_GREEN)
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

    print(f"[{datetime.now().strftime('%H:%M:%S')}] [Streak Monitor] Checking {users_to_check} accounts...")
    
    if users_to_check == 0: return

    start_time_gmt = datetime.now(timezone.utc)
    if channel:
        await channel.send(embed=discord.Embed(
            title=f"{EMOJI_STREAK} Streak Saver Routine",
            description=f"**Status:** Checking streaks...\n**Users to Check:** {users_to_check}\n**Time:** `{start_time_gmt.strftime('%H:%M GMT')}`",
            color=DUO_ORANGE
        ))

    saved_users = []
    async with aiohttp.ClientSession() as session:
        cursor = users_collection.find({})
        async for user_doc in cursor:
            discord_id = user_doc["_id"]
            for acc in user_doc.get("accounts", []):
                if not acc.get("streak_saver", False): continue
                if f"{acc['duo_id']}_Streak" in active_farms: continue 
                
                try:
                    p = await get_duo_profile(session, acc['jwt'], acc['duo_id'])
                    if not p: continue
                    
                    success = await perform_one_lesson(session, acc['jwt'], acc['duo_id'], p['fromLanguage'], p['learningLanguage'])
                    if success:
                        saved_users.append(discord_id)
                    
                    await asyncio.sleep(1.5) 
                except Exception: 
                    continue
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [Streak Monitor] Finished. Saved {len(saved_users)} users.")

    if channel:
        users_saved_count = len(saved_users)
        unique_saved = list(set(saved_users)) 
        mentions = " ".join([f"<@{uid}>" for uid in unique_saved]) if unique_saved else "None"
        
        embed = discord.Embed(title=f"{EMOJI_CHECK} Streak Saver Complete", color=DUO_GREEN)
        embed.add_field(name="Users Checked", value=f"{users_to_check}", inline=True)
        embed.add_field(name="Accounts Processed", value=f"{users_saved_count}", inline=True)
        if unique_saved:
             embed.add_field(name="Saved Users", value=mentions, inline=False)
        embed.set_footer(text=f"Next check in 4 hours.")
        
        await channel.send(embed=embed)

@tasks.loop(hours=3)
async def league_monitor():
    channel = get_saver_log_channel()

    pipeline = [{"$match": {"accounts.league_saver": True}}, {"$project": {"count": {"$size": {"$filter": {"input": "$accounts", "as": "acc", "cond": "$$acc.league_saver"}}}}}, {"$group": {"_id": None, "total": {"$sum": "$count"}}}]
    users_to_check = 0
    async for doc in users_collection.aggregate(pipeline):
        users_to_check = doc.get("total", 0)

    print(f"[{datetime.now().strftime('%H:%M:%S')}] [League Monitor] Checking {users_to_check} accounts...")
    
    if users_to_check == 0: return

    start_time_gmt = datetime.now(timezone.utc)
    if channel:
        await channel.send(embed=discord.Embed(
            title=f"{EMOJI_TROPHY} League Saver Routine",
            description=f"**Status:** Checking leaderboards...\n**Users to Check:** {users_to_check}\n**Time:** `{start_time_gmt.strftime('%H:%M GMT')}`",
            color=DUO_BLUE
        ))

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
                    
                    if "Finished" in str(result):
                        saved_users.append(discord_id)
                    elif "BANNED" in str(result):
                        print(f"🚫 Disabled League Saver for {acc['username']} (Banned)")
                    
                    await asyncio.sleep(3)
                except Exception: 
                    pass

    print(f"[{datetime.now().strftime('%H:%M:%S')}] [League Monitor] Finished. Saved {len(saved_users)} accounts.")

    if channel:
        unique_saved = list(set(saved_users))
        mentions = " ".join([f"<@{uid}>" for uid in unique_saved]) if unique_saved else "None"
        
        embed = discord.Embed(title=f"{EMOJI_CHECK} League Saver Complete", color=DUO_GREEN)
        embed.add_field(name="Users Checked", value=f"{users_to_check}", inline=True)
        embed.add_field(name="Accounts Saved", value=f"{len(saved_users)}", inline=True)
        if unique_saved:
             embed.add_field(name="Secured Users", value=mentions, inline=False)
        embed.set_footer(text=f"Next check in 3 hours.")
        
        await channel.send(embed=embed)

@bot.event
async def on_ready():
    print(f'[{datetime.now().strftime("%H:%M:%S")}] Logged in as {bot.user}')
    try: await bot.tree.sync()
    except: pass
    
    if not streak_monitor.is_running(): streak_monitor.start()
    if not league_monitor.is_running(): league_monitor.start()

# --- STATUS VIEW FOR /FARM ---

class FarmStatusView(View):
    def __init__(self, user_active_farms):
        super().__init__(timeout=None)
        self.user_active_farms = user_active_farms 

        for farm in self.user_active_farms:
            f_type = farm['type']
            d_id = farm['duo_id']
            key_suffix = f"{d_id}_{f_type}"
            if f_type == "Gems": key_suffix = f"{d_id}_Gem"
            
            btn = Button(label=f"{farm['username']} ({f_type})", style=discord.ButtonStyle.secondary, emoji=EMOJI_CHART)
            btn.callback = self.make_progress_callback(key_suffix)
            self.add_item(btn)

        has_xp = any(f['type'] == 'XP' for f in self.user_active_farms)
        has_gem = any(f['type'] in ['Gems', 'Gem'] for f in self.user_active_farms)
        has_streak = any(f['type'] == 'Streak' for f in self.user_active_farms)

        if has_xp:
            btn = Button(label="Stop XP", style=discord.ButtonStyle.danger, emoji=EMOJI_XP)
            btn.callback = self.stop_type_callback("XP")
            self.add_item(btn)
        
        if has_gem:
            btn = Button(label="Stop Gems", style=discord.ButtonStyle.danger, emoji=EMOJI_GEM)
            btn.callback = self.stop_type_callback("Gem") 
            self.add_item(btn)

        if has_streak:
            btn = Button(label="Stop Streak", style=discord.ButtonStyle.danger, emoji=EMOJI_STREAK)
            btn.callback = self.stop_type_callback("Streak")
            self.add_item(btn)

        btn_all = Button(label="Stop ALL", style=discord.ButtonStyle.danger, emoji=EMOJI_STOP, row=2)
        btn_all.callback = self.stop_all_callback
        self.add_item(btn_all)

    def make_progress_callback(self, key_suffix):
        async def callback(interaction: Interaction):
            target_key = key_suffix 
            if target_key not in active_farms:
                 await interaction.response.send_message("❌ Farm is no longer running.", ephemeral=True)
                 return
            
            data = active_farms[target_key]
            current = data['progress']
            total = data['target']
            bar = create_progress_bar(current, total)
            await interaction.response.send_message(f"**{data['username']}** ({data['type']}):\n{bar} ({current}/{total})", ephemeral=True)
        return callback

    def stop_type_callback(self, f_type):
        async def callback(interaction: Interaction):
            stopped_names = []
            print(f"[{datetime.now().strftime('%H:%M:%S')}] User {interaction.user} requested STOP for {f_type} farms.")
            
            keys_to_stop = []
            for key, data in active_farms.items():
                is_correct_type = (data['type'] == f_type) or (f_type == "Gem" and data['type'] == "Gems")
                if data['user_id'] == interaction.user.id and is_correct_type:
                    keys_to_stop.append(key)
                    stopped_names.append(data['username'])
            
            if not keys_to_stop:
                await interaction.response.send_message("❌ No active farms of that type found.", ephemeral=True)
                return

            await interaction.response.send_message(f"🛑 Stopping {f_type} farms for: {', '.join(stopped_names)}...", ephemeral=True)

            for key in keys_to_stop:
                if key in active_farms:
                    data = active_farms[key]
                    stop_reasons[key] = "User Request"
                    data['task'].cancel()

        return callback

    async def stop_all_callback(self, interaction: Interaction):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] User {interaction.user} requested STOP ALL.")
        count = 0
        keys_to_stop = []
        for key, data in active_farms.items():
            if data['user_id'] == interaction.user.id:
                keys_to_stop.append(key)
                count += 1
        
        await interaction.response.send_message(f"🛑 Stopping all {count} active farms...", ephemeral=True)
        
        for key in keys_to_stop:
            if key in active_farms:
                active_farms[key]['task'].cancel()
                stop_reasons[key] = "User Request"

# --- COMMANDS ---

@bot.tree.command(name="guide", description="Learn how to get your Duolingo JWT Token")
async def guide_cmd(interaction: Interaction):
    embed = discord.Embed(title="🎟️ How to get your Login Token", description="Steps to get your JWT Token from Duolingo web:", color=DUO_BLUE)
    embed.add_field(name="1️⃣ Log In", value="Go to [Duolingo.com](https://www.duolingo.com) on PC.", inline=False)
    embed.add_field(name="2️⃣ Console", value="Press **F12** -> **Console** tab.", inline=False)
    js_code = "document.cookie.match(new RegExp('(^| )jwt_token=([^;]+)'))[0].slice(11)"
    embed.add_field(name="3️⃣ Code", value=f"Paste this:\n```javascript\n{js_code}\n```", inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="dashboard", description="View profile and settings")
async def dashboard(interaction: Interaction):
    await interaction.response.defer(ephemeral=True)
    user_doc = await users_collection.find_one({"_id": interaction.user.id})
    if user_doc is None: user_doc = {"_id": interaction.user.id, "accounts": []}
    accounts = user_doc.get("accounts", [])
    view = DashboardView(user_doc, accounts)
    embed = await view.generate_embed()
    await interaction.followup.send(embed=embed, view=view, ephemeral=True)

@bot.tree.command(name="farm", description="Start or manage a farming session")
async def farm_cmd(interaction: Interaction):
    await interaction.response.defer(ephemeral=True)
    user_doc = await users_collection.find_one({"_id": interaction.user.id})
    accounts = user_doc.get("accounts", []) if user_doc else []
    
    if not accounts: 
        return await interaction.followup.send("No linked accounts.", ephemeral=True)

    my_active_farms = []
    for key, data in active_farms.items():
        if data['user_id'] == interaction.user.id:
            my_active_farms.append(data)

    view = ProtectedView(interaction.user.id)
    if len(accounts) == 1:
        view.add_item(FarmTypeSelect(accounts[0], has_active_farms=bool(my_active_farms)))
        
        if my_active_farms:
            btn = Button(label="View Active Tasks", style=discord.ButtonStyle.primary, emoji=EMOJI_CHART, row=1)
            async def show_status_callback(inter: Interaction):
                current_active = [v for k, v in active_farms.items() if v['user_id'] == inter.user.id]
                if current_active:
                    s_view = FarmStatusView(current_active)
                    embed = discord.Embed(title=f"{EMOJI_FARM} Active Farms", description="Manage running farms below.", color=DUO_GREEN)
                    await inter.response.send_message(embed=embed, view=s_view, ephemeral=True)
                else:
                    await inter.response.send_message("No active farms found.", ephemeral=True)
            btn.callback = show_status_callback
            view.add_item(btn)

        await interaction.followup.send(embed=discord.Embed(title=f"{EMOJI_FARM} Select Farm", color=DUO_BLUE), view=view)
    else:
        view.add_item(FarmAccountSelect(accounts, interaction.user.id, has_active_farms=bool(my_active_farms)))
        await interaction.followup.send(embed=discord.Embed(title=f"{EMOJI_DUO_RAIN} Select Account", color=DUO_BLUE), view=view)

@bot.tree.command(name="saver", description="Manage Streak and League Savers")
async def saver_cmd(interaction: Interaction):
    await interaction.response.defer(ephemeral=True)
    user_doc = await users_collection.find_one({"_id": interaction.user.id})
    if not user_doc or not user_doc.get("accounts"):
        await interaction.followup.send("No accounts linked.", ephemeral=True)
        return
    view = SaverView(user_doc, interaction.user.id)
    embed = await view.generate_embed()
    await interaction.followup.send(embed=embed, view=view, ephemeral=True)

@bot.tree.command(name="shop", description="Buy items from the Duolingo Shop for Free")
async def shop_cmd(interaction: Interaction):
    await interaction.response.defer(ephemeral=True)
    user_doc = await users_collection.find_one({"_id": interaction.user.id})
    accounts = user_doc.get("accounts", []) if user_doc else []
    
    if not accounts: 
        return await interaction.followup.send("No linked accounts.", ephemeral=True)
    
    if len(accounts) == 1:
        view = ShopView(accounts[0], interaction.user.id)
        embed = discord.Embed(title=f"{EMOJI_SHOP} Duolingo Shop: {accounts[0]['username']}", description="Select a category below.", color=DUO_GOLD)
        await interaction.followup.send(embed=embed, view=view)
    else:
        view = ProtectedView(interaction.user.id)
        view.add_item(ShopAccountSelect(accounts, interaction.user.id))
        await interaction.followup.send(embed=discord.Embed(title=f"{EMOJI_DUO_RAIN} Select Account", color=DUO_BLUE), view=view)

@bot.tree.command(name="test", description="Admin Dashboard (Restricted)")
async def test_cmd(interaction: Interaction):
    if interaction.user.id != ADMIN_ID: 
        return await interaction.response.send_message("❌ **Unauthorized Access.**", ephemeral=True)
    
    await interaction.response.defer(ephemeral=True)
    view = AdminView()
    embed = await view.get_stats_embed()
    await interaction.followup.send(embed=embed, view=view, ephemeral=True)

class ShopAccountSelect(Select):
    def __init__(self, accounts, author_id):
        self.accounts = accounts
        self.author_id = author_id
        options = [SelectOption(label=acc['username'], value=str(i), emoji=EMOJI_DUO_RAIN) for i, acc in enumerate(accounts)]
        super().__init__(placeholder="Select Account for Shop...", min_values=1, max_values=1, options=options)
    async def callback(self, interaction: Interaction):
        acc = self.accounts[int(self.values[0])]
        view = ShopView(acc, self.author_id)
        await interaction.response.edit_message(embed=discord.Embed(title=f"{EMOJI_SHOP} Duolingo Shop: {acc['username']}", description="Select a category below.", color=DUO_GOLD), view=view)

class ShopView(ProtectedView):
    def __init__(self, account, author_id):
        super().__init__(author_id)
        self.add_item(ShopCategorySelect(account))

class ShopCategorySelect(Select):
    def __init__(self, account):
        self.account = account
        self.cats = categorize_items()
        options = [
            SelectOption(label="XP Boosts", emoji=EMOJI_POTION),
            SelectOption(label="Health/Hearts", emoji=EMOJI_HEALTH),
            SelectOption(label="Outfits", emoji=EMOJI_OUTFIT),
            SelectOption(label="Free Taste", emoji=EMOJI_TASTE),
            SelectOption(label="Misc", emoji=EMOJI_MISC)
        ]
        super().__init__(placeholder="Choose a Category...", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: Interaction):
        cat_name = self.values[0]
        items = self.cats.get(cat_name, [])
        if not items:
            await interaction.response.send_message("No items in this category.", ephemeral=True)
            return
        
        view = View()
        view.add_item(ShopItemSelect(self.account, items, cat_name))
        await interaction.response.edit_message(embed=discord.Embed(title=f"{EMOJI_SHOP} {cat_name}", color=DUO_BLUE), view=view)

class ShopItemSelect(Select):
    def __init__(self, account, items, category_name):
        self.account = account
        self.items = items
        options = []
        for item in items[:25]:
            options.append(SelectOption(label=item['name'][:100], value=item['id']))
        
        super().__init__(placeholder=f"Select {category_name} Item...", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        item_id = self.values[0]
        item_name = next((i['name'] for i in self.items if i['id'] == item_id), item_id)
        
        async with aiohttp.ClientSession() as session:
            p = await get_duo_profile(session, self.account['jwt'], self.account['duo_id'])
            if not p: 
                await interaction.followup.send("❌ Could not fetch profile data.", ephemeral=True)
                return
            
            success = await purchase_shop_item(session, self.account['jwt'], self.account['duo_id'], item_id, p['fromLanguage'], p['learningLanguage'])
            
            if success:
                await interaction.followup.send(f"✅ Successfully purchased **{item_name}**!", ephemeral=True)
            else:
                await interaction.followup.send(f"❌ Failed to purchase **{item_name}**.", ephemeral=True)

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
            if interaction.response.is_done():
                await interaction.edit_original_response(embed=embed, view=self)
            else:
                await interaction.response.edit_message(embed=embed, view=self)
        except: pass

    @discord.ui.button(label="Settings", style=discord.ButtonStyle.primary, emoji=EMOJI_SETTINGS, row=1)
    async def open_settings(self, interaction: Interaction, button: Button):
        if not self.accounts: return await interaction.response.send_message("No accounts linked.", ephemeral=True)
        current_acc = self.accounts[self.current_index]
        await interaction.response.send_message(embed=discord.Embed(title=f"{EMOJI_SETTINGS} Settings for {current_acc.get('username')}", color=DUO_BLUE), view=SettingsView(current_acc), ephemeral=True)
    
    @discord.ui.button(label="Add Account", style=discord.ButtonStyle.success, emoji="➕", row=1)
    async def add_account_btn(self, interaction: Interaction, button: Button):
        view = View()
        view.add_item(Button(label="Login (Email)", style=discord.ButtonStyle.green, custom_id="login_email"))
        view.add_item(Button(label="Login (Token)", style=discord.ButtonStyle.blurple, custom_id="login_token"))
        async def email_cb(inter): await inter.response.send_modal(EmailLoginModal())
        async def token_cb(inter): await inter.response.send_modal(JWTModal())
        view.children[0].callback = email_cb
        view.children[1].callback = token_cb
        await interaction.response.send_message("Select login method:", view=view, ephemeral=True)

    @discord.ui.button(label="Remove", style=discord.ButtonStyle.danger, emoji=EMOJI_TRASH, row=1)
    async def remove_account_btn(self, interaction: Interaction, button: Button):
        if not self.accounts: return await interaction.response.send_message("No accounts to remove.", ephemeral=True)
        current_acc = self.accounts[self.current_index]
        await users_collection.update_one({"_id": interaction.user.id}, {"$pull": {"accounts": {"duo_id": current_acc['duo_id']}}})
        await interaction.response.send_message(f"Removed **{current_acc.get('username')}**.", ephemeral=True)

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, emoji=EMOJI_REFRESH, row=1)
    async def refresh_btn(self, interaction: Interaction, button: Button):
        await interaction.response.defer()
        await self.refresh_dashboard(interaction, temp_disable=True)
        await asyncio.sleep(3)
        self.setup_items(disable_refresh=False)
        try:
            await interaction.edit_original_response(view=self)
        except:
            pass

    async def generate_embed(self):
        if not self.accounts: return discord.Embed(title="Dashboard", description="No accounts linked.", color=DUO_DARK)
        acc = self.accounts[self.current_index]
        async with aiohttp.ClientSession() as session:
            profile = await get_duo_profile(session, acc['jwt'], acc['duo_id'])
            privacy_settings = await get_privacy_settings(session, acc['jwt'], acc['duo_id'])
        if not profile: return discord.Embed(title="Connection Issue", description="Could not fetch data (Check headers/token).", color=DUO_ORANGE)
        username, xp, streak, gems = profile.get('username'), profile.get('totalXp'), profile.get('streak'), profile.get('gems')
        is_private = is_social_disabled(privacy_settings)
        privacy_str = f"{EMOJI_LOCK} Private" if is_private else f"{EMOJI_GLOBE} Public"
        embed = discord.Embed(color=DUO_GREEN)
        embed.set_author(name=f"{username} | Dashboard")
        embed.add_field(name=f"{EMOJI_XP} Total XP", value=f"**{xp:,}**", inline=True)
        embed.add_field(name=f"{EMOJI_STREAK} Streak", value=f"**{streak:,}**", inline=True)
        embed.add_field(name=f"{EMOJI_GEM} Gems", value=f"**{gems:,}**", inline=True)
        embed.add_field(name="Status", value=f"**Privacy:** {privacy_str}", inline=False)
        return embed

class SettingsView(View):
    def __init__(self, account):
        super().__init__()
        self.account = account
    @discord.ui.button(label="Set Delay", style=discord.ButtonStyle.blurple, emoji="⏱️")
    async def set_delay(self, interaction: Interaction, button: Button):
        await interaction.response.send_modal(DelayModal(self.account))
    @discord.ui.button(label="Toggle Privacy", style=discord.ButtonStyle.gray, emoji="👁️")
    async def toggle_privacy(self, interaction: Interaction, button: Button):
        await interaction.response.defer(ephemeral=True)
        async with aiohttp.ClientSession() as session:
            privacy_settings = await get_privacy_settings(session, self.account['jwt'], self.account['duo_id'])
            is_private = is_social_disabled(privacy_settings)
            new_disable_social = not is_private
            success = await set_privacy_settings(session, self.account['jwt'], self.account['duo_id'], new_disable_social)
            if success:
                status_str = "Private 🔒" if new_disable_social else "Public 🌎"
                await interaction.followup.send(f"Profile is now **{status_str}**.", ephemeral=True)
            else:
                await interaction.followup.send("Failed to update privacy settings.", ephemeral=True)

class FarmTypeSelect(Select):
    def __init__(self, account, has_active_farms=False):
        self.account = account
        self.has_active_farms = has_active_farms
        options = [
            SelectOption(label="XP Farm", emoji=EMOJI_XP, description="Farm XP using High-Yield method"),
            SelectOption(label="Gem Farm", emoji=EMOJI_GEM, description="Farm Gems (60 per loop)"),
            SelectOption(label="Streak Farm", emoji=EMOJI_STREAK, description="Restore or increase streak")
        ]
        super().__init__(placeholder="Select a Farm Type...", min_values=1, max_values=1, options=options, row=0)
    async def callback(self, interaction: Interaction):
        farm_type = self.values[0].replace(" Farm", "")
        delay = self.account.get('loop_delay', 100)
        await interaction.response.send_modal(FarmModal(farm_type, self.account['jwt'], self.account['duo_id'], self.account['username'], delay))

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
                    embed = discord.Embed(title=f"{EMOJI_FARM} Active Farms", description="Manage running farms below.", color=DUO_GREEN)
                    await inter.response.send_message(embed=embed, view=s_view, ephemeral=True)
                else:
                    await inter.response.send_message("No active farms found.", ephemeral=True)
            btn.callback = show_status_callback
            view.add_item(btn)
        await interaction.response.edit_message(embed=discord.Embed(title=f"{EMOJI_FARM} Select Farm for {acc['username']}", color=DUO_BLUE), view=view)

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
            await interaction.response.send_message(f"{EMOJI_CROSS} This menu is not for you.", ephemeral=True)
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
            SelectOption(label="Streak Saver", value="streak", emoji=EMOJI_STREAK, default=(self.selected_mode=="streak")),
            SelectOption(label="League Saver", value="league", emoji=EMOJI_TROPHY, default=(self.selected_mode=="league"))
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

    async def generate_embed(self):
        acc = self.get_current_acc()
        if not acc: return discord.Embed(title="Saver Menu", description="No accounts.", color=DUO_DARK)
        
        embed = discord.Embed(title=f"🛡️ Saver Menu: {acc['username']}", color=DUO_GREEN)
        s_stat = "✅ Active" if acc.get("streak_saver") else "❌ Inactive"
        l_stat = "✅ Active" if acc.get("league_saver") else "❌ Inactive"
        
        embed.add_field(name=f"{EMOJI_STREAK} Streak Saver", value=s_stat, inline=True)
        embed.add_field(name=f"{EMOJI_TROPHY} League Saver", value=l_stat, inline=True)
        
        if self.selected_mode == "league" and acc.get("league_saver"):
            embed.add_field(name="Target Rank", value=f"#{acc.get('target_league_pos', 10)}", inline=False)
            embed.set_footer(text="Checks every 3 hours. Keeps you above target rank.")
        else:
            embed.set_footer(text="Streak Saver checks every 4 hours.")
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
                     await interaction.followup.send("✅ Streak Lesson Completed!", ephemeral=True)
                else:
                     await interaction.followup.send("❌ Lesson Failed.", ephemeral=True)

    async def force_league(self, interaction: Interaction):
        await interaction.response.defer(ephemeral=True)
        acc = self.get_current_acc()
        target = acc.get("target_league_pos", 10)
        msg = await interaction.followup.send(f"{EMOJI_LOADING} Checking League...", ephemeral=True)
        async with aiohttp.ClientSession() as session:
            p = await get_duo_profile(session, acc['jwt'], acc['duo_id'])
            if not p: return
            res = await league_saver_logic(session, acc['jwt'], acc['duo_id'], target, p['fromLanguage'], p['learningLanguage'], msg)
            await msg.edit(content=f"{EMOJI_CHECK} {res}", embed=None)

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
            
        active_streak = await self.get_active_streak_savers()
        active_league = await self.get_active_league_savers()
        active_farm_count = len(active_farms)

        embed = discord.Embed(title=f"{EMOJI_ADMIN} Admin Control Panel", color=DUO_GOLD)
        embed.add_field(name=f"{EMOJI_PING} Latency", value=f"`{round(bot.latency * 1000)}ms`", inline=True)
        embed.add_field(name="⏱️ Uptime", value=f"`{get_uptime()}`", inline=True)
        embed.add_field(name=f"{EMOJI_WARNING} DB Docs", value=f"**{await users_collection.estimated_document_count():,}**", inline=True)
        embed.add_field(name="👥 Users", value=f"**{total_users}**", inline=True)
        embed.add_field(name=f"{EMOJI_ACCOUNTS} Accounts", value=f"**{total_accounts}**", inline=True)
        embed.add_field(name=f"{EMOJI_FARM} Active Farms", value=f"**{active_farm_count}**", inline=True)
        embed.add_field(name=f"{EMOJI_STREAK} Active Streak Savers", value=f"**{active_streak}**", inline=True)
        embed.add_field(name=f"{EMOJI_TROPHY} Active League Savers", value=f"**{active_league}**", inline=True)
        embed.set_footer(text=f"Server Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return embed

    async def get_active_streak_savers(self):
        pipeline = [{"$match": {"accounts.streak_saver": True}}, {"$project": {"count": {"$size": {"$filter": {"input": "$accounts", "as": "acc", "cond": "$$acc.streak_saver"}}}}}, {"$group": {"_id": None, "total": {"$sum": "$count"}}}]
        async for doc in users_collection.aggregate(pipeline):
            return doc.get("total", 0)
        return 0

    async def get_active_league_savers(self):
        pipeline = [{"$match": {"accounts.league_saver": True}}, {"$project": {"count": {"$size": {"$filter": {"input": "$accounts", "as": "acc", "cond": "$$acc.league_saver"}}}}}, {"$group": {"_id": None, "total": {"$sum": "$count"}}}]
        async for doc in users_collection.aggregate(pipeline):
            return doc.get("total", 0)
        return 0

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, emoji=EMOJI_REFRESH, row=0)
    async def refresh_btn(self, interaction: Interaction, button: Button):
        await interaction.response.edit_message(embed=await self.get_stats_embed(), view=self)

    @discord.ui.button(label="View Users", style=discord.ButtonStyle.primary, emoji=EMOJI_USERS, row=0)
    async def view_users_btn(self, interaction: Interaction, button: Button):
        await interaction.response.defer(ephemeral=True)
        cursor = users_collection.find({})
        users = await cursor.to_list(length=None)
        view = DBUserListView(users)
        await interaction.followup.send(embed=view.get_embed(), view=view, ephemeral=True)

    @discord.ui.button(label="Delete User", style=discord.ButtonStyle.danger, emoji="🚫", row=0)
    async def del_user_btn(self, interaction: Interaction, button: Button):
        await interaction.response.send_modal(AdminDeleteUserModal())

    @discord.ui.button(label="Run Streak Task", style=discord.ButtonStyle.success, emoji=EMOJI_STREAK, row=1)
    async def force_streak_task(self, interaction: Interaction, button: Button):
        await interaction.response.defer(ephemeral=True)
        if not streak_monitor.is_running(): streak_monitor.start()
        else: streak_monitor.restart()
        await interaction.followup.send("✅ **Streak Monitor Forced.**", ephemeral=True)

    @discord.ui.button(label="Nuke DB", style=discord.ButtonStyle.danger, emoji=EMOJI_TRASH, row=1)
    async def nuke_btn(self, interaction: Interaction, button: Button):
        await interaction.response.send_modal(AdminNukeConfirm())

    @discord.ui.button(label="🛑 Disable ALL League", style=discord.ButtonStyle.danger, row=2)
    async def disable_all_league(self, interaction: Interaction, button: Button):
        await interaction.response.defer(ephemeral=True)
        await users_collection.update_many({}, {"$set": {"accounts.$[].league_saver": False}})
        await interaction.followup.send("✅ **Turned OFF League Saver for everyone.**", ephemeral=True)

    @discord.ui.button(label="🧯 Disable ALL Streak", style=discord.ButtonStyle.danger, row=2)
    async def disable_all_streak(self, interaction: Interaction, button: Button):
        await interaction.response.defer(ephemeral=True)
        await users_collection.update_many({}, {"$set": {"accounts.$[].streak_saver": False}})
        await interaction.followup.send("✅ **Turned OFF Streak Saver for everyone.**", ephemeral=True)

class AdminDeleteUserModal(Modal, title="DELETE SPECIFIC USER"):
    user_id = TextInput(label="Discord User ID", placeholder="123456789...", required=True)
    async def on_submit(self, interaction: Interaction):
        try:
            uid = int(self.user_id.value)
            res = await users_collection.delete_one({"_id": uid})
            if res.deleted_count > 0:
                await interaction.response.send_message(f"✅ User `{uid}` deleted from database.", ephemeral=True)
            else:
                await interaction.response.send_message(f"❌ User `{uid}` not found.", ephemeral=True)
        except:
            await interaction.response.send_message("❌ Invalid ID format.", ephemeral=True)

class AdminNukeConfirm(Modal, title="CONFIRM DATABASE DELETION"):
    confirm = TextInput(label="Type 'DELETE' to confirm", placeholder="DELETE", required=True)
    async def on_submit(self, interaction: Interaction):
        if self.confirm.value == "DELETE":
            await users_collection.delete_many({})
            await interaction.response.send_message(f"{EMOJI_TRASH} **DATABASE CLEARED.**", ephemeral=True)
        else:
            await interaction.response.send_message("❌ Verification failed. Aborted.", ephemeral=True)

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
    async def prev_btn(self, interaction: Interaction, button: Button):
        self.page -= 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

    @discord.ui.button(emoji=EMOJI_NEXT, style=discord.ButtonStyle.secondary)
    async def next_btn(self, interaction: Interaction, button: Button):
        self.page += 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

    def get_embed(self):
        start = self.page * self.per_page
        end = start + self.per_page
        subset = self.users[start:end]
        
        embed = discord.Embed(title=f"{EMOJI_DB} User Database ({len(self.users)})", color=DUO_GOLD)
        if not subset:
            embed.description = "No users found."
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
