import os
import json
import re
import time
import requests
import clickhouse_connect
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger("TelegramBot")

# ── CREDENTIALS ───────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN  = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHANNEL_ID = os.environ["TELEGRAM_CHANNEL_ID"]
GROQ_API_KEY        = os.environ.get("GROQ_API_KEY")
GEMINI_API_KEY      = os.environ.get("GEMINI_API_KEY")
OPENROUTER_API_KEY  = os.environ.get("OPENROUTER_API_KEY")
HF_TOKEN            = os.environ.get("HF_TOKEN")
CH_HOST             = os.environ["CLICKHOUSE_HOST"]
CH_USER             = os.environ["CLICKHOUSE_USER"]
CH_PASSWORD         = os.environ["CLICKHOUSE_PASSWORD"]
TELEGRAM_API_URL    = os.environ.get("TELEGRAM_API_URL", "https://api.telegram.org")

# ── DB ────────────────────────────────────────────────────────────────────────

def get_db_client():
    # Tinybird uses port 443, standard ClickHouse uses 8443
    port = 443 if "tinybird" in CH_HOST.lower() else 8443
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=port,
        username=CH_USER,
        password=CH_PASSWORD,
        secure=True
    )

POSTED_FILE = "posted_urls.txt"

def get_posted_urls():
    if os.path.exists(POSTED_FILE):
        with open(POSTED_FILE, "r") as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def save_posted_url(url):
    with open(POSTED_FILE, "a") as f:
        f.write(f"{url}\n")

def ensure_tables(client):
    if "tinybird" in CH_HOST.lower():
        # Tinybird does not support DDL (CREATE TABLE) via SQL driver
        return
    client.command("""
        CREATE TABLE IF NOT EXISTS telegram_posted_v2 (
            url String,
            title String,
            posted_at DateTime DEFAULT now()
        ) ENGINE = MergeTree() ORDER BY posted_at
    """)

def pick_next_article(client):
    """
    Pick exactly 1 un-posted article. Priority: Events -> GKG News -> Scraper.
    """
    is_tinybird = "tinybird" in CH_HOST.lower()
    posted_urls = get_posted_urls() if is_tinybird else set()

    # 1. GDELT Events (Geopolitical Events)
    try:
        limit = 50 if is_tinybird else 1
        where_clause = "" if is_tinybird else """
            WHERE source_url NOT IN (
                  SELECT url FROM telegram_posted_v2
                  UNION ALL
                  SELECT url FROM telegram_posted_urls
              )
              AND source_url != ''
        """
        rows = client.query(f"""
            SELECT event_label, actor1_name, actor2_name, location_name, source_url, avg_tone, event_type
            FROM gdelt_events
            {where_clause}
            ORDER BY ingested_at DESC
            LIMIT {limit}
        """).result_rows
        
        for r in rows:
            url = r[4]
            if is_tinybird and url in posted_urls:
                continue
            title = f"Geopolitical Event: {r[0]} ({r[1]} & {r[2]}) in {r[3]}"
            return {
                "source": "gdelt_event",
                "title": title, "url": url,
                "raw_topic": r[6] or "Conflict",
                "sentiment_score": float(r[5] or 0) / 10.0,
                "text": f"Event Label: {r[0]}, Actor 1: {r[1]}, Actor 2: {r[2]}, Location: {r[3]}, Source: {url}",
                "image_url": None
            }
    except Exception as e:
        logger.warning(f"GDELT Events pick failed: {e}")

    # 2. GDELT GKG News
    try:
        limit = 50 if is_tinybird else 1
        where_clause = "WHERE (locations LIKE '%ET%' OR themes LIKE '%ETHIOPIA%')"
        if not is_tinybird:
            where_clause += """
              AND source_url NOT IN (
                  SELECT url FROM telegram_posted_v2
                  UNION ALL
                  SELECT url FROM telegram_posted_urls
              )
            """
        rows = client.query(f"""
            SELECT title, source_url, themes, avg_tone, language, image_url
            FROM gdelt_gkg
            {where_clause}
            ORDER BY fetch_date DESC
            LIMIT {limit}
        """).result_rows
        
        for r in rows:
            url = r[1]
            if is_tinybird and url in posted_urls:
                continue
            return {
                "source": "gdelt",
                "title": r[0], "url": url,
                "raw_topic": map_gdelt_topic(r[2]),
                "sentiment_score": float(r[3] or 0) / 10.0,
                "text": r[0],
                "image_url": r[5] if len(r) > 5 else None
            }
    except Exception as e:
        logger.warning(f"GDELT GKG pick failed: {e}")

    # 3. Fallback: Scraper + Sentiment pipeline
    try:
        limit = 50 if is_tinybird else 1
        where_clause = "" if is_tinybird else """
            WHERE sr.url NOT IN (
                SELECT url FROM telegram_posted_v2
                UNION ALL
                SELECT url FROM telegram_posted_urls
            )
        """
        rows = client.query(f"""
            SELECT sr.title, sr.url, nt.topic, sr.sentiment_score_normalized, nt.translated_text
            FROM sentiment_results sr
            JOIN news_topics nt ON sr.doc_id = nt.doc_id
            {where_clause}
            ORDER BY nt.processed_at DESC
            LIMIT {limit}
        """).result_rows
        
        for r in rows:
            url = r[1]
            if is_tinybird and url in posted_urls:
                continue
            return {
                "source": "scraper",
                "title": r[0], "url": url,
                "raw_topic": r[2] or "Politics",
                "sentiment_score": float(r[3] or 0),
                "text": r[4] or r[0],
                "image_url": None
            }
    except Exception as e:
        logger.warning(f"Scraper pick failed: {e}")

    return None

def mark_posted(url, title, client):
    if "tinybird" in CH_HOST.lower():
        save_posted_url(url)
    else:
        safe_url   = url.replace("'", "''")
        safe_title = (title or "").replace("'", "''")
        client.command(f"INSERT INTO telegram_posted_v2 (url, title) VALUES ('{safe_url}', '{safe_title}')")

# ── HELPERS ───────────────────────────────────────────────────────────────────

def sanitize(text):
    if not text: return ""
    return text.encode("utf-8", errors="ignore").decode("utf-8").strip()

def map_gdelt_topic(themes):
    if not themes: return "Politics"
    t = str(themes).lower()
    if any(x in t for x in ["health", "medical", "disease"]):       return "Health"
    if any(x in t for x in ["econ", "trade", "business", "market"]): return "Business"
    if any(x in t for x in ["tech", "cyber", "digital"]):           return "Technology"
    if any(x in t for x in ["crime", "police", "prison", "corrupt"]): return "Crime"
    if any(x in t for x in ["conflict", "war", "attack", "militia", "rebel"]): return "Conflict"
    if any(x in t for x in ["humanitar", "refugee", "aid", "famine", "flood"]): return "Humanitarian"
    if any(x in t for x in ["env", "climate", "water", "drought"]): return "Environment"
    return "Politics"

# ── LLM PROVIDERS ─────────────────────────────────────────────────────────────

SYSTEM_MSG = (
    "You are a JSON-only news summarizer. Always respond with raw valid JSON, never markdown, never backticks. "
    "If Ethiopia is mentioned in the article, ensure the summary explicitly highlights that connection "
    "without making the entire article about Ethiopia (if it is a global article)."
)

def build_prompt(title, text):
    return f"""Summarize this news article or geopolitical event.

Respond with ONLY valid JSON in this exact format:
{{"language": "<full language name>", "title": "<clean English title>", "topic": "<Politics/Business/Health/Technology/Crime/Environment/Conflict/Humanitarian/Education>", "sentiment": "<Positive/Negative/Neutral>", "brief": "<3-4 sentence English summary. If Ethiopia is mentioned, include how it relates.>"}}

Article title: {title[:300]}
Article text: {text[:1500]}"""

def extract_json(raw):
    text = raw.strip().strip("`").strip()
    if text.startswith("json"): text = text[4:].strip()
    start, end = text.find("{"), text.rfind("}") + 1
    if start == -1: return None
    try:
        return json.loads(text[start:end])
    except Exception:
        return None

def call_groq(prompt):
    if not GROQ_API_KEY: raise Exception("No Groq key")
    models = ["llama-3.1-8b-instant", "llama-3.3-70b-versatile", "gemma2-9b-it"]
    for model in models:
        try:
            resp = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                json={"model": model, "messages": [
                    {"role": "system", "content": SYSTEM_MSG},
                    {"role": "user",   "content": prompt}
                ], "temperature": 0.0, "max_tokens": 500},
                timeout=20
            )
            if resp.status_code == 429:
                logger.warning(f"[Groq] model {model} rate limited (429), trying next...")
                continue
            if resp.status_code == 200:
                return resp.json()["choices"][0]["message"]["content"]
        except Exception as e:
            logger.warning(f"[Groq] model {model} failed: {e}")
    raise Exception("All Groq models failed")

def call_gemini(prompt):
    if not GEMINI_API_KEY: raise Exception("No Gemini key")
    models = ["gemini-2.0-flash", "gemini-1.5-flash-latest", "gemini-1.5-flash"]
    for model in models:
        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_API_KEY}"
            resp = requests.post(url, json={
                "contents": [{"parts": [{"text": f"{SYSTEM_MSG}\n\n{prompt}"}]}],
                "generationConfig": {"temperature": 0.0, "maxOutputTokens": 500}
            }, timeout=20)
            if resp.status_code == 429:
                logger.warning(f"[Gemini] {model} quota exceeded, trying next...")
                continue
            if resp.status_code == 200:
                return resp.json()["candidates"][0]["content"]["parts"][0]["text"]
        except Exception as e:
            logger.warning(f"[Gemini] {model} failed: {e}")
    raise Exception("All Gemini models failed")

def call_openrouter(prompt):
    if not OPENROUTER_API_KEY: raise Exception("No OpenRouter key")
    resp = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json"},
        json={"model": "meta-llama/llama-3-8b-instruct:free",
              "messages": [
                  {"role": "system", "content": SYSTEM_MSG},
                  {"role": "user",   "content": prompt}
              ], "temperature": 0.0, "max_tokens": 500},
        timeout=20
    )
    if resp.status_code == 200:
        return resp.json()["choices"][0]["message"]["content"]
    raise Exception(f"OpenRouter failed with {resp.status_code}: {resp.text}")

def call_hf(prompt):
    if not HF_TOKEN: raise Exception("No HF token")
    models = ["mistralai/Mistral-7B-Instruct-v0.3", "meta-llama/Meta-Llama-3-8B-Instruct"]
    for model in models:
        try:
            resp = requests.post(
                f"https://api-inference.huggingface.co/models/{model}",
                headers={"Authorization": f"Bearer {HF_TOKEN}"},
                json={"inputs": f"<s>[INST] {SYSTEM_MSG}\n\n{prompt} [/INST]", "parameters": {"max_new_tokens": 500, "temperature": 0.01}},
                timeout=30
            )
            if resp.status_code == 200:
                res = resp.json()
                if isinstance(res, list):
                    return res[0].get("generated_text", "")
                return str(res)
        except Exception as e:
            logger.warning(f"[HF] model {model} failed: {e}")
    raise Exception("All HF models failed")

def summarize(title, text):
    title = sanitize(title)
    text  = sanitize(text)
    if not title: return None

    prompt = build_prompt(title, text)
    for name, fn in [("Groq", call_groq), ("Gemini", call_gemini), ("OpenRouter", call_openrouter), ("HF", call_hf)]:
        try:
            raw    = fn(prompt)
            parsed = extract_json(raw)
            if not parsed:
                raise ValueError(f"No JSON from: {raw[:80]}")
            if not parsed.get("title") or not parsed.get("brief"):
                raise ValueError("Missing required fields")
            parsed["_provider"] = name
            logger.info(f"[{name}] summarized: {parsed.get('title','')[:60]}")
            return parsed
        except Exception as e:
            logger.warning(f"[{name}] failed: {e}")
            time.sleep(1)

    logger.error("All LLM providers failed.")
    return None

# ── TELEGRAM ──────────────────────────────────────────────────────────────────

def send_telegram(message, url, image_url=None):
    base_urls = [TELEGRAM_API_URL.rstrip('/'), "https://api.telegram.org"]
    seen = set()
    base_urls = [b for b in base_urls if not (b in seen or seen.add(b))]

    keyboard = {"inline_keyboard": [[{"text": "🔗 Read Full Article", "url": url}]]}
    last_error = ""

    for base in base_urls:
        try:
            # Always try sendPhoto first if we have an image
            if image_url and str(image_url).startswith("http"):
                try:
                    resp = requests.post(
                        f"{base}/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                        json={
                            "chat_id": TELEGRAM_CHANNEL_ID,
                            "photo": image_url,
                            "caption": message,
                            "parse_mode": "HTML",
                            "reply_markup": keyboard
                        },
                        timeout=12
                    )
                    if resp.status_code == 200:
                        return 200, resp.text
                    logger.warning(f"[{base}] sendPhoto {resp.status_code}, falling back to sendMessage")
                except Exception as pe:
                    logger.warning(f"[{base}] sendPhoto failed: {pe}, falling back to sendMessage")

            # Fallback: text-only
            resp = requests.post(
                f"{base}/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": TELEGRAM_CHANNEL_ID,
                    "text": message,
                    "parse_mode": "HTML",
                    "reply_markup": keyboard,
                    "disable_web_page_preview": False
                },
                timeout=15
            )
            if resp.status_code == 200:
                return 200, resp.text

            last_error = f"Status {resp.status_code}: {resp.text}"
            logger.warning(f"Failed sending to {base}: {last_error}")
        except Exception as e:
            last_error = str(e)
            logger.warning(f"Error sending to {base}: {last_error}")

    return 0, f"All Telegram endpoints failed. Last: {last_error}"

# ── MAIN ──────────────────────────────────────────────────────────────────────

TOPIC_EMOJI = {
    "Politics": "🏛️", "Business": "💼", "Health": "🏥",
    "Technology": "💻", "Crime": "🚔", "Environment": "🌿",
    "Conflict": "⚔️", "Humanitarian": "🤝", "Education": "📚"
}
SENTIMENT_EMOJI = {"Positive": "🟢", "Negative": "🔴", "Neutral": "🟡"}

def fetch_and_post():
    logger.info("=== fetch_and_post cycle ===")
    try:
        client = get_db_client()
        ensure_tables(client)
    except Exception as e:
        logger.error(f"DB connection failed: {e}")
        return

    article = pick_next_article(client)
    if not article:
        logger.info("No new un-posted articles found.")
        return

    title = article.get("title", "").strip()
    url   = article.get("url", "").strip()

    if not title or not url or len(title) < 5:
        logger.warning("Skipping — invalid article data.")
        return

    logger.info(f"Summarizing: {title[:80]}")
    analysis = summarize(title, article.get("text", title))

    if not analysis:
        mark_posted(url, title, client)
        logger.warning(f"LLM failed — skipping and marking: {title[:60]}")
        return

    tone = article.get("sentiment_score", 0)
    llm_sentiment = analysis.get("sentiment", "Neutral")
    if llm_sentiment == "Positive" or tone > 0.1:   sentiment_label = "Positive"
    elif llm_sentiment == "Negative" or tone < -0.1: sentiment_label = "Negative"
    else:                                             sentiment_label = "Neutral"

    lang     = analysis.get("language", "Unknown")
    title_en = analysis.get("title", title)
    topic    = analysis.get("topic", article.get("raw_topic", "Politics"))
    brief    = analysis.get("brief", "")
    provider = analysis.get("_provider", "LLM")
    image_url = article.get("image_url") or "https://images.unsplash.com/photo-1547496614-2c35848bb017?q=80&w=1000&auto=format&fit=crop"

    if not brief:
        mark_posted(url, title_en, client)
        logger.warning("Empty brief — skipping.")
        return

    # Extract source domain for display
    try:
        from urllib.parse import urlparse
        source_domain = urlparse(url).netloc.replace("www.", "")
    except Exception:
        source_domain = url

    s_emoji = SENTIMENT_EMOJI.get(sentiment_label, "🟡")

    # Format matches original style: title, metadata fields, Brief label, Source
    message = (
        f"<b>{title_en}</b>\n\n"
        f"Language: {lang}\n"
        f"Sentiment: {sentiment_label}\n"
        f"Topic: {topic}\n\n"
        f"Brief:\n{brief}\n\n"
        f"Source: {source_domain}\n\n"
        f"<b>Arki-news</b>\n"
        f"Be notified first: https://lnkd.in/eX74MsBC\n"
        f"Follow on X: https://x.com/ArkinewsET\n"
        f"Telegram: @arkinews"
    )

    status, resp_text = send_telegram(message, url, image_url)
    if status == 200:
        mark_posted(url, title_en, client)
        logger.info(f"✅ Posted via {provider}: {title_en[:60]}")
    else:
        logger.error(f"❌ Telegram {status}: {resp_text[:150]}")

    client.close()

if __name__ == "__main__":
    fetch_and_post()
