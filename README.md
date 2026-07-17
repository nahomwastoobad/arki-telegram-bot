# ARKI Telegram Bot 🇪🇹

Automated Ethiopia 360° Intelligence Platform news poster for Telegram. Runs via GitHub Actions every 5 minutes.

## How It Works
1. GitHub Actions triggers `bot.py` every 5 minutes
2. `bot.py` connects to ClickHouse, picks 1 un-posted article (Events → News → Scraper)
3. LLM summarizes it (Groq → Gemini → OpenRouter → HF fallback chain)
4. Message posted to Telegram channel via Cloudflare Worker proxy

## Required GitHub Secrets
Go to **Settings → Secrets and variables → Actions → New repository secret** and add:

| Secret Name | Value |
|---|---|
| `TELEGRAM_BOT_TOKEN` | Your bot token |
| `TELEGRAM_CHANNEL_ID` | Your channel ID (e.g. `-1004413681582`) |
| `TELEGRAM_API_URL` | Your Cloudflare Worker URL |
| `GROQ_API_KEY` | Groq API key |
| `GEMINI_API_KEY` | Gemini API key |
| `OPENROUTER_API_KEY` | OpenRouter API key |
| `HF_TOKEN` | Hugging Face token |
| `CLICKHOUSE_HOST` | ClickHouse host |
| `CLICKHOUSE_USER` | ClickHouse username |
| `CLICKHOUSE_PASSWORD` | ClickHouse password |
