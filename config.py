import os
from dotenv import load_dotenv

load_dotenv()


def _require(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise RuntimeError(f"Обязательная переменная окружения не задана: {key}")
    return value


# ── Telegram ──────────────────────────────────────────────
TOKEN = _require("TELEGRAM_TOKEN")
CHAT_ID = _require("TELEGRAM_CHAT_ID")

# ── База данных ───────────────────────────────────────────
DB_CONFIG = {
    "database": _require("DB_NAME"),
    "user":     _require("DB_USER"),
    "password": _require("DB_PASSWORD"),
    "host":     _require("DB_HOST"),
    "port":     int(os.getenv("DB_PORT", "5432")),
}

# ── Прокси ────────────────────────────────────────────────
_proxy_url = os.getenv("PROXY_URL", "")
PROXY = _proxy_url or None          # для aiohttp (строка или None)
PROXIES = {"http": _proxy_url, "https": _proxy_url} if _proxy_url else {}

# ── Поведение ─────────────────────────────────────────────
WITH_SEND: bool = os.getenv("WITH_SEND", "true").lower() == "true"

# ── HTTP-заголовки ────────────────────────────────────────
# Cookie берём из .env, чтобы не светить сессию в коде.
HEADERS: dict[str, str] = {
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/138.0.0.0 Safari/537.36"
    ),
    "Cookie": os.getenv("DIXY_COOKIE", ""),
}