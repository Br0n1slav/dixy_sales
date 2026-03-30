import asyncio
import json
import logging
import random
import time

import aiohttp
import asyncpg
import requests
from bs4 import BeautifulSoup

from config import CHAT_ID, DB_CONFIG, HEADERS, PROXY, PROXIES, TOKEN, WITH_SEND

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


async def parsing(session: aiohttp.ClientSession, pool: asyncpg.Pool):
    cards = []

    # TODO: иногда возвращает пустой список без ошибки, хз почему, просто ретраим
    while not cards:
        log.info("Запрашиваем каталог...")
        try:
            t = time.monotonic()
            resp = requests.get(
                "https://dixy.ru/ajax/listing-json.php",
                headers=HEADERS,
                params={"block": "product-list", "sid": "0", "perPage": "9999", "page": "1", "gl_filter": ""},
                proxies=PROXIES,
            )
            log.info("%.2fs", time.monotonic() - t)
            data = resp.json()
        except Exception as e:
            log.error("Ошибка запроса: %s", e)
            await asyncio.sleep(30)
            continue

        if not (data and data[0].get("cards")):
            log.warning("Пустой ответ, ждём...")
            await asyncio.sleep(30)
            continue

        cards = data[0]["cards"]

    log.info("Товаров: %d", len(cards))

    async with pool.acquire() as conn:
        to_delete = set()
        to_insert = set()
        bulk = []

        for p in cards:
            old_price = float((p.get("oldPriceSimple") or "0").replace(" ", ""))
            if not old_price:
                continue

            prod_id = int(p["id"])
            price = float(p["priceSimple"].replace(" ", ""))
            sale = round((1 - price / old_price) * 100, 2)

            title = p.get("title")
            section = p.get("section")
            url = p.get("url")
            image_url = p.get("src")
            amount = p.get("amount")
            symbol = p.get("symbol")
            prod_type = p.get("type")
            badges_raw = p.get("badges") or []
            badge = " ".join(b["title"] for b in badges_raw)
            badges = json.dumps(badges_raw, ensure_ascii=False, separators=(",", ":"))

            bulk.append([prod_id, title, p.get("brand"), section, prod_type,
                         url, image_url, badges, int(time.time()),
                         price, sale, old_price, amount, symbol])

            if sale <= 40:
                to_delete.add(prod_id)
                continue

            already = await conn.fetchval("SELECT 1 FROM sales WHERE prod_id = $1", prod_id)
            if already:
                continue

            to_insert.add(prod_id)
            log.info("[%.0f₽ / -%d%%] %s", price, round(sale), title)

            # Пытаемся стянуть БЖУ — если упадёт, не страшно
            k = b_val = j = u = ""
            try:
                await asyncio.sleep(random.random())
                async with session.post(f"https://dixy.ru{url}", headers=HEADERS, proxy=PROXY) as r:
                    soup = BeautifulSoup(await r.text(), "html.parser")
                    block = soup.find(class_="detail-tabs")
                    if block and (inner := block.find(class_="block")):
                        kbju = [i.text.strip().replace("\n", " ").replace(" г", "")
                                for i in inner.find_all(class_="block__wrap")]
                        k, b_val, j, u = [f"<b>{i.split()[0]}</b>-{i.split()[-1][0]}" for i in kbju]
            except Exception:
                pass  # без БЖУ тоже нормально

            if not WITH_SEND:
                continue

            text = (
                f"#{section or 'Другое'}\n"
                f"<code>{prod_type or '—'}</code>\n\n"
                f"{title}\n\n"
                f"<s>{old_price}</s> <b>{price}₽</b> "
                f"({'🔥' if sale >= 60 else ''}<i>-{round(sale)}%</i>)\n\n"
                f"{k.replace('К', ' Ккал')} {b_val} {j} {u}\n\n"
                f"<i>Доступно</i>: {amount} {symbol}\n"
                f"<i>Условия</i>: {badge or 'Нет'}"
            )
            async with session.post(
                f"https://api.telegram.org/bot{TOKEN}/sendPhoto",
                data={"chat_id": CHAT_ID, "photo": f"https://dixy.ru{image_url}",
                      "caption": text, "parse_mode": "HTML"},
            ) as tg:
                # TODO: нормально обработать ошибки тг (флуд лимит и т.д.)
                r = await tg.json()
                if not r.get("ok"):
                    log.warning("TG error: %s", r)
            await asyncio.sleep(3)

        # Пишем всё в БД пачкой
        if bulk:
            await conn.executemany("""
                INSERT INTO products (
                    prod_id, title, brand, section, prod_type, url, urlimage_url,
                    badges, created_at, price, sale, old_price, amount, symbol
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
                ON CONFLICT (prod_id) DO UPDATE SET
                    price=EXCLUDED.price, sale=EXCLUDED.sale, old_price=EXCLUDED.old_price,
                    amount=EXCLUDED.amount, badges=EXCLUDED.badges, created_at=EXCLUDED.created_at
            """, bulk)

        if to_delete:
            await conn.execute("DELETE FROM sales WHERE prod_id = ANY($1)", to_delete)
        if to_insert:
            await conn.executemany(
                "INSERT INTO sales (prod_id) VALUES ($1) ON CONFLICT DO NOTHING",
                [(pid,) for pid in to_insert],
            )

    log.info("Готово. Новых скидок: %d", len(to_insert))


async def create_db(pool: asyncpg.Pool):
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS products (
                prod_id      INT PRIMARY KEY,
                title        TEXT,
                brand        TEXT,
                section      TEXT,
                prod_type    TEXT,
                url          TEXT,
                urlimage_url TEXT,
                badges       TEXT,
                created_at   INT,
                price        NUMERIC(10,2),
                sale         NUMERIC(5,2),
                old_price    NUMERIC(10,2),
                amount       INT,
                symbol       TEXT,
                badge        TEXT
            );
            CREATE TABLE IF NOT EXISTS sales (prod_id INT PRIMARY KEY);
        """)


async def process(pool: asyncpg.Pool):
    timeout = aiohttp.ClientTimeout(total=90)
    async with aiohttp.ClientSession(timeout=timeout, cookie_jar=aiohttp.CookieJar()) as session:
        async with session.get("https://dixy.ru", headers=HEADERS, proxy=PROXY, allow_redirects=True) as r:
            log.info("PING: %s", r.status)
            if r.status != 200:
                # сайт нас забанил или сессия протухла
                await session.post(
                    f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                    data={"chat_id": CHAT_ID, "text": "⚠️ Дикси вернул не 200, нужна реавторизация"},
                )
                await asyncio.sleep(3600 * random.randint(1, 3))
                return

        await asyncio.sleep(random.random() * 10)
        await parsing(session, pool)


async def main():
    log.info("Коннектимся к БД...")
    pool = await asyncpg.create_pool(**DB_CONFIG)
    await create_db(pool)

    while True:
        try:
            await process(pool)
        except Exception as e:
            log.exception("Упали с ошибкой: %s", e)
            # TODO: сделать нормальный алерт в тг с traceback, но чтобы ночью не спамила
            await asyncio.sleep(1800)

        # Следующий запуск в начале следующего часа + 30 мин
        sleep_time = 3600 - time.time() % 3600 + 1800
        log.info("Следующий запуск через %.0f мин", sleep_time / 60)
        await asyncio.sleep(sleep_time)


if __name__ == "__main__":
    import platform
    if "Windows" in platform.platform():
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())