# worker.py
import asyncio, os
from sqlmodel import select
from main import redis, async_session, Link

FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL", "5"))  # seconds

async def flush_counts_once():
    clicks = await redis.hgetall("clicks")  # {code: "3", ...}
    if not clicks:
        return
    async with async_session() as session:
        for code, count_str in clicks.items():
            q = select(Link).where(Link.short_code == code)
            r = await session.exec(q)
            link = r.one_or_none()
            if link:
                link.click_count = link.click_count + int(count_str)
                session.add(link)
        await session.commit()
    # remove flushed fields
    if clicks:
        await redis.hdel("clicks", *clicks.keys())

async def main_loop():
    while True:
        try:
            await flush_counts_once()
        except Exception as e:
            print("Worker error:", e)
        await asyncio.sleep(FLUSH_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main_loop())
