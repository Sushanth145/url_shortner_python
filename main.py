# main.py
import os, string, socket, ipaddress
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, HttpUrl
from sqlmodel import SQLModel, Field, select
from sqlalchemy import Column, String, BigInteger
from sqlalchemy.ext.asyncio import create_async_engine
import asyncio
import ssl
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import redis.asyncio as aioredis
from urllib.parse import urlparse

# ---------- config ----------
DATABASE_URL = "postgresql+asyncpg://url_python_user:0A9mOcZiMD74Kmeh07LekWeP7O8M9A0s@dpg-d3i6hk3ipnbc73du3l70-a.oregon-postgres.render.com/url_python"

# Create SSL context
ssl_context = ssl.create_default_context()
# Optional: allow self-signed certificates if your server uses one
# ssl_context.check_hostname = False
# ssl_context.verify_mode = ssl.CERT_NONE

# Create async engine with SSL
engine = create_async_engine(
    DATABASE_URL,
    connect_args={"ssl": ssl_context},  # <- pass SSL here
    echo=True
)
REDIS_URL="rediss://default:AU86AAIncDI2OTIxNGVlOWVhOTQ0NWQ0OTdmMzk3N2UxMzU1MjIzNXAyMjAyODI@strong-imp-20282.upstash.io:6379"
BASE_HOST = os.getenv("BASE_HOST", "http://localhost:8000")
CACHE_TTL = int(os.getenv("CACHE_TTL", "86400"))  # seconds (default 1 day)

# ---------- DB & Redis ----------
engine = create_async_engine(DATABASE_URL, future=True, echo=False)
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
redis = aioredis.from_url(REDIS_URL, decode_responses=True)

# ---------- Model ----------
class Link(SQLModel, table=True):
    __tablename__ = "links"
    id: Optional[int] = Field(default=None, primary_key=True)
    short_code: Optional[str] = Field(default=None, sa_column=Column(String(64), unique=True, nullable=True))
    long_url: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = None
    is_active: bool = True
    click_count: int = Field(default=0, sa_column=Column(BigInteger, default=0))

# ---------- Base62 encoder ----------
ALPHABET = string.digits + string.ascii_letters
BASE = len(ALPHABET)
def encode_base62(num:int)->str:
    if num == 0: return ALPHABET[0]
    arr=[]
    while num>0:
        num, rem = divmod(num, BASE)
        arr.append(ALPHABET[rem])
    arr.reverse()
    return ''.join(arr)

# ---------- App ----------
app = FastAPI()

@app.get("/")
async def root():
    return {"message": "FastAPI is running!"}

@app.on_event("startup")
async def on_startup():
    # create tables if missing
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    # ensure redis reachable
    await redis.ping()

# ---------- simple SSRF check ----------
def is_private_ip(hostname: str) -> bool:
    try:
        for fam, _, _, _, sockaddr in socket.getaddrinfo(hostname, None):
            ip = sockaddr[0]
            ip_obj = ipaddress.ip_address(ip)
            if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_reserved or ip_obj.is_link_local:
                return True
        return False
    except Exception:
        return True  # be conservative on resolution failure

# ---------- request schemas ----------
class ShortenRequest(BaseModel):
    url: HttpUrl
    custom_alias: Optional[str] = None
    expiry_minutes: Optional[int] = None

class ShortenResponse(BaseModel):
    short_url: str

# ---------- endpoints ----------
@app.post("/shorten", response_model=ShortenResponse)
async def shorten(req: ShortenRequest):
    parsed = urlparse(req.url)
    if parsed.scheme not in ("http", "https"):
        raise HTTPException(status_code=400, detail="Only http/https URLs allowed")
    if not parsed.hostname or is_private_ip(parsed.hostname):
        raise HTTPException(status_code=400, detail="URL resolves to private/reserved address")

    async with async_session() as session:
        if req.custom_alias:
            q = select(Link).where(Link.short_code == req.custom_alias)
            r = await session.exec(q)
            if r.first():
                raise HTTPException(status_code=400, detail="custom_alias already taken")
            link = Link(short_code=req.custom_alias, long_url=req.url)
            if req.expiry_minutes:
                link.expires_at = datetime.utcnow() + timedelta(minutes=req.expiry_minutes)
            session.add(link)
            await session.commit()
            await session.refresh(link)
        else:
            link = Link(short_code=None, long_url=req.url)
            if req.expiry_minutes:
                link.expires_at = datetime.utcnow() + timedelta(minutes=req.expiry_minutes)
            session.add(link)
            await session.commit()
            await session.refresh(link)
            short_code = encode_base62(link.id)
            link.short_code = short_code
            session.add(link)
            await session.commit()

        # cache in redis
        await redis.set(f"link:{link.short_code}", link.long_url, ex=CACHE_TTL)
        return ShortenResponse(short_url=f"{BASE_HOST}/{link.short_code}")

@app.get("/{code}")
async def redirect(code: str):
    url = await redis.get(f"link:{code}")
    if not url:
        async with async_session() as session:
            q = select(Link).where(Link.short_code == code, Link.is_active == True)
            r = await session.exec(q)
            link = r.one_or_none()
            if not link:
                raise HTTPException(status_code=404, detail="Not found")
            if link.expires_at and link.expires_at < datetime.utcnow():
                raise HTTPException(status_code=410, detail="Expired")
            url = link.long_url
            await redis.set(f"link:{code}", url, ex=CACHE_TTL)
    # fast increment in redis (aggregated by worker)
    await redis.hincrby("clicks", code, 1)
    return RedirectResponse(url=url, status_code=302)

@app.get("/info/{code}")
async def info(code: str):
    async with async_session() as session:
        q = select(Link).where(Link.short_code == code)
        r = await session.exec(q)
        link = r.one_or_none()
        if not link:
            raise HTTPException(status_code=404, detail="Not found")
        pending = await redis.hget("clicks", code) or 0
        total_clicks = link.click_count + int(pending)
        return {
            "short_code": link.short_code,
            "long_url": link.long_url,
            "created_at": link.created_at,
            "expires_at": link.expires_at,
            "is_active": link.is_active,
            "click_count": total_clicks
        }
