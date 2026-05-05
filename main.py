"""
BOIFiler API — BOI Filing and Entity Lookup API
RapidAPI-ready FastAPI service with auth, rate limiting, and CORS.
"""
import os
import time
import json
import hmac
import hashlib
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request, Response, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from data_pipeline import (
    get_entity,
    search_entities,
    lookup_officer,
    compliance_check,
    get_cache_stats,
    refresh_cache,
)

# ── Configuration ─────────────────────────────────────────────

API_KEY = os.environ.get("BOIFILER_API_KEY", "boi_sk_live_demo_key")
RAPIDAPI_SECRET = os.environ.get("RAPIDAPI_SECRET", "")
RATE_LIMIT_PER_MIN = int(os.environ.get("RATE_LIMIT_PER_MIN", "60"))
CORS_ORIGINS = os.environ.get("CORS_ORIGINS", "*").split(",")

# ── Rate Limiter (in-memory sliding window) ───────────────────

_rate_limits: Dict[str, list] = {}

def check_rate_limit(client_id: str, max_requests: int = RATE_LIMIT_PER_MIN, window_sec: int = 60):
    now = time.time()
    if client_id not in _rate_limits:
        _rate_limits[client_id] = []
    
    # Prune old entries
    cutoff = now - window_sec
    _rate_limits[client_id] = [t for t in _rate_limits[client_id] if t > cutoff]
    
    if len(_rate_limits[client_id]) >= max_requests:
        raise HTTPException(
            status_code=429,
            detail={"error": "Rate limit exceeded", "retry_after_seconds": int(window_sec)}
        )
    
    _rate_limits[client_id].append(now)

# ── Auth Middleware ────────────────────────────────────────────

async def verify_auth(request: Request):
    """Verify API key from header or RapidAPI proxy."""
    # Check X-API-Key header
    api_key = request.headers.get("X-API-Key", "")
    
    # Check RapidAPI proxy secret
    rapidapi_proxy = request.headers.get("X-RapidAPI-Proxy-Secret", "")
    
    # Check query param (less secure, for testing)
    query_key = request.query_params.get("api_key", "")
    
    valid_key = api_key or rapidapi_proxy or query_key
    
    if not valid_key:
        # On RapidAPI, the proxy adds X-RapidAPI-Proxy-Secret
        # For demo, allow without key in dev mode
        if os.environ.get("DEV_MODE", "true").lower() == "true":
            return "anonymous"
        raise HTTPException(status_code=401, detail={"error": "Missing API key", "docs": "Provide X-API-Key header"})
    
    if valid_key != API_KEY:
        raise HTTPException(status_code=403, detail={"error": "Invalid API key"})
    
    return valid_key

# ── Lifespan ──────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: warm up cache. Shutdown: cleanup."""
    # Warm cache
    stats = get_cache_stats()
    print(f"[BOIFiler] Cache warmed: {stats['total_entities']} entities")
    yield

# ── FastAPI App ───────────────────────────────────────────────

app = FastAPI(
    title="BOIFiler API",
    description="BOI (Beneficial Ownership Information) filing and entity lookup API. "
                "Search companies, lookup officers, check BOI compliance status.\n\n"
                "**Authentication:** Pass your API key via `X-API-Key` header or `api_key` query parameter.\n"
                "**Rate Limit:** 60 requests/minute.\n"
                "**RapidAPI:** Deployed at RapidAPI — uses `X-RapidAPI-Proxy-Secret` for auth.",
    version="1.0.0",
    contact={
        "name": "BOIFiler Team",
        "url": "https://boifiler.com",
        "email": "support@boifiler.com",
    },
    license_info={
        "name": "MIT",
        "url": "https://opensource.org/licenses/MIT",
    },
    lifespan=lifespan,
)

# ── CORS ──────────────────────────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Middleware: rate limit + timing ────────────────────────────

@app.middleware("http")
async def middleware(request: Request, call_next):
    # Get client ID for rate limiting
    client_id = request.headers.get("X-API-Key", 
                  request.headers.get("X-RapidAPI-Proxy-Secret",
                  request.client.host if request.client else "unknown"))
    
    try:
        check_rate_limit(client_id)
    except HTTPException:
        return JSONResponse(
            status_code=429,
            content={"error": "rate_limit_exceeded", "message": "Too many requests. Limit: 60/min"},
            headers={"Retry-After": "60"}
        )
    
    start = time.time()
    response = await call_next(request)
    elapsed = time.time() - start
    
    response.headers["X-Response-Time"] = f"{elapsed:.3f}s"
    response.headers["X-BOIFiler-Version"] = "1.0.0"
    return response

# ── Pydantic Models ───────────────────────────────────────────

class HealthResponse(BaseModel):
    status: str
    version: str
    timestamp: str
    uptime: Optional[float] = None
    entities_count: int
    cache_age_seconds: int

class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None

# ── Endpoints ─────────────────────────────────────────────────

@app.get("/v1/health", response_model=HealthResponse)
async def health():
    """Health check endpoint — verify API is running."""
    stats = get_cache_stats()
    return HealthResponse(
        status="ok",
        version="1.0.0",
        timestamp=datetime.utcnow().isoformat() + "Z",
        entities_count=stats["total_entities"],
        cache_age_seconds=stats["cache_age_seconds"],
    )


@app.get("/v1/search")
async def search(
    request: Request,
    q: Optional[str] = Query(None, description="General search query (name, EIN, officer)"),
    name: Optional[str] = Query(None, description="Entity name search"),
    ein: Optional[str] = Query(None, description="EIN search (full or partial)"),
    state: Optional[str] = Query(None, description="State filter (2-letter code)"),
    boi_status: Optional[str] = Query(None, description="BOI status filter: 'filed', 'not_filed', 'exempt', 'required'"),
    naics: Optional[str] = Query(None, description="NAICS code filter"),
    limit: int = Query(20, ge=1, le=100, description="Results per page"),
    offset: int = Query(0, ge=0, description="Result offset"),
    _auth: str = Depends(verify_auth),
):
    """
    Search business entities by name, EIN, officer, or state.
    
    Returns paginated results with basic entity info and BOI status.
    
    Examples:
      GET /v1/search?q=apple
      GET /v1/search?state=CA&boi_status=not_filed
      GET /v1/search?ein=88-123
      GET /v1/search?name=Stripe&state=DE
    """
    result = search_entities(
        q=q, name=name, ein=ein,
        state=state, boi_status=boi_status,
        naics=naics, limit=limit, offset=offset,
    )
    return result


@app.get("/v1/entity/{ein}")
async def entity_detail(
    ein: str,
    _auth: str = Depends(verify_auth),
):
    """
    Get detailed entity information by EIN (Employer Identification Number).
    
    Returns full entity profile including:
    - Legal name, type, jurisdiction
    - Officers and their titles
    - Ownership structure
    - BOI filing status and exemption info
    - Financial/employee data (where available)
    - SEC filing status
    
    Examples:
      GET /v1/entity/13-4925230  (JPMorgan Chase)
      GET /v1/entity/88-1234567  (Main Street Bakery LLC)
    """
    ent = get_entity(ein)
    if not ent:
        raise HTTPException(status_code=404, detail={
            "error": "entity_not_found",
            "message": f"No entity found with EIN: {ein}",
            "hint": "Search via /v1/search to find entities"
        })
    return ent


@app.get("/v1/officer/{name:path}")
async def officer_lookup(
    name: str,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    _auth: str = Depends(verify_auth),
):
    """
    Look up officers and find entities associated with them.
    
    Searches by officer name (full or partial). Useful for:
    - Due diligence checks
    - Finding all companies linked to a person
    - BOI reporting — identifying beneficial owners
    
    Examples:
      GET /v1/officer/Elon%20Musk
      GET /v1/officer/Tim%20Cook
      GET /v1/officer/Smith
    """
    result = lookup_officer(name=name, limit=limit, offset=offset)
    return result


@app.get("/v1/compliance-check")
async def compliance(
    ein: Optional[str] = Query(None, description="EIN for compliance check"),
    name: Optional[str] = Query(None, description="Entity name for compliance check"),
    _auth: str = Depends(verify_auth),
):
    """
    Check BOI (Beneficial Ownership Information) compliance status.
    
    Returns:
    - Whether entity is exempt or must file
    - Current filing status
    - Applicable due dates
    - Full officer details for BOI reporting
    
    Examples:
      GET /v1/compliance-check?ein=88-1234567
      GET /v1/compliance-check?name=Main%20Street%20Bakery
    """
    if not ein and not name:
        raise HTTPException(status_code=400, detail={
            "error": "missing_parameter",
            "message": "Provide ein or name parameter"
        })
    
    result = compliance_check(ein=ein, name=name)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result)
    return result


@app.post("/v1/cache/refresh")
async def refresh_data_cache(
    _auth: str = Depends(verify_auth),
):
    """Force refresh the entity data cache."""
    result = refresh_cache()
    return {"status": "ok", "message": f"Cache refreshed. {result['count']} entities loaded."}


@app.get("/v1/stats")
async def stats(
    _auth: str = Depends(verify_auth),
):
    """Get API and data statistics."""
    cache_stats = get_cache_stats()
    return {
        "api_version": "1.0.0",
        "entities": cache_stats,
        "rate_limit": f"{RATE_LIMIT_PER_MIN} requests/minute",
        "data_source": "SEC EDGAR + curated entity database",
    }


# ── Root ──────────────────────────────────────────────────────

@app.get("/")
async def root():
    """API root — returns available endpoints."""
    return {
        "name": "BOIFiler API",
        "version": "1.0.0",
        "docs": "/docs",
        "openapi": "/openapi.json",
        "endpoints": {
            "search": "GET /v1/search?q=...&state=...&boi_status=...",
            "entity": "GET /v1/entity/{ein}",
            "officer": "GET /v1/officer/{name}",
            "compliance_check": "GET /v1/compliance-check?ein=...",
            "health": "GET /v1/health",
            "stats": "GET /v1/stats",
        },
        "auth": "X-API-Key header or api_key query parameter",
        "rate_limit": f"{RATE_LIMIT_PER_MIN} req/min",
    }


# ── Error Handlers ────────────────────────────────────────────

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.detail} if isinstance(exc.detail, dict) 
                else {"error": str(exc.detail)},
        headers=exc.headers or {},
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"error": "internal_server_error", "message": str(exc)},
    )


# ── Run ───────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
