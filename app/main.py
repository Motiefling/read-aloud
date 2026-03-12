from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.config import BASE_DIR
from app.database import init_db
from app.api.novels import router as novels_router
from app.api.chapters import router as chapters_router
from app.api.jobs import router as jobs_router
from app.api.dictionaries import router as dictionaries_router
from app.api.websocket import router as websocket_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    await init_db()
    yield


app = FastAPI(
    title="Light Novel Audiobook Server",
    description="Converts Chinese light novels into English audiobooks.",
    version="0.1.0",
    lifespan=lifespan,
)

# --- API Routes ---
app.include_router(novels_router, prefix="/api/novels", tags=["novels"])
app.include_router(chapters_router, prefix="/api/novels", tags=["chapters"])
app.include_router(jobs_router, prefix="/api/jobs", tags=["jobs"])
app.include_router(dictionaries_router, prefix="/api/dictionaries", tags=["dictionaries"])
app.include_router(websocket_router, tags=["websocket"])

# --- Serve PWA static files ---
pwa_dir = BASE_DIR / "pwa"
if pwa_dir.exists():
    app.mount("/", StaticFiles(directory=str(pwa_dir), html=True), name="pwa")
