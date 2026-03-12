from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter()


class ConnectionManager:
    """Manage WebSocket connections for push notifications."""

    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            await connection.send_json(message)


manager = ConnectionManager()


@router.websocket("/ws/notifications")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time job progress and completion alerts."""
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()  # keep connection alive
    except WebSocketDisconnect:
        manager.disconnect(websocket)


async def notify_chapter_complete(novel_id: str, chapter_num: int):
    """Notify connected clients that a chapter's audio is ready."""
    await manager.broadcast({
        "type": "chapter_complete",
        "novel_id": novel_id,
        "chapter_number": chapter_num,
    })


async def notify_novel_complete(novel_id: str):
    """Notify connected clients that an entire novel is ready."""
    await manager.broadcast({
        "type": "novel_complete",
        "novel_id": novel_id,
    })


async def notify_job_progress(job_id: str, progress_percent: float, current_step: str):
    """Notify connected clients of job progress updates."""
    await manager.broadcast({
        "type": "job_progress",
        "job_id": job_id,
        "progress_percent": progress_percent,
        "current_step": current_step,
    })
