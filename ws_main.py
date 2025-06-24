from fastapi import FastAPI, HTTPException, Depends, Body, WebSocket, WebSocketDisconnect, Request
import asyncio
from database.connect_to_db import Session
from database.user import UserDB, UserService
from database.product import ProductDB, ProductService
from database.connect_to_db import test_db_connection, SessionLocal
import database.schemas as schemas
from database.defect import DefectDB
from database.camera import CameraDB, CameraService
from database.planning import PlanningDB
from database.model import DetectionModelDB
from database.transaction import TransactionDB
from database.report import ReportDB
from database.role import RoleDB
from database.permission import PermissionDB
from database.menu import MenuDB
from database.live_inspection import live_defect_ws_handler, active_websockets, pending_updates
from streaming.live_stream import setup_streaming, websocket_clients
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware 

app = FastAPI(
    title="PI Backend API",
    description="Backend service for managing users, roles, products, planning, inspection, and reporting.",
    version="1.0.0",
    openapi_tags=[
        {"name": "General", "description": "Health check and DB test"},
        {"name": "User", "description": "User management"},
        {"name": "Permission", "description": "Permission management"},
        {"name": "Role", "description": "Role management"},
        {"name": "Product", "description": "Product, product type management"},
        {"name": "Menu", "description": "Menu management"},
        {"name": "Camera", "description": "Camera configuration"},
        {"name": "Defect", "description": "Defect types"},
        {"name": "Planning", "description": "Production planning"},
        {"name": "Model", "description": "Detection model registry"},
        {"name": "Transaction", "description": "Lot and quantity tracking"},
        {"name": "Report", "description": "Report logs and results"},
        {"name": "Live", "description": "Live Inspection data"},
    ]
)

@app.on_event("startup")
def on_startup():
    # Register the current event loop for your kafka thread to use
    setup_streaming(asyncio.get_event_loop())

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # allow frontend IP or domains
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

user_db = UserDB()
product_db = ProductDB()
camera_db = CameraDB()
defect_db = DefectDB()
role_db = RoleDB()
permission_db = PermissionDB()
menu_db = MenuDB()

@app.websocket("/live-defect/{camera_id}")
async def live_defect(websocket: WebSocket, camera_id: str):
    db_gen = get_db()
    db = next(db_gen)
    await live_defect_ws_handler(websocket, camera_id, db)

@app.post("/live-defect-data/{camera_id}") # Endpoint to push live defect data from node-red
async def push_live_defect_data(camera_id: str, request: Request):
    data = await request.json()
    if camera_id in pending_updates:
        await pending_updates[camera_id].put(data)
        return {"status": "queued", "clients": len(active_websockets.get(camera_id, []))}
    return {"status": "no_active_socket"}

