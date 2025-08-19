import asyncio
import base64
import numpy as np
from pathlib import Path
import cv2
from datetime import datetime
from kafka import KafkaConsumer
from threading import Thread
from database.connect_to_db import SessionLocal
from database.stream import get_live_inspection_data
from ultralytics import YOLO
# Global set for WebSocket connections
websocket_clients = dict()

def setup_streaming(loop=None):
    global main_loop
    main_loop = loop or asyncio.get_event_loop()
    print(f"[System] setup_streaming: Registered main_loop={main_loop}")

async def broadcast_frame(frame):
    if not websocket_clients:
        print("[Broadcast] No clients to broadcast.")
        return

    b64_frame = frame.decode('utf-8')
    disconnected = set()
    for ws, meta in list(websocket_clients.items()):
        camera_id = meta.get("camera_id")
        try:
            db = SessionLocal()
            inspection_data = get_live_inspection_data(camera_id, db)
            db.close()
            payload = {
                "frame": b64_frame,
                "inspection": inspection_data
            }
            await ws.send_json(payload)
            # print(f"[Broadcast] Frame + inspection sent to camera_id={camera_id}")
        except Exception as e:
            print(f"[Broadcast] Error sending to client: {e}")
            disconnected.add(ws)
    for ws in disconnected:
        websocket_clients.pop(ws, None)
        print("[Broadcast] Removed disconnected client.")

def add_websocket(ws):
    websocket_clients.add(ws)
    print(f"[WebSocket] Client connected. Total: {len(websocket_clients)}")

def remove_websocket(ws):
    websocket_clients.discard(ws)
    print(f"[WebSocket] Client disconnected. Total: {len(websocket_clients)}")

