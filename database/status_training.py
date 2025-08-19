import logging
from fastapi import WebSocket, WebSocketDisconnect
from database.connect_to_db import Session, text
from typing import Dict, List
from fastapi.encoders import jsonable_encoder
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
from queue import Queue
import math
import json
from sqlalchemy import text
import re

async def status_training_ws_handler(websocket: WebSocket, model_id: str, db: Session):
    await websocket.accept()
    
    async def fetch_and_send_data():
        try:
            sql = text("""
                SELECT
                    mv.modelversionid AS "modelVersionId",
                    mv.modelid,
                    mv.versionno,
                    mv.trainpercent,
                    mv.modelstatus,
                    mv.updateddate
                FROM public.modelversion mv
                WHERE mv.modelversionid = :model_id
                ORDER BY mv.versionno DESC
            """)
            result = db.execute(sql, {"model_id": model_id}).mappings().all()
            await websocket.send_json({"status": "success", "data": jsonable_encoder(result)})
        except Exception as e:
            logging.exception(f"Error fetching training status: {e}")
            await websocket.send_json({"status": "error", "message": str(e)})
    
    try:
        # Send initial data immediately after connection
        await fetch_and_send_data()
        
        while True:
            try:
                # Wait for message with timeout
                message = await asyncio.wait_for(websocket.receive_json(), timeout=10.0)
                
                # If message received, send updated data
                if message.get('action') == 'refresh':
                    await fetch_and_send_data()
                    
            except asyncio.TimeoutError:
                # No message received in 10 seconds, send periodic update
                await fetch_and_send_data()
                
    except WebSocketDisconnect:
        logging.info("WebSocket disconnected")
    except Exception as e:
        logging.exception(f"Error in status_training_ws_handler: {e}")
        try:
            await websocket.send_json({"status": "error", "message": str(e)})
        except:
            pass  # WebSocket might be closed
