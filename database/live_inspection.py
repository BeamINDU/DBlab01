import logging
import os
import base64
import pytz
import database.lot_manager
from datetime import datetime
from fastapi import WebSocket, WebSocketDisconnect
from database.connect_to_db import SessionLocal, text
from typing import Dict, List
import asyncio

# --- Workflow context ---
# active_websockets: holds clients per camera
# pending_updates: data to send to clients
# lots_today: loaded in-memory cache from lot_manager.py
bangkok = pytz.timezone("Asia/Bangkok")

logging.basicConfig(level=logging.INFO)

active_websockets: Dict[str, List[WebSocket]] = {}
pending_updates: Dict[str, List[asyncio.Queue]] = {}

IGNORE_KEYS = {
    "frame_id", "liveStream", "planid", "actualProduct", "box", "label",
    "prodid", "prodlot", "camid", "seq", "expected"
}

def load_function_to_defect_mapping():
    db = SessionLocal()
    mapping = {}
    try:
        rows = db.execute(text("SELECT defectid, functionmapping FROM defecttype")).mappings().all()
        for row in rows:
            defectid = row["defectid"]
            mapping_str = row["functionmapping"]
            if not mapping_str:
                continue
            # Handle comma-separated functionid list
            func_ids = [int(fid.strip()) for fid in mapping_str.split(",") if fid.strip().isdigit()]
            for fid in func_ids:
                mapping[fid] = defectid
    except Exception as e:
        logging.error(f"[INIT] Failed to load defecttype mapping: {e}")
    finally:
        db.close()
    return mapping

# dynamic mapping with query
FUNCTIONID_TO_DEFECTID = load_function_to_defect_mapping()

def make_image_path(planid, prodlot, prodid, seq=None):
    if seq is not None:
        file_name = f"{planid}_{prodlot}_{prodid}_{seq}.jpg"
    return file_name

def extract_detection_info(merged_data):
    detection_info = []
    for key, value in merged_data.items():
        if key in IGNORE_KEYS:
            continue
        if not isinstance(value, dict):
            continue
        predicted = value.get("predictedResult", "")
        if isinstance(predicted, list):
            predicted = predicted[0] if predicted else ""
        detection_info.append({
            "function": value.get("function", key),
            "predicted": str(predicted),
            "expected": str(value.get("expected", "")),
            "confident": value.get("conf") if "conf" in value else value.get("confident"),
            "status": value.get("status", "NG"),
        })
    return detection_info

# --- Helper: lookup lot for this camera and current time from cache ---
def get_current_lot_for_camera(camera_id, now=None):
    for lot in database.lot_manager.lots_today:
        if lot.get("started") and not lot.get("ended") and camera_id in lot.get("cameraids", []):
            return lot
    return None

# --- Insert NG data to productdefectresult and insert product quantity to transactionreport ---
def process_defect_data(camera_id: str, merged_data: dict, now=None):
    """
    Insert NG/OK data to productdefectresult and update transactionreport.quantity.
    Assumes the data sender never sends duplicates.
    Relies on DB unique constraint for last-resort protection.
    """
    db = SessionLocal()
    seq = merged_data.get("seq")
    planid = merged_data.get("planid")
    if now is None:
        now = datetime.now(bangkok).replace(tzinfo=None)
    try:
        lot = get_current_lot_for_camera(camera_id, now)
        logging.info(f"[DEBUG] lots_today cache: {database.lot_manager.lots_today}")

        if not lot:
            logging.warning(f"[process_defect_data] No active lot for camera_id={camera_id} at {now}")
            return
        prodid = lot["prodid"]
        prodlot = lot["prodlot"]

        for key, info in merged_data.items():
            if key in IGNORE_KEYS:
                continue
            if not isinstance(info, dict):
                continue
            status = info.get("status") or "OK"
            functionid = info.get("functionid")
            defectid = info.get("defectId") or FUNCTIONID_TO_DEFECTID.get(functionid, f"DEF_{key.upper()}")
            imagepath = make_image_path(planid, prodlot, prodid, seq)
            try:
                db.execute(text("""
                    INSERT INTO productdefectresult (
                        prodstatus,
                        defectid,
                        cameraid,
                        prodid,
                        imagepath,
                        defecttime,
                        prodseq
                    ) VALUES (
                        :prodstatus,
                        :defectid,
                        :cameraid,
                        :prodid,
                        :imagepath,
                        :now,
                        :prodseq
                    )
                """), {
                    "prodstatus": status,
                    "defectid": defectid,
                    "cameraid": camera_id,
                    "prodid": prodid,
                    "imagepath": imagepath,
                    "now": now,
                    "prodseq": seq
                })
            except Exception as e:
                # Catch duplicate error, log, and skip
                logging.warning(f"[DB] Insert duplicate or error skipped for defect: {defectid}, error: {e}")

        db.commit()

        # --- Update transactionreport.quantity ---
        actual_product = merged_data.get("actualProduct", None)
        if actual_product is not None:
            update_sql = text("""
                UPDATE transactionreport
                SET quantity = :actualproduct
                WHERE prodlot = :prodlot AND prodid = :prodid
            """)
            db.execute(update_sql, {
                "actualproduct": actual_product,
                "prodlot": prodlot,
                "prodid": prodid,
            })
            db.commit()
            logging.debug(
                f"[DB] Updated transactionreport.qty to {actual_product} "
                f"for prodlot={prodlot} prodid={prodid}"
            )

        # --- Re-query total NG after insert ---
        sql = text("""
            SELECT COUNT(DISTINCT pdr.prodseq) AS ng_count
            FROM productdefectresult pdr
            JOIN planning p ON pdr.prodid = p.prodid
            WHERE p.prodlot = :prodlot
            AND pdr.cameraid = :camera_id
            AND pdr.prodid = :prodid
            AND pdr.prodstatus = 'NG'
        """)
        ng_row = db.execute(sql, {
            "prodlot": prodlot,
            "camera_id": camera_id,
            "prodid": prodid,
        }).mappings().first()
        total_ng = ng_row["ng_count"] if ng_row else 0
        startdatetime = lot["startdatetime"]
        enddatetime = lot["enddatetime"]
        logging.info(
            f"[DB] Total NG for {camera_id} in lot {prodlot} "
            f"between start: {startdatetime} and end: {enddatetime} = {total_ng}"
        )
        # --- Broadcast merged_data + totalNG ---
        merged_data["totalNG"] = total_ng
        asyncio.create_task(broadcast_update(camera_id, merged_data))

    except Exception as e:
        logging.error(f"[DB] Error in process_defect_data: {e}")
    finally:
        db.close()

from collections import deque, defaultdict

# cache: (camera_id, lot_no) -> deque of (seq, totalNG, actualProduct)
last_broadcast_state = defaultdict(lambda: deque(maxlen=3))

async def broadcast_update(camera_id: str, merged_data: dict):
    lot_no = merged_data.get("lotNo") or merged_data.get("prodlot")
    seq = merged_data.get("seq")
    total_ng = merged_data.get("totalNG")
    actual_product = merged_data.get("actualProduct")
    cache_key = (camera_id, lot_no)
    new_val = (seq, total_ng, actual_product)
    # Check for duplicate (avoid sending the same state twice in a row)
    state_deque = last_broadcast_state[cache_key]
    if state_deque and state_deque[-1] == new_val:
        return  # Do not broadcast duplicate state
    # Append new state (if deque is full, oldest will be removed automatically)
    state_deque.append(new_val)
    logging.info(f"[BROADCAST] {cache_key} {new_val} (cache size: {len(state_deque)})")
    for q in pending_updates.get(camera_id, []):
        await q.put(merged_data)

# --- Step: WebSocket handler, serve live info to clients from cache ---
async def live_defect_ws_handler(websocket: WebSocket, camera_id: str):
    await websocket.accept()

    if camera_id not in active_websockets:
        active_websockets[camera_id] = []
    active_websockets[camera_id].append(websocket)

    if camera_id not in pending_updates:
        pending_updates[camera_id] = []
    
    personal_queue = asyncio.Queue()
    pending_updates[camera_id].append(personal_queue)

    try:
        while True:
            try:
                merged_data = await asyncio.wait_for(personal_queue.get(), timeout=60.0)
            except asyncio.TimeoutError:
                try:
                    await websocket.send_json({"type": "ping"})
                except Exception:
                    break
                continue

            now = datetime.now(bangkok).replace(tzinfo=None)
            lot = get_current_lot_for_camera(camera_id, now)
            if not lot:
                await websocket.send_json({"error": "No active lot for this camera (cache)"})
                continue

            db = SessionLocal()
            try:
                total_ng = int(merged_data.get("totalNG", 0))
                logging.info(f"[WS] Used totalNG={total_ng} from merged_data for camera {camera_id}")
                detection_info = extract_detection_info(merged_data)
                result = {
                    "liveStream": merged_data.get("liveStream", ""),
                    "cameraId": camera_id,
                    "status": "NG" if any(det["status"] == "NG" for det in detection_info) else "OK",
                    "lotNo": lot["prodlot"],
                    "totalNG": total_ng,
                    "totalProduct": int(lot.get("quantity", 0)),
                    "actualProduct": int(merged_data.get("actualProduct", 0)),
                    "currentInspection": {
                        "productId": lot["prodid"],
                        "productName": lot.get("prodname", ""),
                        "serialNo": lot.get("prodserial", ""),
                        "productDateTime": lot["createddate"].strftime("%Y-%m-%d %H:%M:%S") if lot.get("createddate") else None,
                    },
                    "detectionInfo": detection_info
                }
                await websocket.send_json(result)
                logging.info(f"[WebSocket] Broadcasted to {camera_id}")
            finally:
                db.close()

    except WebSocketDisconnect:
        logging.info(f"[WebSocket] Disconnected: live-defect/{camera_id}")
    finally:
        # Cleanup personal queue
        if personal_queue in pending_updates.get(camera_id, []):
            pending_updates[camera_id].remove(personal_queue)
            if not pending_updates[camera_id]:
                pending_updates.pop(camera_id)
        if websocket in active_websockets.get(camera_id, []):
            active_websockets[camera_id].remove(websocket)
            if not active_websockets[camera_id]:
                active_websockets.pop(camera_id)
        try:
            await websocket.close()
        except RuntimeError:
            pass

