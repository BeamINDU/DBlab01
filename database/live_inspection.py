import logging
from fastapi import WebSocket, WebSocketDisconnect
from database.connect_to_db import Session, text
from typing import Dict, List
import asyncio

# Global maps for sockets and incoming updates
active_websockets: Dict[str, List[WebSocket]] = {}
pending_updates: Dict[str, asyncio.Queue] = {}

async def live_defect_ws_handler(websocket: WebSocket, camera_id: str, db: Session):
    await websocket.accept()

    if camera_id not in active_websockets:
        active_websockets[camera_id] = []
    if camera_id not in pending_updates:
        pending_updates[camera_id] = asyncio.Queue()

    active_websockets[camera_id].append(websocket)

    try:
        while True:
            # Wait for new merged data from Node-RED
            merged_data = await pending_updates[camera_id].get()

            # Run SQL to get camera/product metadata
            sql = text("""
                SELECT
                    c.cameralocation AS "location",
                    c.cameraid AS "cameraId",
                    c.cameraname AS "cameraName",
                    c.camerastatus AS "status",
                    p.prodid AS "productId",
                    p.prodname AS "productName",
                    pt.prodtypeid AS "productTypeId",
                    pt.prodtype AS "productTypeName",
                    p.prodserial AS "serialNo",
                    d.resultid,
                    d.imagepath as "imagepath", 
                    d.defecttype AS "defectType",
                    p.createddate AS "productDateTime",
                    ds.prodlot AS "lotNo",
                    ds.totalng AS "totalNG",
                    NULL AS "totalPlanning",
                    NULL AS "totalPlanning",  
                    NULL AS "actualPlanning"
                FROM public.camera c
                INNER JOIN public.productdefectresult d ON c.cameraid = d.cameraid
                INNER JOIN public.product p ON d.prodid = p.prodid
                INNER JOIN public.prodtype pt ON p.prodtypeid = pt.prodtypeid
                LEFT JOIN public.defectsummary ds ON p.prodid = ds.prodid
                WHERE c.isdeleted = false
                AND p.isdeleted = false
                AND c.cameraid = :camera_id
                ORDER BY c.cameraid, p.prodid;
            """)
            row = db.execute(sql, {"camera_id": camera_id}).mappings().first()
            if not row:
                await websocket.send_json({"error": "No defect + planning found"})
                continue

            result = {
                "liveStream": merged_data.get("liveStream", ""),
                "location": row["location"],
                "cameraId": row["cameraId"],
                "cameraName": row["cameraName"],
                "status": "OK" if row["status"] else "NG",
                "lotNo": row["lotNo"],
                "totalNG": str(row["totalNG"] if row["totalNG"] is not None else 10),
                "totalProduct": str(row["totalPlanning"] if row["totalPlanning"] is not None else 1000),
                "actualProduct": str(row["actualPlanning"] if row["actualPlanning"] is not None else 1000),
                "currentInspection": {
                    "productId": row["productId"],
                    "productName": row["productName"],
                    "serialNo": row["serialNo"],
                    "productDateTime": row["productDateTime"].strftime("%Y-%m-%d %H:%M:%S") if row["productDateTime"] else None,
                },
                "colorDetection": merged_data.get("colorDetection", {}),
                "typeClassification": merged_data.get("typeClassification", {}),
                "componentDetection": merged_data.get("componentDetection", {}),
                "objectCounting": merged_data.get("objectCounting", {}),
                "barcodeReading": merged_data.get("barcodeReading", {}),
            }

            await websocket.send_json(result)
            print(f"[WebSocket] Broadcasted to {camera_id}")

    except WebSocketDisconnect:
        logging.info(f"[WebSocket] Disconnected: live-defect/{camera_id}")
        active_websockets[camera_id].remove(websocket)
        if not active_websockets[camera_id]:
            del active_websockets[camera_id]
            del pending_updates[camera_id]

