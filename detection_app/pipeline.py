from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, List, Optional
from datetime import datetime
import asyncio
import httpx

# --- Import your real detection functions here ---
from model_loader import get_function_model
from functions.color_detect import color_check
from functions.barcode_detect import barcode_text_ocr
from functions.count_detect import object_counting
from functions.component_detect import missing_component_check
from functions.classification import classification_type
from detection import run_detection
from plot_detect import run_plot_detection

router = APIRouter()

# ---- Input schema ----
class DetectionInput(BaseModel):
    frame: str  # base64
    frame_id: str
    modelversionid: int
    prodid: Optional[str]
    prodlot: Optional[str]
    camid: str
    planid: Optional[str]
    functions: List[int]
    function_names: List[str]
    expected: Optional[List[str]] = []

@router.post("/pipeline")
async def run_pipeline(input: DetectionInput):
    # Step 1: call /detect
    detect_result = run_detection(
        input.frame,
        expected=input.expected,
        model=get_function_model(input.modelversionid, input.functions[0]) if input.functions else None,
        prodlot=input.prodlot,
        prodid=input.prodid,
    )
    frame = detect_result.get("frame")
    actualProduct = detect_result.get("actualProduct")
    predictedResult = detect_result.get("predictedResult", [])

    if frame is None:
        raise HTTPException(status_code=500, detail="Missing 'frame' from /detect")

    # Extract bounding boxes and labels from /detect
    detect_boxes = []
    detect_labels = []
    if isinstance(detect_result.get("detections"), list):
        for det in detect_result["detections"]:
            detect_boxes += det.get("box", [])
            detect_labels += det.get("label", [])

    # Step 2: run actual functions
    task_map = {}
    for i, function_id in enumerate(input.functions):
        func_name = input.function_names[i].strip().lower().replace(" ", "_")
        func = globals().get(func_name)
        if not func:
            continue  # or raise

        expected = input.expected[i] if i < len(input.expected) else None
        model = get_function_model(input.modelversionid, function_id)

        # Call all functions with the same parameter list
        task_map[func_name] = asyncio.to_thread(func, frame, box=None, expected=expected, model=model)
    results = await asyncio.gather(*task_map.values())

    # Step 3: build payload
    merged_payload = {
        "frame_id": input.frame_id,
        "frame": frame,
        "liveStream": frame,
        "planid": input.planid,
        "actualProduct": actualProduct,
        "box": detect_boxes,
        "label": detect_labels
    }

    for i, topic in enumerate(task_map.keys()):
        res = results[i] or {}
        if not isinstance(res, dict):
            res = {"error": f"Invalid result from function {topic}"}
        res.update({
            "functionid": input.functions[i],
            "function": input.function_names[i],
            "expected": input.expected[i] if i < len(input.expected) else None
        })
        merged_payload[topic] = res

    # Step 4: send to /plot
    output_msg = {
        **merged_payload,
        "prodid": input.prodid,
        "prodlot": input.prodlot,
        "camid": input.camid,
        "label": merged_payload.get("label", ""),
        "seq": actualProduct
    }

    result = run_plot_detection(output_msg)
    result.pop("frame", None)
    return result


