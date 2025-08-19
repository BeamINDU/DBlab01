import base64
import numpy as np
import cv2
from typing import Optional
from ultralytics import YOLO


def color_check(frame: str, box=None, expected=None, model=None, class_label: Optional[str] = None) -> dict:
    if model is None:
        return {"error": "No model provided"}
    # Decode base64 image
    if ',' in frame:
        _, encoded = frame.split(',', 1)
    else:
        encoded = frame
    image_bytes = base64.b64decode(encoded)
    np_arr = np.frombuffer(image_bytes, np.uint8)
    image = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
    if image is None:
        return {"error": "Failed to decode image"}

    # Run model prediction
    results = model.predict(image, stream=True, verbose=False, iou=0.7)

    for r in results:
        # Skip if no OBB
        if not hasattr(r, "obb") or r.obb is None or r.obb.cls is None:
            continue

        cls_ids = r.obb.cls.cpu().numpy().astype(int)
        confs = r.obb.conf.cpu().numpy() if hasattr(r.obb, "conf") and r.obb.conf is not None else [None] * len(cls_ids)

        if len(cls_ids) == 0:
            continue

        class_id = cls_ids[0]
        conf = f"{float(confs[0]):.2f}" if confs[0] is not None else None
        class_name = r.names[class_id] if hasattr(r, "names") else str(class_id)

        # Handle expected
        if expected is None:
            expected_list = []
        elif isinstance(expected, str):
            expected_list = [expected]
        elif isinstance(expected, list):
            expected_list = expected
        else:
            expected_list = []

        status = "OK" if class_name in expected_list else "NG"

        return {
            "predictedResult": class_name,
            "conf": conf,
            "status": status,
            "expected": expected
        }

    # If no valid detections found
    return {
        "predictedResult": "no_object",
        "conf": None,
        "status": "NG",
        "expected": expected
    }

