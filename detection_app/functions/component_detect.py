import base64
import numpy as np
import cv2
from typing import Optional
from ultralytics import YOLO

def missing_component_check(frame: str, box=None, expected=None, model=None, class_label: Optional[str] = None) -> dict:
    if model is None:
        return {"error": "No model provided"}
    # Decode the base64 image
    if ',' in frame:
        _, encoded = frame.split(',', 1)
    else:
        encoded = frame
    jpg_bytes = base64.b64decode(encoded)
    np_arr = np.frombuffer(jpg_bytes, dtype=np.uint8)
    img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
    if img is None:
        return {"error": "Failed to decode image"}

    # Run model prediction
    results = model.predict(img, stream=True, verbose=False)

    detected_classes = set()
    confs = []

    for r in results:
        if hasattr(r, "boxes") and r.boxes is not None and r.boxes.cls is not None:
            cls_ids = r.boxes.cls.cpu().numpy().astype(int)
            conf_arr = r.boxes.conf.cpu().numpy() if hasattr(r.boxes, "conf") and r.boxes.conf is not None else None
            for idx, class_id in enumerate(cls_ids):
                class_name = r.names[class_id] if hasattr(r, "names") else str(class_id)
                detected_classes.add(class_name)
                if conf_arr is not None:
                    confs.append(float(conf_arr[idx]))
                else:
                    confs.append(0.0)

    # Get all possible class names from model
    all_classes = list(model.names.values()) if isinstance(model.names, dict) else list(model.names)
    # Find missing components
    missing_components = [name for name in all_classes if name not in detected_classes]
    confident = max(confs) if confs else 0.0

    result = {
            "predictedResult": ",".join(missing_components),
            "expected": ",".join(all_classes),
            "confident": confident,
            "status": "OK" if len(missing_components) == 0 else "NG"
    }
    return result

