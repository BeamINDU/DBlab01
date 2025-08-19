import base64
import numpy as np
from pathlib import Path
import cv2
from datetime import datetime
from threading import Lock
import json

COUNTER_FILE = Path("/tmp/daily_count_memory.json")
memory_lock = Lock()

def load_count():
    if COUNTER_FILE.exists():
        try:
            with open(COUNTER_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_count(memory):
    try:
        with open(COUNTER_FILE, "w") as f:
            json.dump(memory, f)
    except Exception:
        pass

def reset_count_detect_memory():
    with memory_lock:
        save_count({})

def run_detection(frame_b64: str, expected=None, model=None, prodlot=None, prodid=None):
    if model is None:
        return {"error": "No model provided"}
    jpg_bytes = base64.b64decode(frame_b64)
    np_arr = np.frombuffer(jpg_bytes, dtype=np.uint8)
    img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
    if img is None:
        return {"error": "Failed to decode image"}

    results = model.predict(img, stream=True, verbose=False, iou=0.7)
    output_dir = Path("/home/ubuntu/test_pics")
    output_dir.mkdir(parents=True, exist_ok=True)

    response = []
    frame_count = 0
    main_label = None
    labels = []

    for i, r in enumerate(results):
        class_names = []
        confs = []
        boxes = []
        labels_this_frame = []

        if hasattr(r, "obb") and r.obb is not None and r.obb.cls is not None and r.obb.xyxyxyxy is not None:
            obb_xyxyxyxy = r.obb.xyxyxyxy.cpu().numpy()
            cls_ids = r.obb.cls.cpu().numpy().astype(int)
            conf_arr = r.obb.conf.cpu().numpy() if hasattr(r.obb, "conf") and r.obb.conf is not None else None

            for idx, poly in enumerate(obb_xyxyxyxy):
                box = poly.tolist()
                boxes.append(box)
                class_id = cls_ids[idx]
                class_name = r.names[class_id] if hasattr(r, "names") else str(class_id)
                labels_this_frame.append(class_name)
                class_names.append(class_name)
                if conf_arr is not None:
                    confs.append(f"{float(conf_arr[idx]):.2f}")
        else:
            labels_this_frame.append("no_object")
            boxes.append([])
            class_names.append("no_object")
            confs.append(None)

        # 1 main class per frame (for unique key, pick first or 'no_class')
        main_label = class_names[0] if class_names else "no_class"
        frame_count = len(boxes) if boxes and boxes[0] != [] else 0
        response.append({
            "confs": confs,
            "box": boxes,
            "label": labels_this_frame
        })
        labels = labels_this_frame  # use last result (usually only one result)

    # --- Update persistent daily memory for actualProduct ---
    today_str = datetime.now().strftime("%Y-%m-%d")
    key = f"{prodlot}|{prodid}|{main_label}" if prodlot and prodid else main_label if main_label else "default"
    with memory_lock:
        daily_count_memory = load_count()
        if today_str not in daily_count_memory:
            daily_count_memory[today_str] = {}
        if key not in daily_count_memory[today_str]:
            daily_count_memory[today_str][key] = 0
        daily_count_memory[today_str][key] += frame_count
        actualProduct = daily_count_memory[today_str][key]
        save_count(daily_count_memory)

    return {
        "frame": frame_b64,
        "detections": response,
        "expected": expected,
        "predictedResult": labels,
        "actualProduct": actualProduct
    }

