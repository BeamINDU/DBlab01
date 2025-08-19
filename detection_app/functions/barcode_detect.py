import base64
import io
import numpy as np
from PIL import Image
from pyzbar.pyzbar import decode
import cv2
from typing import Optional
from fastapi.responses import JSONResponse
import json

def barcode_text_ocr(frame: str, box=None, expected=None, model=None, class_label: Optional[str] = None) -> dict:
    if "," in frame:
        frame = frame.split(",")[1]
    image_bytes = base64.b64decode(frame)
   
    image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    image_np = np.array(image) 
    detected = decode(image_np)  
    image_cv = cv2.cvtColor(image_np, cv2.COLOR_RGB2BGR)
    
    # barcode_list = []
    box_list = []
    label_list = []
    predicted_result_list = []

    # Only process objects that are NOT QR codes
    filtered = [obj for obj in detected if obj.type != "QRCODE"]

    # --- handle expected as string or list ---
    if expected is None:
        expected_list = []
    elif isinstance(expected, str):
        expected_list = [expected]
    elif isinstance(expected, list):
        expected_list = expected
    else:
        expected_list = []

    for obj in filtered:
        rect = obj.rect
        # [x1, y1, x2, y2]
        x1, y1 = rect.left, rect.top
        x2, y2 = rect.left + rect.width, rect.top + rect.height

        cv2.rectangle(image_cv, (x1, y1), (x2, y2), (0, 255, 0), 3)

        box_list.append([x1, y1, x2, y2])
        label_list.append(obj.type)
        barcode_value = obj.data.decode('utf-8')
        predicted_result_list.append(barcode_value)

    # Status is OK if any barcode matches expected; otherwise NG
    if any(barcode in expected_list for barcode in predicted_result_list):
        status = "OK"
    elif len(filtered) == 0:
        status = "NG"
    else:
        status = "NG"
        
    result = {
        "status": status,
        "box": box_list,     # List of [x1, y1, x2, y2]
        "label": label_list, # List of str
        "predictedResult": predicted_result_list,
        "expected": expected
    }
    return result

