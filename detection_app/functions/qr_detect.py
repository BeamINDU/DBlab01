import base64
import io
import numpy as np
from PIL import Image
from pyzbar.pyzbar import decode
import cv2

def detect_barcode(image_data: str) -> dict:
    if "," in image_data:
        image_data = image_data.split(",")[1]
    image_bytes = base64.b64decode(image_data)
   
    image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    image_np = np.array(image) 
    detected = decode(image_np)  
    image_cv = cv2.cvtColor(image_np, cv2.COLOR_RGB2BGR)
    
    box_list = []
    label_list = []
    predicted_result_list = []
    status_list = []

    for obj in detected:
        if obj.type != "QRCODE":
            continue  # Skip non-QR codes

        rect = obj.rect
        x1, y1 = rect.left, rect.top
        x2, y2 = rect.left + rect.width, rect.top + rect.height

        cv2.rectangle(image_cv, (x1, y1), (x2, y2), (0, 255, 0), 3)

        box_list.append([x1, y1, x2, y2])
        label_list.append(obj.type)
        predicted_result_list.append(obj.data.decode('utf-8'))
        status_list.append("OK")

    # Calculate status
    status = "OK" if len(box_list) > 0 else "Error"

    result = {
        "status": status,
        "box": box_list,
        "label": label_list,
        "predictedResult": predicted_result_list,
        "expected": "www.csigroups.com"
    }
    return result
