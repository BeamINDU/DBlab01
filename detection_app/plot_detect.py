import base64
import cv2
import numpy as np

def decode_b64_image(b64_str):
    img_bytes = base64.b64decode(b64_str)
    img_array = np.frombuffer(img_bytes, dtype=np.uint8)
    img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
    return img

def encode_image_b64(img):
    _, buffer = cv2.imencode('.jpg', img)
    b64_str = base64.b64encode(buffer).decode('utf-8')
    return b64_str

def plot_box(img, box, label=""):
    color = (0, 255, 0)
    if not box:
        return  # Safeguard
    if isinstance(box, list):
        if len(box) == 4 and all(isinstance(v, (int, float)) for v in box):
            x1, y1, x2, y2 = map(int, box)
            cv2.rectangle(img, (x1, y1), (x2, y2), color, 2)
            if label:
                cv2.putText(img, label, (x1, y1-10), cv2.FONT_HERSHEY_SIMPLEX, 0.7, color, 2)
        elif all(isinstance(pt, list) and len(pt) == 2 for pt in box):
            pts = np.array(box, dtype=np.int32)
            if pts.ndim == 2:
                cv2.polylines(img, [pts], isClosed=True, color=color, thickness=2)
                x1, y1 = pts[:, 0].min(), pts[:, 1].min()
                if label:
                    cv2.putText(img, label, (x1, y1-10), cv2.FONT_HERSHEY_SIMPLEX, 0.7, color, 2)

def plot_boxes_on_image(img, input_dict):
    img_copy = img.copy()
    def recurse_and_plot(d):
        if isinstance(d, dict):
            if 'box' in d and 'label' in d:
                boxes, labels = d['box'], d['label']
                if (
                    isinstance(boxes, list) and isinstance(labels, list)
                    and len(boxes) > 0 and len(labels) > 0
                ):
                    for i, box in enumerate(boxes):
                        label = labels[i] if i < len(labels) else ""
                        plot_box(img_copy, box, label)
            for v in d.values():
                recurse_and_plot(v)
        elif isinstance(d, list):
            for item in d:
                if isinstance(item, dict) and 'box' in item and 'label' in item:
                    box = item['box']
                    label = item['label']
                    plot_box(img_copy, box, label)
                recurse_and_plot(item)
    recurse_and_plot(input_dict)
    return img_copy

# process data and plot
def run_plot_detection(input_dict, expected=None):
    # Get frame base64 string
    frame_b64 = input_dict.get("frame")
    if frame_b64 is None:
        return {"error": "Input dict missing 'frame' field."}

    # Decode and plot
    img = decode_b64_image(frame_b64)
    img_with_boxes = plot_boxes_on_image(img, input_dict)
    new_b64 = encode_image_b64(img_with_boxes)
    input_dict['liveStream'] = new_b64

    # Try to find actualProduct at top level or in any function result
    actual_product = input_dict.get("actualProduct")
    if actual_product is None:
        for v in input_dict.values():
            if isinstance(v, dict) and "actualProduct" in v:
                actual_product = v["actualProduct"]
                break
    if actual_product is not None:
        input_dict["actualProduct"] = actual_product

    # Clean up: ensure 'kafka' is removed if present
    input_dict.pop("kafka", None)

    # Ensure planid is present
    if "planid" not in input_dict:
        for v in input_dict.values():
            if isinstance(v, dict) and "planid" in v:
                input_dict["planid"] = v["planid"]
                break

    return input_dict



