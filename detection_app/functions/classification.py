import base64
import numpy as np
import torch
import torchvision.transforms as transforms
import cv2
import torchvision.models as models
from typing import Optional

# Path to your fine-tuned model
MODEL_PATH = "models/fashion_classification_model.pt"

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

def get_model_and_classes():
    # Load checkpoint (dict with model_state_dict and classes)
    checkpoint = torch.load(MODEL_PATH, map_location=device)
    state = checkpoint["model_state_dict"]  # get weights
    class_names = checkpoint["classes"]     # get class name list
    num_classes = len(class_names)
    model = models.resnet18(num_classes=num_classes)
    model.load_state_dict(state)
    model = model.to(device)
    model.eval()
    return model, class_names

# Load model and class names ONCE when this module is imported
model, CLASSES = get_model_and_classes()

# Set expected class (first class as default, or set as needed)
EXPECTED = CLASSES[0] if CLASSES else "bag"  # fallback

# Define preprocessing
transform = transforms.Compose([
    transforms.ToPILImage(),
    transforms.Resize((224, 224)),
    transforms.ToTensor(),
    transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
])

def classification_type(frame: str, box=None, expected=None, model=None, class_label: Optional[str] = None) -> dict:
    """
    Classifies a base64-encoded image frame.
    Returns:
        dict: {
            "predictedResult": str,
            "expected": str or list,
            "confident": float,
            "status": str
        }
    """
    # Decode the base64 image
    if ',' in frame:
        _, encoded = frame.split(',', 1)
    else:
        encoded = frame
    try:
        jpg_bytes = base64.b64decode(encoded)
        np_arr = np.frombuffer(jpg_bytes, dtype=np.uint8)
        img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
        if img is None:
            return {"error": "Failed to decode image"}
    except Exception as e:
        return {"error": f"Failed to decode base64: {e}"}

    # Preprocess image for classifier
    try:
        img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        input_tensor = transform(img_rgb)
        input_tensor = input_tensor.unsqueeze(0).to(device)
    except Exception as e:
        return {"error": f"Image preprocessing failed: {e}"}

    # Run model prediction
    try:
        with torch.no_grad():
            outputs = model(input_tensor)
            prob = torch.nn.functional.softmax(outputs, dim=1)
            conf, pred = torch.max(prob, 1)
            predicted_label = CLASSES[pred.item()]
            confident = float(conf.item())
    except Exception as e:
        return {"error": f"Model inference failed: {e}"}

    # --- handle expected as string or list ---
    if expected is None:
        expected_list = [EXPECTED]
    elif isinstance(expected, str):
        expected_list = [expected]
    elif isinstance(expected, list):
        expected_list = expected
    else:
        expected_list = [expected]

    status = "OK" if predicted_label in expected_list else "NG"

    return {
        "predictedResult": predicted_label,
        "expected": expected,
        "confident": confident,
        "status": status
    }

