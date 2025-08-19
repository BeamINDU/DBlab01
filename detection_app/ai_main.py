from fastapi import FastAPI, HTTPException, Request, Body
from pydantic import BaseModel
from model_loader import load_all_models, get_detection_model, get_function_model
from detection import run_detection, reset_count_detect_memory
from plot_detect import run_plot_detection
from functions.color_detect import color_check
from functions.barcode_detect import barcode_text_ocr
from functions.count_detect import object_counting
from typing import List, Optional, Union
from functions.component_detect import missing_component_check 
from functions.classification import classification_type 
from pipeline import router as pipeline_router

app = FastAPI()
app.include_router(pipeline_router)

@app.on_event("startup")
def startup_event():
    print("Loading all models required for today's planning ...")
    load_all_models()
    print("Model loading complete.")

class FrameInput(BaseModel):
    frame: str
    frame_id: str
    box: Optional[List] = None
    class_label: Optional[str] = None
    expected: Optional[Union[str, List[str]]] = None
    modelversionid: Optional[int] = None
    functionid: Optional[int] = None
    prodlot: Optional[str] = None
    prodid: Optional[str] = None

@app.post("/reload-models")
async def reload_models():
    try:
        load_all_models()
        return {"status": "success", "detail": "Models reloaded."}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.post("/detect")
def detect(input: FrameInput):
    model = get_detection_model(input.modelversionid)
    if model is None:
        raise HTTPException(status_code=400, detail=f"Detection model not loaded")
    result = run_detection(input.frame, expected=input.expected, model=model, prodlot=input.prodlot, prodid=input.prodid)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    if isinstance(result, dict):
        result["frame_id"] = input.frame_id
        result["expected"] = input.expected
    return result

@app.post("/plot")
async def plot(request: Request):
    input_dict = await request.json()
    frame_id = input_dict.get("frame_id", None)
    expected = input_dict.get("expected", None)
    result = run_plot_detection(input_dict, expected=expected)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    if isinstance(result, dict):
        if frame_id is not None:
            result["frame_id"] = frame_id
        result["expected"] = expected
    return result

@app.post("/detect_color")
def detect_color_endpoint(input: FrameInput = Body(...)):
    try:
        model = get_function_model(input.modelversionid, input.functionid)
        if model is None:
            raise HTTPException(status_code=400, detail=f"Function model not loaded")
        result = color_check(
            input.frame,
            box=getattr(input, 'box', None),
            expected=input.expected,
            model=model
        )
        if isinstance(result, dict):
            result["frame_id"] = input.frame_id
            result["expected"] = input.expected
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/detect_barcode")
def detect_barcode_endpoint(input: FrameInput):
    try:
        result = barcode_text_ocr(input.frame, expected=input.expected)
        if isinstance(result, dict):
            result["frame_id"] = input.frame_id
            result["expected"] = input.expected
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/count_detect")
def count_detect_endpoint(input: FrameInput):
    try:
        result = object_counting(
            input.frame,
            class_label=input.class_label,
            box=input.box,
            expected=input.expected
        )
        if isinstance(result, dict):
            result["frame_id"] = input.frame_id
            result["expected"] = input.expected
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/reset_count")
def reset_count_endpoint():
    reset_count_detect_memory()
    return {"status": "reset", "detail": "All daily counts have been cleared."}

@app.post("/detect_component")
def detect_component_endpoint(input: FrameInput):
    model = get_function_model(input.modelversionid, input.functionid)
    if model is None:
        raise HTTPException(status_code=400, detail=f"Function model not loaded")
    try:
        result = missing_component_check(input.frame, expected=input.expected, model=model)
        if isinstance(result, dict):
            result["frame_id"] = input.frame_id
            result["expected"] = input.expected
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/classification")
def classification_endpoint(input: FrameInput):
    """
    Classify a base64-encoded image frame and return predicted result, expected, confidence, and status.
    """
    try:
        result = classification_type(input.frame, expected=input.expected)
        if isinstance(result, dict):
            result["frame_id"] = input.frame_id
            result["expected"] = input.expected
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

