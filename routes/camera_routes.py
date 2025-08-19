from fastapi import APIRouter, Depends, HTTPException, Form, File, UploadFile
from sqlalchemy.orm import Session

from database import SessionLocal, CameraService, CameraDB
from database.schemas import CameraSearch, CameraCreate, CameraUpdate

router = APIRouter(tags=["Camera"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/cameras")
def cameras(model: CameraSearch = Depends()):
    try:
        return {"cameras": CameraDB().get_cameras(model)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/add-camera")
def add_camera(camera: CameraCreate, db: Session = Depends(get_db)):
    try:
        return CameraService.add_camera(camera, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/update-camera")
def update_camera(cameraid: str, camera: CameraUpdate, db: Session = Depends(get_db)):
    try:
        return CameraService.update_camera(cameraid, camera, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/delete-camera")
def delete_camera_api(cameraid: str, db: Session = Depends(get_db)):
    try:
        return CameraService.delete_camera(cameraid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/upload-cameras")
async def upload_cameras(uploadby: str = Form(...), file: UploadFile = File(...), db: Session = Depends(get_db)):
    try:
        return CameraService.upload_cameras(uploadby, file, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))   
    
@router.get("/camera-options")
def camera_options(q: str):
    try:
        return CameraDB().camera_options(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/suggest-camera-id")
def suggest_camera_id(q: str):
    try:
        return CameraDB().suggest_camera_id(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/suggest-camera-name")
def suggest_camera_name(q: str):
    try:
        return CameraDB().suggest_camera_name(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/suggest-camera-location")
def suggest_camera_location(q: str):
    try:
        return CameraDB().suggest_camera_location(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/suggest-camera-ip")
def suggest_camera_ip(q: str):
    try:
        return CameraDB().suggest_camera_ip(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
