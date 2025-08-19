from fastapi import APIRouter, Depends, HTTPException, Form, UploadFile, File
from sqlalchemy.orm import Session
from typing import Optional, List

from database import SessionLocal, DetectionModelDB, DetectionModelService
from database.schemas import LabelClassUpdate, DetectionModelSearch, DetectionModelCreate, DetectionModelDuplicate, DetectionModelUpdateStep1, DetectionModelUpdateStep2, DetectionModelUpdateStep3, DetectionModelUpdateStep4

router = APIRouter(tags=["Model"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/label-class")
def get_label_class(modelversionid : int):
    try:
        return DetectionModelDB().get_label_class(modelversionid)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/update-label-class")
def update_label_class(
    modelversionid: int,
    model_list: List[LabelClassUpdate],
    db: Session = Depends(get_db)
):
    try:
        return DetectionModelService().update_labelclass(modelversionid, model_list, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.delete("/delete-label-class")
def delete_label_class(classid: int, db: Session = Depends(get_db)):
    try:
        return DetectionModelService().delete_labelclass(classid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/suggest-modelname")
def suggest_modelname(q: str):
    try:
        return DetectionModelDB().suggest_modelname(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/suggest-function")
def suggest_function(q: str):
    try:
        return DetectionModelDB().suggest_function(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/functions")
def get_functions():
    try:
        return DetectionModelDB().get_functions()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    
@router.get("/versions")
def get_versions(modelid : int):
    try:
        return DetectionModelDB().get_versions(modelid)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/model-functions")
def get_model_functions(modelversionid : int):
    try:
        return DetectionModelDB().get_model_functions(modelversionid )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
  
@router.get("/model-images")
def get_model_images(modelversionid: int):
    try:
        return DetectionModelDB().get_model_images(modelversionid)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/model-camera")
def get_model_camera(modelversionid: int):
    try:
        return DetectionModelDB().get_model_camera(modelversionid)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/model-version")
def get_model_version(modelversionid : int):
    try:
        return DetectionModelDB().get_model_version(modelversionid )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
  
# @router.get("/model-detail")
# def model_detail(modelversionid: int, db: Session = Depends(get_db)):
#     try:
#         return DetectionModelService().model_detail(modelversionid, db)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))  
    
@router.get("/detection-model")
def detection_model(model: DetectionModelSearch = Depends(), db: Session = Depends(get_db)):
    try:
        return DetectionModelService().detection_model(model, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/add-model")
def add_model(model: DetectionModelCreate, db: Session = Depends(get_db)):
    try:
        return DetectionModelService().add_model(model, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/duplicate-model")
def duplicate_model(model: DetectionModelDuplicate, db: Session = Depends(get_db)):
    try:
        return DetectionModelService().duplicate_model(model, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.delete("/delete-model")
def delete_model(modelid: str, db: Session = Depends(get_db)):
    try:
        return DetectionModelService().delete_model(modelid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.delete("/delete-model-version")
def delete_modelversion(modelversionid: str, db: Session = Depends(get_db)):
    try:
        return DetectionModelService().delete_modelversion(modelversionid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/delete-image")
def delete_image(imageid: str, db: Session = Depends(get_db)):
    try:
        return DetectionModelService().delete_image(imageid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/update-model-step1")
def update_model_step1(modelversionid: int, model: DetectionModelUpdateStep1, db: Session = Depends(get_db)):
    try:
        return DetectionModelService().update_model_step1(modelversionid, model, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
  
@router.put("/update-model-step2")
def update_model_step2(modelversionid: int, model: DetectionModelUpdateStep2, db: Session = Depends(get_db)):
    try:
        return DetectionModelService().update_model_step2(modelversionid, model, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/update-model-step3")
def update_model_step3(modelversionid: int, model: DetectionModelUpdateStep3, db: Session = Depends(get_db)):
    try:
        return DetectionModelService().update_model_step3(modelversionid, model, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
     
@router.put("/update-model-step4")
def update_model_step4(modelversionid: int, model: DetectionModelUpdateStep4, db: Session = Depends(get_db)):
    try:
        return DetectionModelService().update_model_step4(modelversionid, model, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))   

# @router.post("/upload-base64-image")
# def upload_base64_image(model: schemas.DetectionModelImage, db: Session = Depends(get_db)):
#     try:
#         return DetectionModelService().upload_base64_image(model, db)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))   
    
@router.post("/upload-image-file")
def upload_image_file(
    modelversionid: int = Form(...),
    modelid: str = Form(...),
    updatedby: str = Form(...),
    annotate: str = Form(...),
    imageid: Optional[int] = Form(None),
    file: Optional[UploadFile] = File(None),
    size: Optional[int] = Form(None),
    width: Optional[int] = Form(None),
    height: Optional[int] = Form(None),
    db: Session = Depends(get_db)
) -> str:
    try:
        return DetectionModelService().upload_image_file(modelversionid, modelid, updatedby, annotate, imageid, file, size, width, height, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))   
    
        