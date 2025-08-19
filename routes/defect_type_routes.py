from fastapi import APIRouter, Depends, HTTPException, Form, File, UploadFile
from sqlalchemy.orm import Session

from database import SessionLocal, DefectTypeDB
from database.schemas import DefectTypeSearch, DefectTypeCreate, DefectTypeUpdate

router = APIRouter(tags=["Defect Type"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/defect-types", tags=["DefectType"])
def get_defect_types(model: DefectTypeSearch = Depends()):
    try:
        return {"defect_types": DefectTypeDB().get_defect_types(model)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/add-defect-type", tags=["DefectType"])
def add_defect_type(defect: DefectTypeCreate, db: Session = Depends(get_db)):
    try:
        return DefectTypeDB().add_defect_type(defect, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/update-defect-type", tags=["DefectType"])
def update_defect_type(defectid: str, defect: DefectTypeUpdate, db: Session = Depends(get_db)):
    try:
        return DefectTypeDB().update_defect_type(defectid, defect, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/delete-defect-type", tags=["DefectType"])
def delete_defecttype_api(defectid: str, db: Session = Depends(get_db)):
    try:
        return DefectTypeDB().delete_defect_type(defectid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/upload-defect-types", tags=["DefectType"])
async def upload_defect_types(uploadby: str = Form(...), file: UploadFile = File(...), db: Session = Depends(get_db)):
    try:
        return DefectTypeDB().upload_defect_types(uploadby, file, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))   

@router.get("/suggest-defecttype-id", tags=["DefectType"])
def suggest_defecttype_id(q: str):
    try:
        return DefectTypeDB().suggest_defecttype_id(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/suggest-defecttype-name", tags=["DefectType"])
def suggest_defecttype_name(q: str):
    try:
        return DefectTypeDB().suggest_defecttype_name(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
        