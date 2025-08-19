from fastapi import APIRouter, Depends, HTTPException, Form, UploadFile, File
from sqlalchemy.orm import Session

from database import SessionLocal, PlanningDB
from database.schemas import PlanningSearch, PlanningCreate, PlanningUpdate, PlanningStart, PlanningStop

router = APIRouter(tags=["Planning"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("/planning")
def planning(model: PlanningSearch = Depends()):
    try:
        return PlanningDB().get_planning(model)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/export-planning")
def export_planning(model: PlanningSearch = Depends()):
    try:
        return PlanningDB().export_planning(model)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/add-planning")
def add_planning(plan: PlanningCreate, db: Session = Depends(get_db)):
    try:
        return PlanningDB().add_planning(plan, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/update-planning")
def update_planning(planid: str, plan: PlanningUpdate, db: Session = Depends(get_db)):
    try:
        return PlanningDB().update_planning(planid, plan, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/delete-planning")
def delete_planning(planid: str, updatedby: str, db: Session = Depends(get_db)):
    try:
        return PlanningDB().delete_planning(planid, updatedby, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.put("/start-planning")
def start_planning(plan: PlanningStart, db: Session = Depends(get_db)):
    try:
        return PlanningDB().start_planning(plan, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.put("/stop-planning")
def stop_planning(plan: PlanningStop, db: Session = Depends(get_db)):
    try:
        return PlanningDB().stop_planning(plan, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/upload-plannings")
async def upload_planning(uploadby: str = Form(...), file: UploadFile = File(...), db: Session = Depends(get_db)):
    try:
        return PlanningDB().upload_planning(uploadby, file, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/suggest-planid")
def suggest_planid(q: str):
    try:
        return PlanningDB().suggest_planid(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/suggest-plan-lotno")
def suggest_plan_lotno(q: str):
    try:
        return PlanningDB().suggest_plan_lotno(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/suggest-plan-lineid")
def suggest_plan_partno(q: str):
    try:
        return PlanningDB().suggest_plan_lineid(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
            