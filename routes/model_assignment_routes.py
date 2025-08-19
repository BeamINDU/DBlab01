from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from database import SessionLocal, ModelAssignmentDB, ModelAssignmentService
from database.schemas import ModelAssignmentSearch, ModelAssignmentUpdate

router = APIRouter(tags=["Model Assignmentser"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/model-assignments")
def model_assignments(model: ModelAssignmentSearch = Depends()):
    try:
        return {"model_assignments": ModelAssignmentDB().get_model_assignments(model)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/update-model-assignment")
def update_model_assignment(id: int, model: ModelAssignmentUpdate, db: Session = Depends(get_db)):
    try:
        return ModelAssignmentService.update_model_assignment(id, model, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        