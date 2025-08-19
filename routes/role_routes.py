from fastapi import APIRouter, Depends, HTTPException, Form, UploadFile, File
from sqlalchemy.orm import Session

from database import SessionLocal, RoleDB
from database.schemas import RoleSearch, RoleCreate, RoleUpdate

router = APIRouter(tags=["Role"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/roles")
def roles(model: RoleSearch = Depends()):
    try:
        return {"roles": RoleDB().get_roles(model)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/add-role")
def add_role(role: RoleCreate, db: Session = Depends(get_db)):
    try:
        return RoleDB().add_role(role, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/update-role")
def update_role(roleid: str, role: RoleUpdate, db: Session = Depends(get_db)):
    try:
        return RoleDB().update_role(roleid, role, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/delete-role")
def delete_role_api(roleid: str, db: Session = Depends(get_db)):
    try:
        return RoleDB().delete_role(roleid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/upload-roles")
async def upload_roles(uploadby: str = Form(...), file: UploadFile = File(...), db: Session = Depends(get_db)):
    try:
        return RoleDB().upload_roles(uploadby, file, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/suggest-role-name")
def suggest_role_name(q: str):
    try:
        return RoleDB().suggest_role_name(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/role-permissions")
def get_role_permissions(roleid: int, db: Session = Depends(get_db)):
    try:
        return RoleDB().get_role_permissions(roleid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/update-role-permissions")
def update_role_permissions(roleid: int, permissions_data: dict, db: Session = Depends(get_db)):
    try:
        return RoleDB().update_role_permissions(roleid, permissions_data, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
        