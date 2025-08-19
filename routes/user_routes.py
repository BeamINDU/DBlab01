from fastapi import APIRouter, Depends, HTTPException, Form, UploadFile, File
from sqlalchemy.orm import Session

from database import SessionLocal, UserDB, UserService
from database.schemas import UserSearch, UserCreate, UserUpdate

router = APIRouter(tags=["User"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/users")
def users(model: UserSearch = Depends()):
    try:
        return {"users": UserDB().get_users(model)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/add-user")
def add_user(user: UserCreate, db: Session = Depends(get_db)):
    try:
        return UserService.add_user(user, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/update-user")
def edit_user(userid: str, user: UserUpdate, db: Session = Depends(get_db)):
    try:
        return UserService.edit_user(userid, user, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/delete-user")
def delete_user_api(userid: str, db: Session = Depends(get_db)):
    try:
        return UserService.delete_user(userid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/upload-users")
async def upload_user(uploadby: str = Form(...), file: UploadFile = File(...), db: Session = Depends(get_db)):
    try:
        return UserService.upload_users(uploadby, file, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/suggest-userid")
def suggest_userid(q: str):
    try:
        return UserDB().suggest_userid(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/suggest-username")
def suggest_username(q: str):
    try:
        return UserDB().suggest_username(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/suggest-fullname")
def suggest_fullname(q: str):
    try:
        return UserDB().suggest_fullname(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        