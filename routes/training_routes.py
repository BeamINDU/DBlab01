from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from database import SessionLocal, cancel_training_by_modelversion

router = APIRouter(tags=["Training"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.delete("/training-cancel/{modelversionid}")
async def training_cancel(modelversionid: int, db: Session = Depends(get_db)):
    try:
        result = await cancel_training_by_modelversion(modelversionid, db)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    
# @router.get("/files/{full_path:path}")
# async def serve_image(full_path: str):
#     file_path = (Path(UPLOAD_FOLDER) / full_path).resolve()
#     base_path = Path(UPLOAD_FOLDER).resolve()

#     if base_path in file_path.parents and file_path.is_file():
#         return FileResponse(file_path)
#     else:
#         raise HTTPException(status_code=404, detail="File not found")
    
# @router.get("/{full_path:path}")
# async def serve_image(full_path: str):
#     file_path = (Path(UPLOAD_FOLDER) / full_path).resolve()
#     base_path = Path(UPLOAD_FOLDER).resolve()

#     if base_path in file_path.parents and file_path.is_file():
#         return FileResponse(file_path)
#     else:
#         raise HTTPException(status_code=404, detail="File not found")
        