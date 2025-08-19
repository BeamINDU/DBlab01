from fastapi import APIRouter, Depends, HTTPException, Form, UploadFile, File
from sqlalchemy.orm import Session

from database import SessionLocal, ProductTypeService, ProductTypeDB
from database.schemas import ProdTypeSearch, ProdTypeCreate, ProdTypeUpdate

router = APIRouter(tags=["Product Type"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/product-types")
def product_types(model: ProdTypeSearch = Depends()):
    try:
        return {"product_types": ProductTypeDB().get_product_types(model)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/add-product-type")
def add_prodtype(prodtype: ProdTypeCreate, db: Session = Depends(get_db)):
    try:
        return ProductTypeService.add_prodtype(prodtype, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/update-product-type")
def update_prodtype(prodtypeid: str, prodtype: ProdTypeUpdate, db: Session = Depends(get_db)):
    try:
        return ProductTypeService.update_prodtype(prodtypeid, prodtype, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/delete-product-type")
def delete_prodtype_api(prodtypeid: str, db: Session = Depends(get_db)):
    try:
        return ProductTypeService.delete_producttype(prodtypeid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/upload-product-types")
async def upload_product_types(uploadby: str = Form(...), file: UploadFile = File(...), db: Session = Depends(get_db)):
    try:
        return ProductTypeService.upload_product_types(uploadby, file, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/suggest-producttype-id")
def suggest_producttype_id(q: str):
    try:
        return ProductTypeDB().suggest_producttype_id(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/suggest-producttype-name")
def suggest_producttype_name(q: str):
    try:
        return ProductTypeDB().suggest_producttype_name(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

        