from fastapi import APIRouter, Depends, HTTPException, Body, Form, File, UploadFile
from sqlalchemy.orm import Session

from database import SessionLocal, ProductService, ProductDB
from database.schemas import ProductSearch, ProductCreate, ProductUpdate

router = APIRouter(tags=["Product"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/products")
def products(model: ProductSearch = Depends()):
    try:
        return {"products": ProductDB().get_products(model)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/add-product")
def add_product(product: ProductCreate, db: Session = Depends(get_db)):
    try:
        return ProductService.add_product(product, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/update-product")
def update_product(prodid: str, product: ProductUpdate = Body(...), db: Session = Depends(get_db)):
    try:
        return ProductService.update_product(prodid, product, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/delete-product")
def delete_product_api(prodid: str, db: Session = Depends(get_db)):
    try:
        return ProductService.delete_product(prodid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/upload-products")
async def upload_products(uploadby: str = Form(...), file: UploadFile = File(...), db: Session = Depends(get_db)):
    try:
        return ProductService.upload_products(uploadby, file, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/product-options")
def product_options(q: str):
    try:
        return ProductDB().product_options(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/suggest-product-id")
def suggest_product_id(q: str):
    try:
        return ProductDB().suggest_product_id(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/suggest-product-name")
def suggest_product_name(q: str):
    try:
        return ProductDB().suggest_product_name(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/suggest-serial-no")
def suggest_serial_no(q: str):
    try:
        return ProductDB().suggest_serial_no(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/suggest-erp")
def suggest_erp(q: str):
    try:
        return ProductDB().suggest_erp(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))    
    
@router.get("/suggest-partno")
def suggest_partno(q: str):
    try:
        return ProductDB().suggest_partno(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        