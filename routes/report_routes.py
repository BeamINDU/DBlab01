from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from database import SessionLocal, ReportDB
from database.schemas import ReportDefectSearch, ReportProductSearch, ReportProductSearchDetail, ProductDetailUpdate

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# -------------------- Report Defect Summary Service --------------------
@router.get("/report-defect-summary", tags=["ReportDefect"])
def defect_summary(model: ReportDefectSearch = Depends()):
    try:
        return ReportDB().get_defect_summary(model)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/export-report-defect-summary", tags=["ReportDefect"])
def export_defect_summary(model: ReportDefectSearch = Depends()):
    try:
        return ReportDB().export_defect_summary(model)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/suggest-defect-lotno", tags=["ReportDefect"])
def suggest_defect_lotno(q: str):
    try:
        return ReportDB().suggest_defect_lotno(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------- Report Product Defect Result Service --------------------
@router.get("/report-product-defect", tags=["ReportProduct"])
def report_product_defect(model: ReportProductSearch = Depends()):
    try:
        return ReportDB().get_report_product_defect(model)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/export-report-product-defect", tags=["ReportProduct"])
def export_report_product_defect(model: ReportProductSearch = Depends()):
    try:
        return ReportDB().export_report_product_defect(model)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/product-defect-detail", tags=["ReportProduct"])
def report_product_defect_detail(model: ReportProductSearchDetail, db: Session = Depends(get_db)):
    try:
        return ReportDB().report_product_defect_detail(model, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
  
@router.put("/update-product-defect-detail", tags=["ReportProduct"])
def update_product_defect_detail(model: ProductDetailUpdate, db: Session = Depends(get_db)):
    try:
        return ReportDB().update_product_defect_detail(model, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        