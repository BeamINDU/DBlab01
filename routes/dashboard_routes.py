from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime

from database import SessionLocal, DashboardService

router = APIRouter(tags=["Dashboard"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/dashboard-totalproduct")
def endpoint_total_products(
    start: datetime,
    end: datetime,
    productname: Optional[str] = Query(None),
    prodline: Optional[str] = Query(None),
    cameraid: Optional[str] = Query(None),
    month: Optional[str] = Query(None),
    year: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        return DashboardService.get_total_products(start, end, productname, prodline, cameraid, month, year, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/dashboard-goodngratio")
def endpoint_ratio(
    start: datetime,
    end: datetime,
    productname: Optional[str] = Query(None),
    prodline: Optional[str] = Query(None),
    cameraid: Optional[str] = Query(None),
    month: Optional[str] = Query(None),
    year: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        return DashboardService.get_ratio(start, end, productname, prodline, cameraid, month, year, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/dashboard-top5trends")
def endpoint_top5trends(
    start: datetime,
    end: datetime,
    productname: Optional[str] = Query(None),
    prodline: Optional[str] = Query(None),
    cameraid: Optional[str] = Query(None),
    month: Optional[str] = Query(None),
    year: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        return DashboardService.top_5_trends(start, end, productname, prodline, cameraid, month, year, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/dashboard-top5defects")
def endpoint_top5defects(
    start: datetime,
    end: datetime,
    productname: Optional[str] = Query(None),
    prodline: Optional[str] = Query(None),
    cameraid: Optional[str] = Query(None),
    month: Optional[str] = Query(None),
    year: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        return DashboardService.top_5_defects(start, end, productname, prodline, cameraid, month, year, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/dashboard-ngdistribution")
def endpoint_distribution(
    start: datetime,
    end: datetime,
    productname: Optional[str] = Query(None),
    prodline: Optional[str] = Query(None),
    cameraid: Optional[str] = Query(None),
    month: Optional[str] = Query(None),
    year: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        return DashboardService.ng_distribution(start, end, productname, prodline, cameraid, month, year, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/dashboard-defectscamera")
def endpoint_defects_camera(
    start: datetime,
    end: datetime,
    productname: Optional[str] = Query(None),
    prodline: Optional[str] = Query(None),
    month: Optional[str] = Query(None),
    year: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        return DashboardService.get_defects_with_ng_gt_zero(start, end, productname, prodline, month, year, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/filter-lines")
def get_lines_dropdown_list(db: Session = Depends(get_db)):
    try:
        return DashboardService.get_lines_list(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/filter-products") 
def get_products_dropdown_list(db: Session = Depends(get_db)):
    try:
        return DashboardService.get_products_list(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/filter-cameras")
def get_cameras_dropdown_list(db: Session = Depends(get_db)):
    try:
        return DashboardService.get_cameras_list(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))    
        