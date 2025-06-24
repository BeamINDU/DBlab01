from fastapi import FastAPI, HTTPException, Depends, Body, Query, UploadFile, File, Form, WebSocket, WebSocketDisconnect
# import asyncio
from typing import Optional, List
from datetime import datetime
from sqlalchemy.sql import text
from sqlalchemy.orm import Session
from database.connect_to_db import Session
from database.user import UserDB, UserService
from database.product import ProductDB, ProductService
from database.connect_to_db import test_db_connection, SessionLocal
import database.schemas as schemas
from database.defect import DefectDB
from database.camera import CameraDB, CameraService
from database.planning import PlanningDB
from database.model import DetectionModelDB, DetectionModelService
from database.transaction import TransactionDB
from database.report import ReportDB
from database.role import RoleDB
from database.permission import PermissionDB
from database.menu import MenuDB
from database.dashboard import DashboardService
# from database.live_inspection import live_inspection_ws_handler
# from streaming.live_stream import setup_streaming, websocket_clients
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware 

app = FastAPI(
    title="PI Backend API",
    description="Backend service for managing users, roles, products, planning, inspection, and reporting.",
    version="1.0.0",
    openapi_tags=[
        {"name": "General", "description": "Health check and DB test"},
        {"name": "Permission", "description": "Permission management"},
        {"name": "User", "description": "User management"},
        {"name": "Role", "description": "Role management"},
        {"name": "Product", "description": "Product management"},
        {"name": "ProdType", "description": "Product type management"},
        {"name": "Menu", "description": "Menu management"},
        {"name": "Camera", "description": "Camera configuration"},
        {"name": "Defect", "description": "Defect types"},
        {"name": "Planning", "description": "Production planning"},
        {"name": "Model", "description": "Detection model registry"},
        {"name": "Transaction", "description": "Lot and quantity tracking"},
        {"name": "Report", "description": "Report logs and results"},
        {"name": "Dashboard","description": "Dashboard"}
        # {"name": "Live", "description": "Live Inspection data"},
    ]
)
'''
@app.on_event("startup")
def on_startup():
    # Register the current event loop for your kafka thread to use
    setup_streaming(asyncio.get_event_loop())
'''
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # or frontend IP 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

user_db = UserDB()
product_db = ProductDB()
camera_db = CameraDB()
defect_db = DefectDB()
role_db = RoleDB()
permission_db = PermissionDB()
menu_db = MenuDB()
transaction_db = TransactionDB()
planning_db = PlanningDB()

# -------------------- General --------------------
@app.get("/", tags=["General"])
def read_root():
    return {"message": "API is working"}

@app.get("/test-db", tags=["General"])
def test_db():
    try:
        return test_db_connection()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# -------------------- User Service --------------------
@app.get("/users", tags=["User"])
def users():
    try:
        return {"users": user_db.get_users()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/add-user", tags=["User"])
def add_user(user: schemas.UserCreate, db: Session = Depends(get_db)):
    try:
        return UserService.add_user(user, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/update-user", tags=["User"])
def edit_user(userid: str, user: schemas.UserUpdate, db: Session = Depends(get_db)):
    try:
        return UserService.edit_user(userid, user, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/delete-user", tags=["User"])
def delete_user_api(userid: str, db: Session = Depends(get_db)):
    try:
        return UserService.delete_user(userid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------- Role Service --------------------
@app.get("/roles", tags=["Role"])
def roles():
    try:
        return {"roles": role_db.get_roles()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/add-role", tags=["Role"])
def add_role(role: schemas.RoleCreate, db: Session = Depends(get_db)):
    try:
        return role_db.add_role(role, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/update-role", tags=["Role"])
def update_role(roleid: str, role: schemas.RoleUpdate, db: Session = Depends(get_db)):
    try:
        return role_db.update_role(roleid, role, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/delete-role", tags=["Role"])
def delete_role_api(roleid: str, db: Session = Depends(get_db)):
    try:
        return role_db.delete_role(roleid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------- Product Service --------------------
@app.get("/products", tags=["Product"])
def products():
    try:
        return {"products": product_db.get_products()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/add-product", tags=["Product"])
def add_product(product: schemas.ProductCreate, db: Session = Depends(get_db)):
    try:
        return ProductService.add_product(product, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/update-product", tags=["Product"])
def update_product(prodid: str, product: schemas.ProductUpdate = Body(...), db: Session = Depends(get_db)):
    try:
        return ProductService.update_product(prodid, product, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/delete-product", tags=["Product"])
def delete_product_api(prodid: str, db: Session = Depends(get_db)):
    try:
        return ProductService.delete_product(prodid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------- Product Type Service --------------------
@app.get("/product-types", tags=["ProdType"])
def product_types():
    try:
        return {"product_types": product_db.get_product_types()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/add-product-type", tags=["ProdType"])
def add_prodtype(prodtype: schemas.ProdTypeCreate, db: Session = Depends(get_db)):
    try:
        return ProductService.add_prodtype(prodtype, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/update-product-type", tags=["ProdType"])
def update_prodtype(prodtypeid: str, prodtype: schemas.ProdTypeUpdate, db: Session = Depends(get_db)):
    try:
        return ProductService.update_prodtype(prodtypeid, prodtype, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/delete-product-type", tags=["ProdType"])
def delete_prodtype_api(prodtypeid: str, db: Session = Depends(get_db)):
    try:
        return ProductService.delete_producttype(prodtypeid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------- Camera Service --------------------
@app.get("/cameras", tags=["Camera"])
def cameras():
    try:
        return {"cameras": camera_db.get_cameras()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/add-camera", tags=["Camera"])
def add_camera(camera: schemas.CameraCreate, db: Session = Depends(get_db)):
    try:
        return CameraService.add_camera(camera, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/update-camera", tags=["Camera"])
def update_camera(cameraid: str, camera: schemas.CameraUpdate, db: Session = Depends(get_db)):
    try:
        return CameraService.update_camera(cameraid, camera, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/delete-camera", tags=["Camera"])
def delete_camera_api(cameraid: str, db: Session = Depends(get_db)):
    try:
        return CameraService.delete_camera(cameraid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------- Defect Types Service --------------------
@app.get("/defect-types", tags=["Defect"])
def get_defect_types():
    try:
        return {"defect_types": defect_db.get_defect_types()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/add-defect-type", tags=["Defect"])
def add_defect_type(defect: schemas.DefectTypeCreate, db: Session = Depends(get_db)):
    try:
        return defect_db.add_defect_type(defect, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/update-defect-type", tags=["Defect"])
def update_defect_type(defectid: str, defect: schemas.DefectTypeUpdate, db: Session = Depends(get_db)):
    try:
        return defect_db.update_defect_type(defectid, defect, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/delete-defect-type", tags=["Defect"])
def delete_defecttype_api(defectid: str, db: Session = Depends(get_db)):
    try:
        return DefectDB().delete_defecttype(defectid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------- Planning Service --------------------
@app.get("/planning", tags=["Planning"])
def planning():
    try:
        return {"planning": planning_db.get_planning()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/add-planning", tags=["Planning"])
def add_planning(plan: schemas.PlanningCreate, db: Session = Depends(get_db)):
    try:
        return planning_db.add_planning(plan, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/update-planning", tags=["Planning"])
def update_planning(planid: str, plan: schemas.PlanningUpdate, db: Session = Depends(get_db)):
    try:
        return planning_db.update_planning(planid, plan, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/delete-planning", tags=["Planning"])
def delete_planning_api(planid: str, db: Session = Depends(get_db)):
    try:
        return PlanningDB().delete_planning(planid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------- Detection Model Service --------------------
@app.get("/function", tags=["Model"])
def get_function():
    try:
        return DetectionModelDB().get_function()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/label-class", tags=["Model"])
def get_label_class():
    try:
        return DetectionModelDB().get_label_class()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/model-function", tags=["Model"])
def get_model_function(modelversionid : int):
    try:
        return DetectionModelDB().get_model_function(modelversionid )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/model-version", tags=["Model"])
def get_model_version(modelversionid : int):
    try:
        return DetectionModelDB().get_model_version(modelversionid )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/model-image", tags=["Model"])
def get_model_version(modelversionid: int):
    try:
        return DetectionModelDB().get_model_image(modelversionid)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/model-camera", tags=["Model"])
def get_model_camera(modelversionid: int):
    try:
        return DetectionModelDB().get_model_camera(modelversionid)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/model-detail", tags=["Model"])
def get_model_detail(modelversionid: int, db: Session = Depends(get_db)):
    try:
        return DetectionModelService().get_model_detail(modelversionid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))  
    
@app.get("/detection-model", tags=["Model"])
def get_detection_model(db: Session = Depends(get_db)):
    try:
        return DetectionModelService().get_detection_model(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/add-model", tags=["Model"])
def add_model(model: schemas.DetectionModelCreate, db: Session = Depends(get_db)):
    try:
        return DetectionModelService().add_model(model, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.delete("/delete-model", tags=["Model"])
def delete_model(modelid: str, db: Session = Depends(get_db)):
    try:
        return DetectionModelService().delete_model(modelid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/update-model-step1", tags=["Model"])
def update_model_step1(modelversionid: str, model: schemas.DetectionModelUpdateStep1, db: Session = Depends(get_db)):
    try:
        return DetectionModelService().update_model_step1(modelversionid, model, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/update-model-step2", tags=["Model"])
def update_model_step2(
    modelversionid: int = Form(...),
    modelid: int = Form(...),
    updatedby: str = Form(...),
    files: List[UploadFile] = File(...),
    db: Session = Depends(get_db)
):
    try:
        return DetectionModelService().update_model_step2(modelversionid, modelid, updatedby, files, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.put("/update-model-step3", tags=["Model"])
def update_model_step3(modelversionid: str, model: schemas.DetectionModelUpdateStep3, db: Session = Depends(get_db)):
    try:
        return DetectionModelService().update_model_step3(modelversionid, model, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.put("/update-model-step4", tags=["Model"])
def update_model_step4(modelversionid: str, model: schemas.DetectionModelUpdateStep4, db: Session = Depends(get_db)):
    try:
        return DetectionModelService().update_model_step4(modelversionid, model, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

    
# -------------------- Transaction Service --------------------
@app.get("/transaction", tags=["Transaction"])
def transaction():
    try:
        return {"transaction": transaction_db.get_transaction()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/add-transaction", tags=["Transaction"])
def add_transaction(txn: schemas.TransactionCreate, db: Session = Depends(get_db)):
    try:
        return transaction_db.add_transaction(txn, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/update-transaction", tags=["Transaction"])
def update_transaction(runningno: int, txn: schemas.TransactionUpdate, db: Session = Depends(get_db)):
    try:
        return transaction_db.update_transaction(runningno, txn, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------- Report Service --------------------
@app.get("/report-product-defect", tags=["Report"])
def product_defect_results():
    try:
        return {"product_defect_results": ReportDB().get_product_defect_results()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/report-defect-summary", tags=["Report"])
def defect_summary():
    try:
        return {"defect_summary": ReportDB().get_defect_summary()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/add-report-defect", tags=["Report"])
def add_report_defect(item: schemas.ReportDefectCreate, db: Session = Depends(get_db)):
    try:
        return ReportDB().add_report_defect(item, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/update-report-defect", tags=["Report"])
def update_report_defect(lotno: str, item: schemas.ReportDefectUpdate, db: Session = Depends(get_db)):
    try:
        return ReportDB().update_report_defect(lotno, item, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/add-report-product", tags=["Report"])
def add_report_product(item: schemas.ReportProductCreate, db: Session = Depends(get_db)):
    try:
        return ReportDB().add_report_product(item, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/add-report-product-detail", tags=["Report"])
def add_product_detail(item: schemas.ProductDetailCreate, db: Session = Depends(get_db)):
    try:
        return ReportDB().add_product_detail(item, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/update-product-detail", tags=["Report"])
def update_report_product(productid: str, item: schemas.ReportProductUpdate, db: Session = Depends(get_db)):
    try:
        return ReportDB().update_report_product(productid, item, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------- Permission Service --------------------
@app.put("/update_permission", tags=["Permission"])
def update_permission_api(permissionid: int, perm: schemas.PermissionUpdate, db: Session = Depends(get_db)):
    try:
        return PermissionDB().update_permission(permissionid, perm, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/delete_permission", tags=["Permission"])
def delete_permission_api(permissionid: int, db: Session = Depends(get_db)):
    try:
        return permission_db.delete_permission(permissionid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------- Menu Service --------------------
@app.post("/add_menu", tags=["Menu"])
def add_menu_api(menu: schemas.MenuCreate, db: Session = Depends(get_db)):
    try:
        return menu_db.add_menu(menu, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/update_menu", tags=["Menu"])
def update_menu_api(menuid: str, menu: schemas.MenuUpdate, db: Session = Depends(get_db)):
    try:
        return menu_db.update_menu(menuid, menu, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
  
# -------------------- Dashboard Service --------------------
@app.get("/dashboard-defectscamera", tags=["Dashboard"])
def endpoint_defects_camera(start: datetime, end: datetime, db: Session = Depends(get_db)):
    try:
      return DashboardService.get_defects_with_ng_gt_zero(start, end, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dashboard-goodngratio", tags=["Dashboard"])
def endpoint_ratio(
    start: datetime,
    end: datetime,
    productname: Optional[str] = Query(None),
    prodline: Optional[str] = Query(None),
    cameraid: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
      return DashboardService.get_ratio(start, end, productname, prodline, cameraid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dashboard-ngdistribution", tags=["Dashboard"])
def endpoint_distribution(
    start: datetime,
    end: datetime,
    productname: Optional[str] = Query(None),
    prodline: Optional[str] = Query(None),
    cameraid: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
      return DashboardService.ng_distribution(start, end, productname, prodline, cameraid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dashboard-top5defects", tags=["Dashboard"])
def endpoint_top5defects(
    start: datetime,
    end: datetime,
    productname: Optional[str] = Query(None),
    prodline: Optional[str] = Query(None),
    cameraid: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
      return DashboardService.top_5_defects(start, end, productname, prodline, cameraid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dashboard-top5trends", tags=["Dashboard"])
def endpoint_top5trends(
    start: datetime,
    end: datetime,
    db: Session = Depends(get_db)
):
    try:
      return DashboardService.top_5_trends(start, end, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/dashboard-totalproduct", tags=["Dashboard"])
def get_total_products(
    start: datetime,
    end: datetime,
    productname: Optional[str] = Query(None),
    prodline: Optional[str] = Query(None),
    cameraid: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        return DashboardService.get_total_products(start, end, productname, prodline, cameraid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/filter-lines", tags=["Dashboard"])
def get_lines_dropdown_list(db: Session = Depends(get_db)):
    try:
        return DashboardService.get_lines_list(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/filter-products", tags=["Dashboard"]) 
def get_products_dropdown_list(db: Session = Depends(get_db)):
    try:
        return DashboardService.get_products_list(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/filter-cameras", tags=["Dashboard"])
def get_cameras_dropdown_list(db: Session = Depends(get_db)):
    try:
        return DashboardService.get_cameras_list(db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------- Service --------------------


# -------------------- Run Server --------------------
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

