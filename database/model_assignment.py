from database.connect_to_db import engine, SessionLocal, Session, text, SQLAlchemyError
from datetime import datetime
import database.schemas as schemas
from fastapi.responses import JSONResponse
from typing import Union, Dict, Any
from fastapi import UploadFile
import pandas as pd

def error_response(code: int, message: str):
    return JSONResponse( status_code=code, content={"detail": {"error": message}} )

def success_response(code: int, content: Union[Dict[str, Any], str]):
    return JSONResponse( status_code=code, content=content)

class ModelAssignmentDB:
    def _fetch_all(self, query: str, params: dict = None):
        try:
            with engine.connect() as conn:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))
                return list(result.mappings())
        except SQLAlchemyError as e:
            print(f"Database error: {e}")
            return []
        
    def get_model_assignments(self, model: schemas.ModelAssignmentSearch):
        filters = []
        params = {}
        
        if model.modelname:
            filters.append("mv.modelname ILIKE :modelname")
            params["modelname"] = f"%{model.modelname}%"

        if model.prodid:
            filters.append("cmp.prodid ILIKE :prodid")
            params["prodid"] = f"%{model.prodid}%"
            
        if model.cameraid:
            filters.append("cmp.cameraid ILIKE :cameraid")
            params["cameraid"] = f"%{model.cameraid}%"

        if model.appliedstatus is not None:
            filters.append("cmp.appliedstatus = :appliedstatus")
            params["appliedstatus"] = model.appliedstatus

        where_clause = " WHERE " + " AND ".join(filters) if filters else ""

        query = f"""
            SELECT mv.modelid, mv.modelname, mv.versionno, p.prodname, c.cameraname,  cmp.*
            FROM cameramodelprodapplied cmp 
            LEFT JOIN modelversion mv ON mv.modelversionid = cmp.modelversionid
            LEFT JOIN product p ON p.prodid = cmp.prodid 
            LEFT JOIN camera c  on c.cameraid = cmp.cameraid
            {where_clause}
            ORDER BY mv.modelname
        """

        return self._fetch_all(query, params)

class ModelAssignmentService:
    
    @staticmethod
    def update_model_assignment(id: int, model: schemas.ModelAssignmentUpdate, db: Session):
        try:
            update_fields = {}
            now = datetime.now()

            # ตรวจสอบ appliedby
            if not db.execute(text("SELECT 1 FROM \"user\" WHERE userid = :userid"), {"userid": model.appliedby}).first():
                return error_response(400, "Invalid user (appliedby)")
            
            # ดึงข้อมูลปัจจุบันจาก record
            current_record = db.execute(text("""
                SELECT prodid, cameraid, modelversionid
                FROM cameramodelprodapplied 
                WHERE id = :id
            """), {"id": id}).first()

            if not current_record:
                return error_response(404, "Model assignment not found")

            current_prodid = current_record.prodid
            current_cameraid = current_record.cameraid
            current_modelversionid = current_record.modelversionid

            # print("modelid", model.modelid)
            # print("current_modelversionid", current_modelversionid)
            # print("new_modelversionid", model.modelversionid)

            # ตรวจสอบ modelversionid
            if model.modelversionid != current_modelversionid:
              if db.execute(text("SELECT 1 FROM modelversion WHERE modelversionid = :modelversionid"), {"modelversionid": model.modelversionid}).first():
                  return error_response(400, f"Model version {model.version} already exists.")

            # ตรวจสอบซ้ำของ productId และ cameraId
            if db.execute(text("""
                SELECT 1 
                FROM cameramodelprodapplied 
                WHERE prodid = :prodid 
                  AND cameraid = :cameraid 
                  AND appliedstatus = true 
                  AND id != :id
            """), {"prodid": model.prodid, "cameraid": model.cameraid, "id": id}).first():
                return error_response(400, "This Product ID and Camera already have an active model assigned.")

            # ตรวจสอบว่ามี modelversionid ที่ active ซ้ำหรือไม่
            if model.appliedstatus:
                if db.execute(text("""
                    SELECT 1
                    FROM cameramodelprodapplied
                    WHERE modelversionid = :modelversionid
                      AND appliedstatus = true
                      AND id != :id
                """), {"modelversionid": model.modelversionid, "id": id}).first():
                    return error_response(400, "This model version is already active in another assignment.")

            # เตรียมข้อมูลอัปเดต
            update_fields.update({
                "cameraid": model.cameraid,
                "modelversionid": model.modelversionid,
                "appliedstatus": model.appliedstatus,
                "appliedby": model.appliedby,
                "id": id
            })

            if model.appliedstatus:
                update_fields["applieddate"] = now

            # อัปเดต cameramodelprodapplied
            set_clause = ", ".join([f"{key} = :{key}" for key in update_fields if key != "id"])
            update_sql = text(f"UPDATE cameramodelprodapplied SET {set_clause} WHERE id = :id")
            db.execute(update_sql, update_fields)

            if model.appliedstatus:
                # อัปเดต 'modelversion' ให้เป็น Using
                update_new_model_sql = text("""
                    UPDATE modelversion 
                    SET modelstatus = :modelstatus,
                        updatedby = :updatedby,
                        updateddate = :updateddate
                    WHERE modelversionid = :modelversionid
                """)
                db.execute(update_new_model_sql, {
                    "modelstatus": "Using",
                    "updatedby": model.appliedby,
                    "updateddate": now,
                    "modelversionid": model.modelversionid
                })

                # อัปเดต 'modelversion' อื่นที่มี modelid เดียวกัน แต่ไม่ใช่ตัวปัจจุบัน และ status ไม่ใช่ 'Using', 'Ready' ให้เป็น "Ready"
                update_current_model_sql = text("""
                    UPDATE modelversion 
                    SET modelstatus = :modelstatus,
                        updatedby = :updatedby,
                        updateddate = :updateddate
                    WHERE modelid = :modelid
                        AND modelversionid != :modelversionid
                        AND modelstatus NOT IN ('Using', 'Ready')                 
                """)
                db.execute(update_current_model_sql, {
                    "modelstatus": "Ready",
                    "updatedby": model.appliedby,
                    "updateddate": now,
                    "modelid": model.modelid,
                    "modelversionid": model.modelversionid
                })
            else:
                # อัปเดต 'modelversion' ให้เป็น Ready
                update_new_model_sql = text("""
                    UPDATE modelversion 
                    SET modelstatus = :modelstatus,
                        updatedby = :updatedby,
                        updateddate = :updateddate
                    WHERE modelversionid = :modelversionid
                """)
                db.execute(update_new_model_sql, {
                    "modelstatus": "Ready",
                    "updatedby": model.appliedby,
                    "updateddate": now,
                    "modelversionid": model.modelversionid
                })

            db.commit()
            return success_response(200, {
                "id": id,
                "applieddate": str(now),
                "previous_prodid": current_prodid,
                "previous_cameraid": current_cameraid
            })

        except Exception as e:
            db.rollback()
            return error_response(500, f"Database error: {str(e)}")



