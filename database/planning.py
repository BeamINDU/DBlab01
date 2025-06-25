from database.connect_to_db import engine, Session, text, SQLAlchemyError
from fastapi import HTTPException
from datetime import datetime
from fastapi.responses import JSONResponse
import database.schemas as schemas
from typing import Union, Dict, Any
from fastapi import UploadFile
import pandas as pd

def error_response(code: int, message: str):
    return JSONResponse( status_code=code, content={"detail": {"error": message}} )

def success_response(code: int, content: Union[Dict[str, Any], str]):
    return JSONResponse( status_code=code, content=content)

class PlanningDB:
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
        
    def get_planning(self):
        return self._fetch_all("SELECT * FROM planning")
    
    def suggest_planid(self, q: str):
        rows = self._fetch_all("""
            SELECT DISTINCT prodid FROM planning
            WHERE LOWER(prodid) LIKE LOWER(:keyword)
            ORDER BY prodid ASC
            LIMIT 10; """,
            {"keyword": q + "%"}
        )
        return [{"value": row["prodid"], "label": row["prodid"]} for row in rows]
    
    def suggest_plan_lotno(self, q: str):
        rows = self._fetch_all("""
            SELECT DISTINCT prodlot FROM planning
            WHERE LOWER(prodlot) LIKE LOWER(:keyword)
            ORDER BY prodlot ASC
            LIMIT 10; """,
            {"keyword": q + "%"}
        )
        return [{"value": row["prodlot"], "label": row["prodlot"]} for row in rows]
    
    def suggest_plan_lineid(self, q: str):
        rows = self._fetch_all("""
            SELECT DISTINCT prodline FROM planning
            WHERE LOWER(prodline) LIKE LOWER(:keyword)
            ORDER BY prodline ASC
            LIMIT 10; """,
            {"keyword": q + "%"}
        )
        return [{"value": row["prodline"], "label": row["prodline"]} for row in rows]
    
    def add_planning(self, plan: schemas.PlanningCreate, db: Session):
        now = datetime.now()

        # Validate planid
        if db.execute(text("SELECT 1 FROM planning WHERE planid = :planid"), {"planid": plan.planid}).first():
            return error_response(400, "New Plan ID already exists")

        # Validate createdby
        if not db.execute(text("SELECT 1 FROM \"user\" WHERE userid = :userid"), {"userid": plan.createdby}).first():
            return error_response(400, "Invalid user (createdby)")
        
        # Validate prodid
        if not db.execute(text("SELECT 1 FROM product WHERE prodid = :prodid"), {"prodid": plan.prodid}).first():
            return error_response(400, "Invalid Product ID")

        # Check duplicate prodlot + prodid
        duplicate_combo_check = db.execute(text("""
            SELECT planid FROM planning
            WHERE prodlot = :prodlot
              AND prodid = :prodid
        """), {
            "prodlot": plan.prodlot,
            "prodid": plan.prodid,
        }).first()

        if duplicate_combo_check:
            return error_response(400, f"Combination of prodlot '{plan.prodlot}' and prodid '{plan.prodid}' already exists in another plan.")
        
        # Insert new record
        insert_sql = text("""
            INSERT INTO planning (
                planid, prodid, prodlot, prodline, quantity, 
                startdatetime, enddatetime, createdby, createddate
            ) VALUES (
                :planid, :prodid, :prodlot, :prodline, :quantity, 
                :startdatetime, :enddatetime, :createdby, :createddate
            )
        """)
        db.execute(insert_sql, {
            "planid": plan.planid,
            "prodid": plan.prodid,
            "prodlot": plan.prodlot,
            "prodline": plan.prodline,
            "quantity": plan.quantity,
            "startdatetime": plan.startdatetime,
            "enddatetime": plan.enddatetime,
            "createdby": plan.createdby,
            "createddate": now,
        })

        db.commit()
        return success_response(200, {"planid": plan.planid, "createddate": str(now)})

    def update_planning(self, planid: str, plan: schemas.PlanningUpdate, db: Session):
      # Check if planning exists
      if not db.execute(text("SELECT 1 FROM planning WHERE planid = :planid"), {"planid": planid}).first():
          return error_response(404, "Plan ID not found")

      update_fields = {}
      now = datetime.now()
      update_fields["planid"] = plan.planid
      update_fields["updateddate"] = now
      update_fields["update_planid"] = planid

      # Validate updatedby
      if not db.execute(text("SELECT 1 FROM \"user\" WHERE userid = :userid"), {"userid": plan.updatedby}).first():
          return error_response(400, "Invalid user (updatedby)")
      update_fields["updatedby"] = plan.updatedby

      # Validate prodid
      if not db.execute(text("SELECT 1 FROM product WHERE prodid = :prodid"), {"prodid": plan.prodid}).first():
          return error_response(400, "Invalid Product ID")
      update_fields["prodid"] = plan.prodid

      # Check duplicate prodlot + prodid
      duplicate_combo_check = db.execute(text("""
          SELECT planid FROM planning
          WHERE prodlot = :prodlot
            AND prodid = :prodid
            AND planid != :planid
      """), {
          "prodlot": plan.prodlot,
          "prodid": plan.prodid,
          "planid": planid
      }).first()

      if duplicate_combo_check:
          return error_response(400, f"Combination of prodlot '{plan.prodlot}' and prodid '{plan.prodid}' already exists in another plan.")

      try:
          # Add update fields
          if plan.prodlot: update_fields["prodlot"] = plan.prodlot
          if plan.prodline: update_fields["prodline"] = plan.prodline
          if plan.startdatetime: update_fields["startdatetime"] = plan.startdatetime
          if plan.enddatetime: update_fields["enddatetime"] = plan.enddatetime

          set_clause = ", ".join([f"{key} = :{key}" for key in update_fields if key != "update_planid"])
          update_sql = text(f"UPDATE planning SET {set_clause} WHERE planid = :update_planid")

          db.execute(update_sql, update_fields)
          db.commit()
          return success_response(200, {"planid": update_fields.get("planid", planid), "updateddate": str(now)})
      except Exception as e:
          db.rollback()
          return error_response(500, f"Database error: {str(e)}")
    
    @staticmethod
    def delete_planning(planid: str, db: Session):
        if not db.execute(text("SELECT 1 FROM planning WHERE planid = :planid"), {"planid": planid}).first():
            return error_response(404, "Plan not found")

        db.execute(text("DELETE FROM planning WHERE planid = :planid"), {"planid": planid})
        db.commit()
        return success_response(200,{"planid": planid, "isdeleted": True})

    async def upload_planning(uploadby: str, file: UploadFile, db: Session):
      try:
        now = datetime.now()

        # ตรวจสอบประเภทไฟล์
        filename = file.filename.lower()
        file.file.seek(0)
        if filename.endswith(".xlsx") or filename.endswith(".xls"):
            df = pd.read_excel(file.file, engine="openpyxl")
        elif filename.endswith(".csv"):
            df = pd.read_csv(file.file)
        else:
            raise error_response(400, "File must be .xlsx or .csv")

        # แปลงข้อมูลแต่ละแถวเป็น dict ที่ตรงกับ SQL
        role_data = []
        for _, row in df.iterrows():
            role_data.append({
                "planid": row.get("Plan ID"),
                "prodid": row.get("Product ID"),
                "prodlot": row.get("Lot No"),
                "prodline": row.get("Line ID"),
                "quantity": row.get("Quantity"),
                "rolename": row.get("Role Name"),
                "startdatetime": pd.to_datetime(row.get("Start Date")) if pd.notnull(row.get("Start Date")) else None,
                "enddatetime": pd.to_datetime(row.get("End Date")) if pd.notnull(row.get("End Date")) else None,
                "createdby": uploadby,
                "createddate": now,
            })

        # SQL สำหรับ insert
        insert_sql = """
            INSERT INTO planning (
                planid, prodid, prodlot, prodline, quantity, startdatetime, enddatetime, createdby, createddate
            )
            VALUES (
                :planid, :prodid, :prodlot, :prodline, :quantity, :startdatetime, :enddatetime, :createdby, :createddate
            )
        """
        # ทำ bulk insert
        db.execute(text(insert_sql), role_data)
        db.commit()
        return success_response(200,{"message": f"{len(role_data)} records uploaded successfully!"})
 
      except Exception as e:
          print(f"Error uploading planning: {e}")
          db.rollback()
          raise error_response(500, "Failed to upload plan")
