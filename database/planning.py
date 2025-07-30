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
    def _fetch_one(self, query: str, params: dict):
        try:
            with engine.connect() as conn:
              result = conn.execute(text(query), params)
              return result.mappings().first()
        except SQLAlchemyError as e:
            print(f"Database error: {e}")
            return []

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
        
    def get_planning(self, model: schemas.PlanningSearch):
        where_filters = []
        params = {}

        allowed_order_fields = [ "planid", "startdatetime", "enddatetime", "actualstartdatetime", "actualenddatetime", "prodid", "prodname", "prodlot", "prodline", "quantity" ]

        order_by = model.order_by if model.order_by in allowed_order_fields else "startdatetime"
        order_dir = "DESC" if model.order_dir.lower() == "desc" else "ASC"

        # --- Filters (WHERE) ---
        if model.startdatetime:
            where_filters.append("pl.startdatetime >= :startdatetime")
            params["startdatetime"] = model.startdatetime

        if model.enddatetime:
            where_filters.append("pl.enddatetime <= :enddatetime")
            params["enddatetime"] = model.enddatetime

        if model.planid:
            where_filters.append("pl.planid ILIKE :planid")
            params["planid"] = f"%{model.planid}%"

        if model.prodid:
            where_filters.append("pl.prodid ILIKE :prodid")
            params["prodid"] = f"%{model.prodid}%"

        if model.prodname:
            where_filters.append("p.prodname ILIKE :prodname")
            params["prodname"] = f"%{model.prodname}%"

        if model.prodlot:
            where_filters.append("pl.prodlot ILIKE :prodlot")
            params["prodlot"] = f"%{model.prodlot}%"

        if model.prodline:
            where_filters.append("pl.prodline ILIKE :prodline")
            params["prodline"] = f"%{model.prodline}%"

        where_clause = " WHERE " + " AND ".join(where_filters) if where_filters else ""

        # --- Pagination ---
        page = model.page or 1
        page_size = model.pageSize or 10
        offset = (page - 1) * page_size

        # --- Main Query (with LIMIT) ---
        main_query = f"""
            SELECT pl.planid, pl.startdatetime, pl.enddatetime, pl.actualstartdatetime, pl.actualenddatetime, pl.prodid, p.prodname, pl.prodlot, pl.prodline, pl.quantity
            FROM planning pl
            LEFT JOIN product p ON p.prodid = pl.prodid 
            {where_clause}
            ORDER BY {order_by} {order_dir}
            LIMIT :limit OFFSET :offset
        """
        params["limit"] = page_size
        params["offset"] = offset

        # --- Total Count Query ---
        count_query = f"""
            SELECT COUNT(*) FROM (
                SELECT 1
                FROM planning pl
                LEFT JOIN product p ON p.prodid = pl.prodid 
                {where_clause}
            ) AS count
        """

        # print("SQL Query:", main_query)
        # print("Parameters:", params)

        total = self._fetch_one(count_query, params)["count"]
        items = self._fetch_all(main_query, params)

        return {
            "total": total,
            "items": items
        }

    def suggest_planid(self, q: str):
        rows = self._fetch_all("""
            SELECT DISTINCT planid FROM planning
            WHERE LOWER(planid) LIKE LOWER(:keyword)
            ORDER BY planid ASC
            LIMIT 10; """,
            {"keyword": q + "%"}
        )
        return [{"value": row["planid"], "label": row["planid"]} for row in rows]
    
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
          # if plan.actualstartdatetime: update_fields["actualstartdatetime"] = plan.actualstartdatetime
          # if plan.actualenddatetime: update_fields["actualenddatetime"] = plan.actualenddatetime

          set_clause = ", ".join([f"{key} = :{key}" for key in update_fields if key != "update_planid"])
          update_sql = text(f"UPDATE planning SET {set_clause} WHERE planid = :update_planid")

          result = self._fetch_one( "SELECT prodname FROM product WHERE prodid = :prodid", {"prodid": plan.prodid})
          prodname = result["prodname"] if result else None

          db.execute(update_sql, update_fields)
          db.commit()
          return success_response(200, {"planid": update_fields.get("planid", planid), "prodname": prodname, "updateddate": str(now)})
      except Exception as e:
          db.rollback()
          return error_response(500, f"Database error: {str(e)}")
    
    def delete_planning(self, planid: str, db: Session):
        # check planid
        planning_record = db.execute(text("""
            SELECT prodlot, prodid FROM planning WHERE planid = :planid
        """), {"planid": planid}).first()

        if not planning_record:
            return error_response(404, "Plan not found")

        prodlot, prodid = planning_record

        # check in transactionreport
        transaction_exists = db.execute(text("""
            SELECT 1 FROM transactionreport
            WHERE prodlot = :prodlot AND prodid = :prodid
            LIMIT 1
        """), {"prodlot": prodlot, "prodid": prodid}).first()

        if transaction_exists:
            return error_response(400, f"Cannot delete: This planning is used in transactionreport.")

        # delete
        db.execute(text("DELETE FROM planning WHERE planid = :planid"), {"planid": planid})
        db.commit()
        return success_response(200, {"planid": planid, "isdeleted": True})
    
    def start_planning(self, plan: schemas.PlanningStart, db: Session):
        now = datetime.now()

        planning_record = db.execute(text("""
            SELECT planid FROM planning WHERE planid = :planid
        """), {"planid": plan.planid}).first()

        if not planning_record:
            return error_response(404, "Plan not found")

        db.execute(text("""
            UPDATE planning 
            SET actualstartdatetime = :actualstartdatetime,
                updatedby = :updatedby,
                updateddate = :updateddate
            WHERE planid = :planid
        """), {
            "planid": plan.planid,
            "actualstartdatetime": now,
            "updatedby": plan.updatedby,
            "updateddate": now,
        })

        db.commit()
        
        return success_response(200, {
            "planid": plan.planid,
            "actualstartdatetime": str(now),
            "updateddate": str(now)
        })

    def stop_planning(self, plan: schemas.PlanningStop, db: Session):
        now = datetime.now()

        planning_record = db.execute(text("""
            SELECT planid FROM planning WHERE planid = :planid
        """), {"planid": plan.planid}).first()

        if not planning_record:
            return error_response(404, "Plan not found")

        db.execute(text("""
            UPDATE planning 
            SET actualenddatetime = :actualenddatetime,
                updatedby = :updatedby,
                updateddate = :updateddate
            WHERE planid = :planid
        """), {
            "planid": plan.planid,
            "actualenddatetime": now,
            "updatedby": plan.updatedby,
            "updateddate": now,
        })

        db.commit()
        return success_response(200, {
            "planid": plan.planid,
            "actualenddatetime": str(now),
            "updateddate": str(now)
        })
    
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

    