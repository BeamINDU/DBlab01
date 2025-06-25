from database.connect_to_db import engine, Session, text, SQLAlchemyError
from fastapi import HTTPException
from datetime import datetime
from fastapi.responses import JSONResponse
import database.schemas as schemas
from typing import Union, Dict, Any

def error_response(code: int, message: str):
    return JSONResponse( status_code=code, content={"detail": {"error": message}} )

def success_response(code: int, content: Union[Dict[str, Any], str]):
    return JSONResponse( status_code=code, content=content)

class PlanningDB:
    def _fetch_all(self, query: str):
        try:
            with engine.connect() as conn:
                result = conn.execute(text(query))
                return [dict(row) for row in result.mappings()]
        except SQLAlchemyError as e:
            print(f"Database error: {e}")
            return []

    def get_planning(self):
        return self._fetch_all("SELECT * FROM planning")

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


