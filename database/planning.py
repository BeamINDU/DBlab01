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
        # Check if planning already exists
        existing_planning = db.execute(
            text("SELECT isdeleted FROM planning WHERE planid = :planid"),
            {"planid": plan.planid}
        ).first()

        now = datetime.now()

        if existing_planning:
            if not existing_planning.isdeleted:  # isdeleted = False
                return error_response(400, "Plan ID already exists")

            # If isdeleted = true, restore the old record
            update_sql = text("""
                UPDATE planning SET
                    prodid = :prodid,
                    prodlot = :prodlot,
                    prodline = :prodline,
                    quantity = :quantity,
                    startdatetime = :startdatetime,
                    enddatetime = :enddatetime,
                    createdby = :createdby,
                    createddate = :createddate,
                    isdeleted = false
                WHERE planid = :planid
            """)
            db.execute(update_sql, {
                "planid": plan.planid,
                "prodid": plan.prodid,
                "prodlot": plan.prodlot,
                "prodline": plan.prodline,
                "quantity": plan.quantity,
                "startdatetime": plan.startdatetime,
                "enddatetime": plan.enddatetime,
                "createdby": plan.createdby,
                "createddate": now,
                "updatedby": None  ,
                "updateddate": None  
            })

        else:
            # Insert new record
            insert_sql = text("""
                INSERT INTO planning (
                    planid, prodid, prodlot, prodline, quantity, startdatetime, enddatetime,
                    createdby, createddate, isdeleted
                ) VALUES (
                    :planid, :prodid, :prodlot, :prodline, :quantity, :startdatetime, :enddatetime,
                    :createdby, :createddate, false
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
        # Check if planning already exists
        if not db.execute(text("SELECT 1 FROM planning WHERE planid = :planid"), {"planid": planid}).first():
            return error_response(404, "Plan ID not found")

        update_fields = {}
        now = datetime.now()

        # Check updatedby (user id)
        if plan.updatedby:
            if not db.execute(text("SELECT 1 FROM \"user\" WHERE userid = :userid"),
                              {"userid": plan.updatedby}).first():
                return error_response(400, "Invalid user (updatedby)")
            update_fields["updatedby"] = plan.updatedby

        # Check prodid
        if plan.prodid is not None:
            if not db.execute(text("SELECT 1 FROM product WHERE prodid = :prodid"),
                              {"prodid": plan.prodid}).first():
                return error_response(400, "Invalid Product ID")
            update_fields["prodid"] = plan.prodid


        # Check planid duplicate (not self)
        if plan.planid and plan.planid != planid:
            duplicate_check = db.execute(text("""
                SELECT isdeleted FROM planning WHERE planid = :new_planid
            """), {"new_planid": plan.planid}).first()

            if duplicate_check:
                if not duplicate_check.isdeleted:
                    return error_response(400, "New Plan ID already exists")
                else:
                    # duplicate → delete record where isdeleted = true
                    db.execute(text("DELETE FROM planning WHERE planid = :new_planid"), {"new_planid": plan.planid})
                    db.commit()

            update_fields["planid"] = plan.planid

        # field other
        if plan.prodlot: update_fields["prodlot"] = plan.prodlot
        if plan.prodline: update_fields["prodline"] = plan.prodline
        if plan.startdatetime: update_fields["startdatetime"] = plan.startdatetime
        if plan.enddatetime: update_fields["enddatetime"] = plan.enddatetime

        update_fields["updateddate"] = now

        if not update_fields:
          return error_response(400, "No fields to update")

        update_fields["old_planid"] = planid
        set_clause = ", ".join([f"{key} = :{key}" for key in update_fields if key != "old_planid"])

        update_sql = text(f"UPDATE planning SET {set_clause} WHERE planid = :old_planid")
        db.execute(update_sql, update_fields)
        db.commit()
        return success_response(200, { "planid": update_fields.get("planid", planid), "updateddate": str(now)})
    
    @staticmethod
    def delete_planning(planid: str, db: Session):
        if not db.execute(text("SELECT 1 FROM planning WHERE planid = :planid"), {"planid": planid}).first():
            return error_response(404, "Plan not found")

        db.execute(text("UPDATE planning SET isdeleted = true WHERE planid = :planid"), {"planid": planid})
        db.commit()
        return success_response(200,{"planid": planid, "isdeleted": True})


