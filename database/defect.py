from database.connect_to_db import engine, SessionLocal, Session, text, SQLAlchemyError
from fastapi import HTTPException
import database.schemas as schemas
from datetime import datetime
from fastapi.responses import JSONResponse
from typing import Union, Dict, Any

def error_response(code: int, message: str):
    return JSONResponse( status_code=code, content={"detail": {"error": message}} )

def success_response(code: int, content: Union[Dict[str, Any], str]):
    return JSONResponse( status_code=code, content=content)

class DefectDB:
    def get_defect_types(self):
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT * FROM defecttype WHERE isdeleted = false"))
                return [dict(row) for row in result.mappings()]
        except SQLAlchemyError as e:
            print(f"Database error: {e}")
            return []
        
    def add_defect_type(self, defect: schemas.DefectTypeCreate, db: Session):
        # Check if user exists
        if not db.execute(text("SELECT 1 FROM \"user\" WHERE userid = :userid"),
                      {"userid": defect.createdby}).first():
          return error_response(400, "Invalid user (createdBy)")

        # Check if defect already exists
        existing_defect_type = db.execute(
            text("SELECT isdeleted FROM defecttype WHERE defectid = :defectid"),
            {"defectid": defect.defectid}
        ).first()

        now = datetime.now()

        if existing_defect_type:
            if not existing_defect_type:  # isdeleted = False
                return error_response(400, "Defect ID already exists")

            # If isdeleted = true, restore the old record
            update_sql = text("""
                UPDATE defecttype SET
                    defecttype = :defecttype,
                    defectdescription = :defectdescription,
                    defectstatus = :defectstatus,
                    isdeleted = false,
                    createdby = :createdby,
                    createddate = :createddate
                WHERE defectid = :defectid
            """)
            db.execute(update_sql, {
                "defectid": defect.defectid,
                "defecttype": defect.defecttype,
                "defectdescription": defect.defectdescription,
                "defectstatus": defect.defectstatus,
                "createdby": defect.createdby,
                "createddate": now,
                "updatedby": None  ,
                "updateddate": None  
            })
        else:
            # Insert new record
            insert_sql = text("""
                INSERT INTO defecttype (
                    defectid, defecttype, defectdescription,
                    defectstatus, createdby, createddate, isdeleted
                ) VALUES (
                    :defectid, :defecttype, :defectdescription,
                    :defectstatus, :createdby, :createddate, false
                )
            """)
            db.execute(insert_sql, {
                "defectid": defect.defectid,
                "defecttype": defect.defecttype,
                "defectdescription": defect.defectdescription,
                "defectstatus": defect.defectstatus,
                "createdby": defect.createdby,
                "createddate": defect.createddate,
            })

        db.commit()
        return success_response(200, {"defectid": defect.defectid, "createddate": str(now)})
    
    def update_defect_type(self, defectid: str, defect: schemas.DefectTypeUpdate, db: Session):
        if not db.execute(text("SELECT 1 FROM defecttype WHERE defectid = :defectid"),
                            {"defectid": defectid}).first():
            return error_response(404, "Defect type not found")

        update_fields = {}
        now = datetime.now()

        if defect.updatedby:
          if not db.execute(text("SELECT 1 FROM \"user\" WHERE userid = :userid"),
                            {"userid": defect.updatedby}).first():
              return error_response(400, "Invalid user (updatedby)")
          update_fields["updatedby"] = defect.updatedby


        if defect.defectid and defect.defectid != defectid:
          duplicate_check = db.execute(text("""
              SELECT isdeleted FROM defecttype WHERE defectid = :defectid
          """), {"new_defectid": defect.defectid}).first()

          if duplicate_check:
              if not duplicate_check.isdeleted:
                  return error_response(400, "New Defect Type already exists")
              else:
                  db.execute(
                        text("UPDATE defecttype SET isdeleted = true WHERE defectid = :new_defectid"),
                        {"new_defectid": defect.defectid}
                    )
                  db.commit()

              update_fields["defectid"] = defect.defectid
          
        if defect.defecttype is not None:
            update_fields["defecttype"] = defect.defecttype
        if defect.defectdescription is not None:
            update_fields["defectdescription"] = defect.defectdescription
        if defect.defectstatus is not None:
            update_fields["defectstatus"] = defect.defectstatus
        if defect.updatedby:
            update_fields["updatedby"] = defect.updatedby

        update_fields["updateddate"] = now

        if not update_fields:
            return error_response(404, "No fields to update")

        update_fields["old_defectid"] = defectid
        set_clause = ", ".join([f"{key} = :{key}" for key in update_fields if key != "old_defectid"])

        update_sql = text(f"UPDATE defecttype SET {set_clause} WHERE defectid = :old_defectid")
        db.execute(update_sql, update_fields)
        db.commit()
        return success_response(200, { "defectid": update_fields.get("defectid", defectid), "updateddate": str(now)})
        
    @staticmethod
    def delete_defecttype(defectid: str, db: Session):
        if not db.execute(text("SELECT 1 FROM defecttype WHERE defectid = :defectid"), {"defectid": defectid}).first():
            return error_response(404, "Defect type not found")

        db.execute(text("UPDATE defecttype SET isdeleted = true WHERE defectid = :defectid"), {"defectid": defectid})
        db.commit()
        return success_response(200, {"defectid": defectid, "isdeleted": True})
      

