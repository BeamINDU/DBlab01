from database.connect_to_db import engine, SessionLocal, Session, text, SQLAlchemyError
from fastapi import HTTPException
import database.schemas as schemas
from datetime import datetime
from fastapi.responses import JSONResponse
from typing import Union, Dict, Any
from fastapi import UploadFile
import pandas as pd

def error_response(code: int, message: str):
    return JSONResponse( status_code=code, content={"detail": {"error": message}} )

def success_response(code: int, content: Union[Dict[str, Any], str]):
    return JSONResponse( status_code=code, content=content)

class DefectDB:
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
        
    def get_defect_types(self):
        return self._fetch_all("SELECT * FROM defecttype WHERE isdeleted = false")
    
    def suggest_defecttype_id(self, q: str):
        rows = self._fetch_all("""
            SELECT DISTINCT defectid FROM defecttype
            WHERE isdeleted = false AND defectstatus = true AND LOWER(defectid) LIKE LOWER(:keyword)
            ORDER BY defectid ASC
            LIMIT 10; """,
            {"keyword": q + "%"}
        )
        return [{"value": row["defectid"], "label": row["defectid"]} for row in rows]
    
    def suggest_defecttype_name(self, q: str):
        rows = self._fetch_all("""
            SELECT DISTINCT defecttype FROM defecttype
            WHERE isdeleted = false AND defectstatus = true AND LOWER(defecttype) LIKE LOWER(:keyword)
            ORDER BY defecttype ASC
            LIMIT 10; """,
            {"keyword": q + "%"}
        )
        return [{"value": row["defecttype"], "label": row["defecttype"]} for row in rows]
        
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
    def delete_defect_type(defectid: str, db: Session):
        if not db.execute(text("SELECT 1 FROM defecttype WHERE defectid = :defectid"), {"defectid": defectid}).first():
            return error_response(404, "Defect type not found")

        db.execute(text("UPDATE defecttype SET isdeleted = true WHERE defectid = :defectid"), {"defectid": defectid})
        db.commit()
        return success_response(200, {"defectid": defectid, "isdeleted": True})
      
    @staticmethod
    async def upload_defect_types(uploadby: str, file: UploadFile, db: Session):
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
        defect_type_data = []
        for _, row in df.iterrows():
            defect_type_data.append({
                "defectid": row.get("Defect Type ID"),
                "defecttype": row.get("Defect Type Name"),
                "defectdescription": row.get("Description"),
                "defectstatus": row.get("Status", "Active"),
                "createdby": uploadby,
                "createddate": now
            })

        # SQL สำหรับ insert
        insert_sql = """
            INSERT INTO defecttype (
                defectid, defecttype, defectdescription, defectstatus, createdby, createddate
            )
            VALUES (
                :defectid, :defecttype, :defectdescription, :defectstatus, :createdby, :createddate
            )
        """
        # ทำ bulk insert
        db.execute(text(insert_sql), defect_type_data)
        db.commit()
        return success_response(200,{"message": f"{len(defect_type_data)} records uploaded successfully!"})
 
      except Exception as e:
          print(f"Error uploading defect type: {e}")
          db.rollback()
          raise error_response(500, "Failed to upload defect type")
      
