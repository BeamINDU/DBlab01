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

class CameraDB:
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
        
    def get_cameras(self):
        return self._fetch_all("SELECT * FROM camera WHERE isdeleted = false")

    def suggest_camera_id(self, q: str):
        rows = self._fetch_all("""
            SELECT DISTINCT cameraid FROM camera
            WHERE isdeleted = false AND camerastatus = true AND LOWER(cameraid) LIKE LOWER(:keyword)
            ORDER BY cameraid ASC
            LIMIT 10; """,
            {"keyword": q + "%"}
        )
        return [{"value": row["cameraid"], "label": row["cameraid"]} for row in rows]
    
    def suggest_camera_name(self, q: str):
        rows = self._fetch_all("""
            SELECT DISTINCT cameraname FROM camera
            WHERE isdeleted = false AND camerastatus = true AND LOWER(cameraname) LIKE LOWER(:keyword)
            ORDER BY cameraname ASC
            LIMIT 10; """,
            {"keyword": q + "%"}
        )
        return [{"value": row["cameraname"], "label": row["cameraname"]} for row in rows]
    
    def suggest_camera_location(self, q: str):
        rows = self._fetch_all("""
            SELECT DISTINCT cameralocation FROM camera
            WHERE isdeleted = false AND camerastatus = true AND LOWER(cameralocation) LIKE LOWER(:keyword)
            ORDER BY cameralocation ASC
            LIMIT 10; """,
            {"keyword": q + "%"}
        )
        return [{"value": row["cameralocation"], "label": row["cameralocation"]} for row in rows]


class CameraService:
    @staticmethod
    def add_camera(camera: schemas.CameraCreate, db: Session):
        # Check if user exists
        if not db.execute(text("SELECT 1 FROM \"user\" WHERE userid = :userid"),
                      {"userid": camera.createdby}).first():
            return error_response(400, "Invalid user (createdBy)")
        
         # Check if camera already exists
        existing_camera = db.execute(
            text("SELECT isdeleted FROM camera WHERE cameraid = :cameraid"),
            {"cameraid": camera.cameraid}
        ).first()

        now = datetime.now()

        if existing_camera:
            if not existing_camera.isdeleted:  # isdeleted = False
                return error_response(400, "Camera ID already exists")

            # If isdeleted = true, restore the old record
            update_sql = text("""
                UPDATE camera SET
                    cameraname = :cameraname,
                    cameralocation = :cameralocation,
                    camerastatus = :camerastatus,
                    createdby = :createdby,
                    createddate = :createddate,
                    isdeleted = false
                WHERE cameraid = :cameraid
            """)
            db.execute(update_sql, {
                "cameraid": camera.cameraid,
                "cameraname": camera.cameraname,
                "cameralocation": camera.cameralocation,
                "camerastatus": bool(camera.camerastatus),
                "createdby": camera.createdby,
                "createddate": now,
                "updatedby": None  ,
                "updateddate": None  
            })
        else:
          # Insert new record
          insert_sql = text("""
              INSERT INTO camera (
                  cameraid, cameraname, cameralocation,
                  camerastatus, createdby, createddate, isdeleted
              ) VALUES (
                  :cameraid, :cameraname, :cameralocation,
                  :camerastatus, :createdby, :createddate, false
              )
          """)
          db.execute(insert_sql, {
              "cameraid": camera.cameraid,
              "cameraname": camera.cameraname,
              "cameralocation": camera.cameralocation,
              "camerastatus": camera.camerastatus,
              "createdby": camera.createdby,
              "createddate": now,
          })

        db.commit()
        return success_response(200, {"cameraid": camera.cameraid, "createddate": str(now)})
    
    @staticmethod
    def update_camera(cameraid: str, camera: schemas.CameraUpdate, db: Session):
        # Check if camera already exists
        if not db.execute(text("SELECT 1 FROM camera WHERE cameraid = :cameraid"), {"cameraid": cameraid}).first():
            return error_response(404, "Camera not found")

        update_fields = {}
        now = datetime.now()

        # Check updatedby (user id)
        if not db.execute(text("SELECT 1 FROM \"user\" WHERE userid = :userid"),{"userid": camera.updatedby}).first():
            return error_response(400, "Invalid user (updatedby)")
        
        update_fields["cameraid"] = camera.cameraid
        update_fields["updatedby"] = camera.updatedby
        update_fields["updateddate"] = now
        update_fields["update_cameraid"] = cameraid
        
        # Check cameraid duplicate (not self)
        if camera.cameraid != cameraid:
            duplicate_check = db.execute(
                text("SELECT isdeleted FROM camera WHERE cameraid = :new_cameraid"), 
                {"new_cameraid": camera.cameraid}).first()

            if duplicate_check:
                if not duplicate_check.isdeleted:
                    return error_response(400, "New camera ID already exists")
                else:
                    db.execute(
                        text("UPDATE camera SET isdeleted = true WHERE cameraid = :old_cameraid"),
                        {"old_cameraid": cameraid}
                    )
                    db.commit()
                    update_fields["update_cameraid"] = camera.cameraid
                    
        # field other
        if camera.cameraname is not None: update_fields["cameraname"] = camera.cameraname
        if camera.cameralocation is not None: update_fields["cameralocation"] = camera.cameralocation
        if camera.camerastatus is not None: update_fields["camerastatus"] = camera.camerastatus
        update_fields["isdeleted"] = False

        set_clause = ", ".join([f"{key} = :{key}" for key in update_fields if key != "update_cameraid"])
        update_sql = text(f"UPDATE camera SET {set_clause} WHERE cameraid = :update_cameraid")

        try:
          db.execute(update_sql, update_fields)
          db.commit()
          return success_response(200, { "cameraid": update_fields.get("cameraid", cameraid), "updateddate": str(now)})
        except Exception as e:
            db.rollback()
            return error_response(500, f"Database error: {str(e)}")
    
    @staticmethod
    def delete_camera(cameraid: str, db: Session):
        if not db.execute(text("SELECT 1 FROM camera WHERE cameraid = :cameraid"), {"cameraid": cameraid}).first():
            return error_response(404, "Camera not found")

        db.execute(text("UPDATE camera SET isdeleted = true WHERE cameraid = :cameraid"), {"cameraid": cameraid})
        db.commit()
        return success_response(200,{"cameraid": cameraid, "isdeleted": True})

    @staticmethod
    async def upload_cameras(uploadby: str, file: UploadFile, db: Session):
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
        camera_data = []
        for _, row in df.iterrows():
            camera_data.append({
                "cameraid": row.get("Camera ID"),
                "cameraname": row.get("Camera Name"),
                "cameralocation": row.get("Location"),
                "camerastatus": row.get("Status", "Active"),
                "createdby": uploadby,
                "createddate": now,
            })
 
        # SQL สำหรับ insert
        insert_sql = """
            INSERT INTO prodtype (
                cameraid, cameraname, cameralocation, camerastatus, createdby, createddate
            )
            VALUES (
                :cameraid, :cameraname, :cameralocation, :camerastatus, :createdby, :createddate
            )
        """
        # ทำ bulk insert
        db.execute(text(insert_sql), camera_data)
        db.commit()
        return success_response(200,{"message": f"{len(camera_data)} records uploaded successfully!"})
 
      except Exception as e:
          print(f"Error uploading camera: {e}")
          db.rollback()
          raise error_response(500, "Failed to upload camera")
      

