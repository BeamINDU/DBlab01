from database.connect_to_db import engine, SessionLocal, Session, text, SQLAlchemyError
from datetime import datetime
import database.schemas as schemas
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from typing import Union, Dict, Any

def error_response(code: int, message: str):
    return JSONResponse( status_code=code, content={"detail": {"error": message}} )

def success_response(code: int, content: Union[Dict[str, Any], str]):
    return JSONResponse( status_code=code, content=content)

class CameraDB:
    def _fetch_all(self, query: str):
        try:
            with engine.connect() as conn:
                result = conn.execute(text(query))
                return [dict(row) for row in result.mappings()]

        except SQLAlchemyError as e:
            print(f"Database error: {e}")
            return []

    def get_cameras(self):
        return self._fetch_all("SELECT * FROM camera WHERE isdeleted = false")


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
        if camera.updatedby:
            if not db.execute(text("SELECT 1 FROM \"user\" WHERE userid = :userid"),
                              {"userid": camera.updatedby}).first():
                return error_response(400, "Invalid user (updatedby)")
            update_fields["updatedby"] = camera.updatedby

        # Check cameraid duplicate (not self)
        if camera.cameraid and camera.cameraid != cameraid:
            duplicate_check = db.execute(text("""
                SELECT isdeleted FROM camera WHERE cameraid = :new_cameraid
            """), {"new_cameraid": camera.cameraid}).first()

            if duplicate_check:
                if not duplicate_check.isdeleted:
                    return error_response(400, "New camera ID already exists")
                else:
                    # duplicate → delete record where isdeleted = true
                    db.execute(
                        text("UPDATE camera SET isdeleted = true WHERE cameraid = :new_cameraid"),
                        {"new_cameraid": camera.cameraid}
                    )
                    db.commit()

            update_fields["cameraid"] = camera.cameraid

        # field other
        if camera.cameraname is not None:
            update_fields["cameraname"] = camera.cameraname
        if camera.cameralocation is not None:
            update_fields["cameralocation"] = camera.cameralocation
        if camera.camerastatus is not None:
            update_fields["camerastatus"] = camera.camerastatus
        if camera.updatedby:
            update_fields["updatedby"] = camera.updatedby

        update_fields["updateddate"] = camera.updateddate or datetime.now()

        if not update_fields:
          return error_response(400, "No fields to update")

        update_fields["old_cameraid"] = cameraid
        set_clause = ", ".join([f"{key} = :{key}" for key in update_fields if key != "old_cameraid"])

        update_sql = text(f"UPDATE camera SET {set_clause} WHERE cameraid = :old_cameraid")
        db.execute(update_sql, update_fields)
        db.commit()

        return success_response(200, { "cameraid": update_fields.get("cameraid", cameraid), "updateddate": str(now)})
    
    @staticmethod
    def delete_camera(cameraid: str, db: Session):
        if not db.execute(text("SELECT 1 FROM camera WHERE cameraid = :cameraid"), {"cameraid": cameraid}).first():
            return error_response(404, "Camera not found")

        db.execute(text("UPDATE camera SET isdeleted = true WHERE cameraid = :cameraid"), {"cameraid": cameraid})
        db.commit()
        return success_response(200,{"cameraid": cameraid, "isdeleted": True})



