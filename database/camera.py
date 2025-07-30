from database.connect_to_db import engine, SessionLocal, Session, text, SQLAlchemyError
from datetime import datetime, date
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
        
    def get_cameras(self, model: schemas.CameraSearch):
        filters = []
        params = {}
        
        if model.cameraid:
            filters.append("cameraid ILIKE :cameraid")
            params["cameraid"] = f"%{model.cameraid}%"

        if model.cameraname:
            filters.append("cameraname ILIKE :cameraname")
            params["cameraname"] = f"%{model.cameraname}%"

        if model.cameraip:
            filters.append("cameraip ILIKE :cameraip")
            params["cameraip"] = f"%{model.cameraip}%"

        if model.cameralocation:
            filters.append("cameralocation ILIKE :cameralocation")
            params["cameralocation"] = f"%{model.cameralocation}%"

        if model.camerastatus is not None:
            filters.append("camerastatus = :camerastatus")
            params["camerastatus"] = model.camerastatus

        where_clause = " AND " + " AND ".join(filters) if filters else ""

        query = f"""
            SELECT * FROM camera
            WHERE isdeleted = false {where_clause}
            ORDER BY cameraname
        """

        return self._fetch_all(query, params)

    def camera_options(self, q: str):
        rows = self._fetch_all("""
            SELECT DISTINCT cameraid, cameraname FROM camera
            WHERE isdeleted = false AND camerastatus = true AND LOWER(cameraid) LIKE LOWER(:keyword)
            ORDER BY cameraid ASC
            LIMIT 10; """,
            {"keyword": q + "%"}
        )
        return [{"value": row["cameraid"], "label": row["cameraname"]} for row in rows]
    
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

    def suggest_camera_ip(self, q: str):
        rows = self._fetch_all("""
            SELECT DISTINCT cameraip FROM camera
            WHERE isdeleted = false AND camerastatus = true AND LOWER(cameraip) LIKE LOWER(:keyword)
            ORDER BY cameraip ASC
            LIMIT 10; """,
            {"keyword": q + "%"}
        )
        return [{"value": row["cameraip"], "label": row["cameraip"]} for row in rows]

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
                    cameraip = :cameraip,
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
                "cameraip": camera.cameraip,
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
                  cameraid, cameraname, cameralocation, cameraip,
                  camerastatus, createdby, createddate, isdeleted
              ) VALUES (
                  :cameraid, :cameraname, :cameralocation, :cameraip,
                  :camerastatus, :createdby, :createddate, false
              )
          """)
          db.execute(insert_sql, {
              "cameraid": camera.cameraid,
              "cameraname": camera.cameraname,
              "cameralocation": camera.cameralocation,
              "cameraip": camera.cameraip,
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
        if camera.cameraip is not None: update_fields["cameraip"] = camera.cameraip
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
            filename = file.filename.lower()
            file.file.seek(0)
            if filename.endswith(".xlsx") or filename.endswith(".xls"):
                df = pd.read_excel(file.file, engine="openpyxl")
            elif filename.endswith(".csv"):
                df = pd.read_csv(file.file)
            else:
                raise error_response(400, detail="File must be .xlsx or .csv")

            if df.empty:
                raise error_response(400, detail="File is empty")

            schema_query = text("""
                SELECT column_name, udt_name
                FROM information_schema.columns
                WHERE table_name = 'camera' AND table_schema = 'public'
            """)
            schema_result = db.execute(schema_query).mappings().fetchall()
            column_types = {row['column_name']: row['udt_name'] for row in schema_result}

            postgres_to_python = {
                'int4': int,
                'varchar': str,
                'text': str,
                'bool': bool,
                'date': date,
                'timestamp': datetime
            }

            all_data = df.to_dict(orient="records")
            for i, row in enumerate(all_data, start=1):
                print(f"ROW {i}: {row}")
                camId = row.get('camera ID')
                camName = row.get('camera name')
                camLocation = row.get('camera location')
                camip = row.get('camera ip')
                status = True if row.get('Status') == "Active" else False
                status = str(status).strip().lower() in ["active", "true", "1"]
                insert_data = {
                    "cameraid": camId,
                    "cameraname": camName,
                    "cameralocation": camLocation,
                    "camerastatus": status,
                    "cameraip": camip
                }
                for field, value in insert_data.items():
                    expected_udt = column_types.get(field)
                    expected_type = postgres_to_python.get(expected_udt)
                    if expected_type:
                        try:
                            if expected_type == bool:
                                if isinstance(value, str):
                                    value = value.strip().lower() in ["true", "active", "1"]
                                else:
                                    value = bool(value)
                            else:
                                value = expected_type(value)
                            insert_data[field] = value 
                        except (ValueError, TypeError):
                            raise error_response(
                                400,
                                detail=f"Row {i}: Field '{field}' must be of type {expected_type.__name__}, "
                                    f"got '{value}' ({type(value).__name__})"
                            )
                sql_check = text("SELECT 1 FROM camera WHERE  cameraid = :cameraid")
                data_check = db.execute(sql_check, {"cameraid": insert_data["cameraid"]}).first()
                if not data_check:
                    sql_insert = text("""
                        INSERT INTO camera ( cameraid , cameraname, cameralocation, camerastatus , cameraip)
                        VALUES ( :cameraid, :cameraname, :cameralocation, :camerastatus, :cameraip )
                """)
                    db.execute(sql_insert, insert_data)
                elif data_check.isdeleted:
                    sql_update = text("""
                        UPDATE camera SET 
                            cameraname = :cameraname, 
                            cameralocation = :cameralocation, 
                            camerastatus = :camerastatus,
                            cameraip = :cameraip
                        WHERE cameraid = :cameraid
                """)
                    db.execute(sql_update, insert_data)
                else : 
                    return error_response(400, "Camera ID already exists")

                db.commit()
            return success_response(200, {"message": "camera uploaded successfully"})

        except Exception as e:
            import traceback
            traceback.print_exc()
            raise error_response(500, detail=str(e))

