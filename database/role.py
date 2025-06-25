from database.connect_to_db import engine, Session, text, SQLAlchemyError
from fastapi import HTTPException
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

class RoleDB:
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
        
    def get_roles(self):
        return self._fetch_all("SELECT * FROM role WHERE isdeleted = false ORDER BY rolename")

    def suggest_role_name(self, q: str):
        rows = self._fetch_all("""
            SELECT DISTINCT rolename FROM role
            WHERE isdeleted = false AND rolestatus = true AND LOWER(rolename) LIKE LOWER(:keyword)
            ORDER BY rolename ASC
            LIMIT 10; """,
            {"keyword": q + "%"}
        )
        return [{"value": row["rolename"], "label": row["rolename"]} for row in rows]

    def add_role(self, role: schemas.RoleCreate, db: Session):
        # Check if user exists
        if not db.execute(text("SELECT 1 FROM \"user\" WHERE userid = :userid"),
                      {"userid": role.createdby}).first():
            return error_response(400, "Invalid user (createdBy)")
        
        # Check if role already exists
        existing_role = db.execute(
            text("SELECT roleid, isdeleted FROM role WHERE LOWER(rolename) = LOWER(:rolename)"),
            {"rolename": role.rolename}
        ).first()

        now = datetime.now()

        if existing_role:
            if not existing_role.isdeleted:  # isdeleted = False
                return error_response(400, "Role Name already exists")

            # If isdeleted = true, restore the old record
            update_sql = text("""
                UPDATE role SET
                    rolename = :rolename,
                    roledescription = :roledescription,
                    rolestatus = :rolestatus,
                    createdby = :createdby,
                    createddate = :createddate,
                    isdeleted = false
                WHERE roleid = :roleid
            """)
            db.execute(update_sql, {
                "roleid": existing_role.roleid,
                "rolename": role.rolename,
                "roledescription": role.roledescription,
                "rolestatus": bool(role.rolestatus),
                "createdby": role.createdby,
                "createddate": now,
                "updatedby": None  ,
                "updateddate": None  
            })
        else:
          # Insert new record
          insert_sql = text("""
              INSERT INTO role (
                  rolename, roledescription, rolestatus,
                  createdby, createddate, isdeleted
              ) VALUES (
                  :rolename, :roledescription, :rolestatus,
                  :createdby, :createddate, false
              )
          """)
          db.execute(insert_sql, {
              "rolename": role.rolename,
              "roledescription": role.roledescription,
              "rolestatus": role.rolestatus,
              "createdby": role.createdby,
              "createddate": role.createddate or datetime.now(),
          })

        db.commit()

        query = text("SELECT roleid FROM role ORDER BY roleid DESC LIMIT 1")
        row = db.execute(query).first()

        return success_response(200, {"roleid": row.roleid, "createddate": str(now)})

    def update_role(self, roleid: str, role: schemas.RoleUpdate, db: Session):
        # Check if role already exists
        existing_role = db.execute(text("SELECT rolename FROM role WHERE roleid = :roleid"), {"roleid": roleid}).first()
        if not existing_role:
            return error_response(404, "Role not found")
        
        update_fields = {}
        now = datetime.now()
        update_fields["roleid"] = roleid
        update_fields["updateddate"] = now
        update_fields["update_roleid"] = roleid

        # Check updatedby (user id)
        if not db.execute(text("SELECT 1 FROM \"user\" WHERE userid = :userid"),{"userid": role.updatedby}).first():
            return error_response(400, "Invalid user (updatedby)")
        update_fields["updatedby"] = role.updatedby

        # Check rolename duplicate (not self)
        old_rolename = existing_role.rolename

        if role.rolename != old_rolename:
            duplicate_check = db.execute(
                text("SELECT isdeleted FROM role WHERE rolename = :new_rolename"), 
                {"new_rolename": role.rolename}).first()

            if duplicate_check:
                if not duplicate_check.isdeleted:
                    return error_response(400, "New role name already exists")

        if not update_fields:
          return error_response(400, "No fields to update")
        
        set_clause = ", ".join([f"{key} = :{key}" for key in update_fields if key != "update_roleid"])
        update_sql = text(f"UPDATE role SET {set_clause} WHERE roleid = :update_roleid")

        try:
          db.execute(update_sql, update_fields)
          db.commit()
          return success_response(200, { "roleid": update_fields.get("roleid", roleid), "updateddate": str(now)})
        except Exception as e:
            db.rollback()
            return error_response(500, f"Database error: {str(e)}")
    
    @staticmethod
    def delete_role(roleid: str, db: Session):
        if not db.execute(text("SELECT 1 FROM role WHERE roleid = :roleid"), {"roleid": roleid}).first():
            return error_response(404, "Role not found")

        update_sql = text("UPDATE role SET isdeleted = true WHERE roleid = :roleid")
        db.execute(update_sql, {"roleid": roleid})
        db.commit()
        return success_response(200,{"roleid": roleid, "isdeleted": True})

    @staticmethod
    async def upload_roles(uploadby: str, file: UploadFile, db: Session):
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
                "rolename": row.get("Role Name"),
                "roledescription": row.get("Description"),
                "rolestatus": row.get("Status", "Active"),
                "createdby": uploadby,
                "createddate": now,
            })

        # SQL สำหรับ insert
        insert_sql = """
            INSERT INTO role (
                rolename, roledescription, rolestatus, createdby, createddate
            )
            VALUES (
                :rolename, :roledescription, :rolestatus, :createdby, :createddate
            )
        """
        # ทำ bulk insert
        db.execute(text(insert_sql), role_data)
        db.commit()
        return success_response(200,{"message": f"{len(role_data)} records uploaded successfully!"})
 
      except Exception as e:
          print(f"Error uploading role: {e}")
          db.rollback()
          raise error_response(500, "Failed to upload role")
      
