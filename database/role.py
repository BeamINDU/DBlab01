from database.connect_to_db import engine, Session, text, SQLAlchemyError
from fastapi import HTTPException
from datetime import datetime
import database.schemas as schemas
from fastapi.responses import JSONResponse
from typing import Union, Dict, Any

def error_response(code: int, message: str):
    return JSONResponse( status_code=code, content={"detail": {"error": message}} )

def success_response(code: int, content: Union[Dict[str, Any], str]):
    return JSONResponse( status_code=code, content=content)

class RoleDB:
    def get_roles(self):
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT * FROM role WHERE isdeleted = false"))
                return [dict(row) for row in result.mappings()]
        except SQLAlchemyError as e:
            print(f"Database error: {e}")
            return []
        
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

