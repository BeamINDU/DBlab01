from database.connect_to_db import engine, Session, text, SQLAlchemyError
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

class UserDB:
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

    def get_users(self):
        return self._fetch_all("""
          SELECT  u.*,
            COALESCE(array_remove(array_agg(ur.roleid), NULL), '{}') AS roles,
            COALESCE(string_agg(DISTINCT r.rolename, ','), '') AS rolenames
          FROM \"user\" u
          LEFT JOIN userrole ur ON u.userid = ur.userid
          LEFT JOIN role r ON ur.roleid = r.roleid
          WHERE u.isdeleted = false 
          GROUP BY u.userid
        """)

    def suggest_userid(self, q: str):
        rows = self._fetch_all("""
            SELECT DISTINCT userid FROM \"user\"
            WHERE isdeleted = false AND userstatus = true AND LOWER(userid) LIKE LOWER(:keyword)
            ORDER BY userid ASC
            LIMIT 10; """,
            {"keyword": q + "%"}
        )
        return [{"value": row["userid"], "label": row["userid"]} for row in rows]
    
    def suggest_username(self, q: str):
        rows = self._fetch_all("""
            SELECT DISTINCT username FROM \"user\"
            WHERE isdeleted = false AND userstatus = true AND LOWER(username) LIKE LOWER(:keyword)
            ORDER BY username ASC
            LIMIT 10; """,
            {"keyword": q + "%"}
        )
        return [{"value": row["username"], "label": row["username"]} for row in rows]
    


class UserService:
    @staticmethod
    def add_user(user: schemas.UserCreate, db: Session):
        # Check if user exists
        if not db.execute(text("SELECT 1 FROM \"user\" WHERE userid = :userid"),
                      {"userid": user.createdby}).first():
            return error_response(400, "Invalid user (createdBy)")
        
        # Check if user already exists
        existing_user = db.execute(
            text('SELECT isdeleted FROM "user" WHERE userid = :userid'),
            {"userid": user.userid}
        ).first()

        now = datetime.now()

        if existing_user:
            if not existing_user.isdeleted:  # isdeleted = False
                return error_response(400, "User ID already exists")

            # If isdeleted = true, restore the old record
            update_sql = text("""
                UPDATE "user" SET
                    ufname = :ufname,
                    ulname = :ulname,
                    username = :username,
                    upassword = :upassword,
                    email = :email,
                    userstatus = :userstatus,
                    createdby = :createdby,
                    createddate = :createddate,
                    isdeleted = false
                WHERE userid = :userid
            """)
            db.execute(update_sql, {
                "userid": user.userid,
                "ufname": user.ufname,
                "ulname": user.ulname,
                "username": user.username,
                "upassword": user.upassword,
                "email": user.email,
                "userstatus": bool(user.userstatus),
                "createdby": user.createdby,
                "createddate": now,
                "updatedby": None  ,
                "updateddate": None  
            })
        else:
          # Insert new record
          insert_sql = text("""
              INSERT INTO "user" (
                  userid, ufname, ulname, username, upassword, email,
                  userstatus, createdby, createddate, isdeleted
              ) VALUES (
                  :userid, :ufname, :ulname, :username, :upassword, :email,
                  :userstatus, :createdby, :createddate, false
              )
          """)
          db.execute(insert_sql, {
            "userid": user.userid,
            "ufname": user.ufname or "",
            "ulname": user.ulname or "",
            "username": user.username,
            "upassword": user.upassword or "",
            "email": user.email,
            "userstatus": bool(user.userstatus),
            "createdby": user.createdby,
            "createddate": now
        })

        # Update userrole
        if user.roles:
            new_roles = set(user.roles or [])

            existing_rows = db.execute(text("""
                SELECT roleid FROM userrole WHERE userid = :userid
            """), {"userid": user.userid}).fetchall()
            existing_roles = set(row[0] for row in existing_rows)

            to_insert = new_roles - existing_roles
            to_delete = existing_roles - new_roles
        
            for roleid in to_insert:
                db.execute(text("""
                    INSERT INTO userrole (userid, roleid)
                    VALUES (:userid, :roleid)
                """), {"userid": user.userid, "roleid": roleid})

            for roleid in to_delete:
                db.execute(text("""
                    DELETE FROM userrole
                    WHERE userid = :userid AND roleid = :roleid
                """), {"userid": user.userid, "roleid": roleid})

            new_roles = db.execute(text("""
                SELECT COALESCE(string_agg(DISTINCT r.rolename, ','), '') AS rolenames
                FROM "user" u
                LEFT JOIN userrole ur ON u.userid = ur.userid
                LEFT JOIN role r ON ur.roleid = r.roleid
                WHERE u.isdeleted = false AND u.userid = :userid
            """), {"userid": user.userid}).fetchone()
            rolenames = new_roles.rolenames if new_roles else ''

        db.commit()
        return success_response(200, {"userid": user.userid, "rolenames": rolenames, "createddate": str(now)})

    @staticmethod
    def edit_user(userid: str, user: schemas.UserUpdate, db: Session):
        # Check if user already exists
        if not db.execute(text('SELECT 1 FROM \"user\" WHERE userid = :userid'), {"userid": userid}).first():
            return error_response(404, "User not found")
        
        update_fields = {}
        now = datetime.now()
        update_fields["userid"] = user.userid
        update_fields["updateddate"] = now
        update_fields["update_userid"] = userid

        # Check updatedby (user id)
        if not db.execute(text("SELECT 1 FROM \"user\" WHERE userid = :userid"),
                          {"userid": user.updatedby}).first():
            return error_response(400, "Invalid user (updatedby)")
        update_fields["updatedby"] = user.updatedby

        # Check username duplicate
        duplicate_user = db.execute(text("""
            SELECT 1 FROM "user"
            WHERE username = :username
              AND userid != :userid
        """), {
            "username": user.username,
            "userid": userid
        }).first()

        if duplicate_user:
            return error_response(400, f"Username '{user.username}' already exists")

        # Check userid duplicate
        if user.userid != userid:
            duplicate_check = db.execute(
                text("SELECT isdeleted FROM \"user\" WHERE userid = :new_userid"), 
                {"new_userid": user.userid}).first()

            if duplicate_check:
                if not duplicate_check.isdeleted:
                     return error_response(400, f"User ID '{user.userid}' already exists")
            
        # field other
        if user.ufname is not None: update_fields["ufname"] = user.ufname
        if user.ulname is not None: update_fields["ulname"] = user.ulname
        if user.username is not None: update_fields["username"] = user.username
        if user.upassword is not None: update_fields["upassword"] = user.upassword
        if user.email is not None: update_fields["email"] = user.email
        if user.userstatus is not None: update_fields["userstatus"] = user.userstatus
        update_fields["isdeleted"] = False

        if not update_fields:
          return error_response(400, "No fields to update")

        set_clause = ", ".join([f"{key} = :{key}" for key in update_fields if key != "update_userid"])
        update_sql = text(f'UPDATE "user" SET {set_clause} WHERE userid = :update_userid')

        # Update userrole
        if user.roles:
            new_roles = set(user.roles or [])

            existing_rows = db.execute(text("""
                SELECT roleid FROM userrole WHERE userid = :userid
            """), {"userid": user.userid}).fetchall()
            existing_roles = set(row[0] for row in existing_rows)

            to_insert = new_roles - existing_roles
            to_delete = existing_roles - new_roles
        
            for roleid in to_insert:
                db.execute(text("""
                    INSERT INTO userrole (userid, roleid)
                    VALUES (:userid, :roleid)
                """), {"userid": user.userid, "roleid": roleid})

            for roleid in to_delete:
                db.execute(text("""
                    DELETE FROM userrole
                    WHERE userid = :userid AND roleid = :roleid
                """), {"userid": user.userid, "roleid": roleid})

            new_roles = db.execute(text("""
                SELECT COALESCE(string_agg(DISTINCT r.rolename, ','), '') AS rolenames
                FROM "user" u
                LEFT JOIN userrole ur ON u.userid = ur.userid
                LEFT JOIN role r ON ur.roleid = r.roleid
                WHERE u.isdeleted = false AND u.userid = :userid
            """), {"userid": user.userid}).fetchone()
            rolenames = new_roles.rolenames if new_roles else ''

        try:
          db.execute(update_sql, update_fields)
          db.commit()
          return success_response(200, { "userid": update_fields.get("userid", userid), "rolenames": rolenames, "updateddate": str(now)})
        except Exception as e:
            db.rollback()
            return error_response(500, f"Database error: {str(e)}")
    
    @staticmethod
    def delete_user(userid: str, db: Session):
        if not db.execute(text('SELECT 1 FROM public."user" WHERE userid = :userid'), {"userid": userid}).first():
            return error_response(404, "User not found")

        update_sql = text('UPDATE public."user" SET isdeleted = true WHERE userid = :userid')
        db.execute(update_sql, {"userid": userid})
        db.commit()
        return success_response(200,{ "userid": userid, "isdeleted": True})

    @staticmethod
    async def upload_users(uploadby: str, file: UploadFile, db: Session):
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
        user_data = []
        for _, row in df.iterrows():
            user_data.append({
                "userid": row.get("User ID"),
                "ufname": row.get("First Name"),
                "ulname": row.get("Last Name"),
                "username": row.get("Username"),
                "upassword": '',
                "email": row.get("Email"),
                "userstatus": row.get("Status", "Active"),
                "createdby": uploadby,
                "createddate": now,
                # "roles": row.get("Role Name"),
            })

       
        # SQL สำหรับ insert
        insert_sql = """
            INSERT INTO \"user\" (
                userid, ufname, ulname, username, upassword, email, userstatus, createdby, createddate
            )
            VALUES (
                :userid, :ufname, :ulname, :username, :upassword, :email, :userstatus, :createdby, :createddate
            )
        """
        # ทำ bulk insert
        db.execute(text(insert_sql), user_data)
        db.commit()
        return success_response(200,{"message": f"{len(user_data)} records uploaded successfully!"})
 
      except Exception as e:
          print(f"Error uploading user: {e}")
          db.rollback()
          raise error_response(500, "Failed to upload user")
