from database.connect_to_db import engine, Session, text, SQLAlchemyError
from fastapi import HTTPException
import database.schemas as schemas
from datetime import datetime
from fastapi.responses import JSONResponse
from typing import Union, Dict, Any

def error_response(code: int, message: str):
    return JSONResponse( status_code=code, content={"detail": {"error": message}} )

def success_response(code: int, content: Union[Dict[str, Any], str]):
    return JSONResponse( status_code=code, content=content)

class UserDB:
    def _fetch_all(self, query: str):
        try:
            with engine.connect() as conn:
                result = conn.execute(text(query))
                return [dict(row) for row in result.mappings()]
        except SQLAlchemyError as e:
            print(f"Database error: {e}")
            return []

    def get_users(self):
        return self._fetch_all("SELECT * FROM public.\"user\" WHERE isdeleted = false")


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

        # Lookup roleid from roleName
        # roleid = None
        # if user.roleid is None and user.roleName:
        #     role_sql = text("SELECT roleid FROM role WHERE rolename = :rolename")
        #     role_row = db.execute(role_sql, {"rolename": user.roleName}).first()
        #     if role_row:
        #         roleid = role_row.roleid
        #     else:
        #         raise HTTPException(status_code=400, detail="Invalid roleName")
        # else:
        #     roleid = user.roleid

        db.commit()
        return success_response(200, {"userid": user.userid, "createddate": str(now)})

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
                # else:
                #     db.execute(
                #         text("UPDATE \"user\" SET isdeleted = true WHERE userid = :old_userid"),
                #         {"old_userid": userid}
                #     )
                #     db.commit()
                #     update_fields["update_userid"] = user.userid
            
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

        try:
          db.execute(update_sql, update_fields)
          db.commit()
          return success_response(200, { "userid": update_fields.get("userid", userid), "updateddate": str(now)})
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



