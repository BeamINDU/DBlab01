from database.connect_to_db import engine, Session, text, SQLAlchemyError
from fastapi import UploadFile
import database.schemas as schemas
from datetime import datetime, date
from fastapi.responses import JSONResponse
from typing import Union, Dict, Any
from fastapi import UploadFile
import pandas as pd

def error_response(code: int, message: str):
    return JSONResponse( status_code=code, content={"detail": {"error": message}} )

def success_response(code: int, content: Union[Dict[str, Any], str]):
    return JSONResponse( status_code=code, content=content)

class UserDB:
    def _fetch_one(self, query: str, params: dict):
        try:
            with engine.connect() as conn:
                result = conn.execute(text(query), params)
                return result.mappings().first()
        except SQLAlchemyError as e:
            print(f"Database error: {e}")
            return None

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

    def get_users(self, model: schemas.UserSearch):
        filters = []
        params = {}

        if model.userid:
            filters.append("u.userid ILIKE :userid")
            params["userid"] = f"%{model.userid}%"

        if model.username:
            filters.append("u.username ILIKE :username")
            params["username"] = f"%{model.username}%"

        if model.fullname:
            filters.append("u.ufname || ' ' || u.ulname ILIKE :fullname")
            params["fullname"] = f"%{model.fullname}%"

        if model.rolename:
            filters.append("r.rolename ILIKE :rolename")
            params["rolename"] = f"%{model.rolename}%"

        if model.userstatus is not None:
            filters.append("u.userstatus = :userstatus")
            params["userstatus"] = model.userstatus

        where_clause = " AND " + " AND ".join(filters) if filters else ""

        query = f"""
            SELECT  u.*,
              COALESCE(array_remove(array_agg(ur.roleid), NULL), '{{}}') AS roles,
              COALESCE(string_agg(DISTINCT r.rolename, ','), '') AS rolenames
            FROM "user" u
            LEFT JOIN userrole ur ON u.userid = ur.userid
            LEFT JOIN role r ON ur.roleid = r.roleid
            WHERE u.isdeleted = false {where_clause}
            GROUP BY u.userid
        """
      
        return self._fetch_all(query, params)

    def get_detail(self, userid: str):
        query = """
            SELECT  
                u.*,
                COALESCE(array_remove(array_agg(DISTINCT ur.roleid), NULL), '{}') AS roles,
                COALESCE(string_agg(DISTINCT r.rolename, ','), '') AS rolenames
            FROM "user" u
            LEFT JOIN userrole ur ON u.userid = ur.userid
            LEFT JOIN role r ON ur.roleid = r.roleid
            WHERE u.userid = :userid
            GROUP BY u.userid
        """
        return self._fetch_one(query, {"userid": userid})

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
    
    def suggest_fullname(self, q: str):
      rows = self._fetch_all("""
          SELECT DISTINCT u.ufname || ' ' || u.ulname AS fullname
          FROM "user" u
          WHERE isdeleted = false 
          AND userstatus = true
          AND LOWER(u.ufname) || ' ' || LOWER(u.ulname) ILIKE :fullname
          ORDER BY fullname ASC
          LIMIT 10;
          """,
          {"fullname": q.lower() + "%"}
      )
      return [{"value": row["fullname"], "label": row["fullname"]} for row in rows]

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
        if user.upassword is None: user.upassword = user.username

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
        rolenames = None
        if user.roles is not None:
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
            return success_response(200, {
                "userid": update_fields.get("userid", userid),
                "rolenames": rolenames,
                "updateddate": str(now)
            })
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
                WHERE table_name = 'user' AND table_schema = 'public'
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
                userid = row.get('userid')
                fristname = row.get('fristname')
                lastname = row.get('lastname')
                username = row.get('username')
                password = row.get('password')
                email = row.get('email')
                status = True if row.get('Status') == "Active" else False
                status = str(status).strip().lower() in ["active", "true", "1"]
                insert_data = {
                    "userid": userid,
                    "ufname": fristname,
                    "ulname": lastname,
                    "username": username,
                    "upassword": password,
                    "email": email,
                    "userstatus": status
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
                sql_check = text("SELECT 1 FROM \"user\" WHERE  userid = :userid")
                if not db.execute(sql_check, {"userid": insert_data["userid"]}).first():
                    sql_insert = text("""
                    INSERT INTO \"user\" ( userid , ufname, ulname, username, upassword, email, userstatus )
                    VALUES ( :userid, :ufname, :ulname, :username, :upassword, :email, :userstatus )
                """)
                    db.execute(sql_insert, insert_data)
                else:
                    sql_update = text("""
                    UPDATE \"user\" SET 
                        ufname = :ufname, 
                        ulname = :ulname, 
                        username = :username,
                        upassword = :upassword,
                        email = :email,
                        userstatus = :userstatus
                    WHERE userid = :userid
                """)
                db.execute(sql_update, insert_data)
            db.commit()
            return success_response(200, {"message": "user uploaded successfully"})

        except Exception as e:
            import traceback
            traceback.print_exc()
            raise error_response(500, detail=str(e))