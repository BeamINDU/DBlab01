from database.connect_to_db import engine, Session, text, SQLAlchemyError
from fastapi import HTTPException,UploadFile
import database.schemas as schemas
from datetime import datetime, date
from fastapi.responses import JSONResponse
from typing import Union, Dict, Any
import pandas as pd
def error_response(code: int, message: str):
    return JSONResponse(status_code=code, content={"detail": {"error": message}})

def success_response(code: int, content: Union[Dict[str, Any], str]):
    return JSONResponse(status_code=code, content=content)

class RoleDB:
    def get_roles(self, model: schemas.RoleSearch):
        filters = []
        params = {}
        
        if model.rolename:
            filters.append("rolename ILIKE :rolename")
            params["rolename"] = f"%{model.rolename}%"

        if model.rolestatus is not None:
            filters.append("rolestatus = :rolestatus")
            params["rolestatus"] = model.rolestatus

        where_clause = " AND " + " AND ".join(filters) if filters else ""

        query = f"""
            SELECT * FROM role
            WHERE isdeleted = false {where_clause}
            ORDER BY rolename
        """

        # print("SQL Query:", query)
        # print("Parameters:", params)

        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            return list(result.mappings())
        
    def suggest_role_name(self, q: str):
        try:
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT rolename 
                    FROM role 
                    WHERE isdeleted = false AND rolestatus = true AND rolename ILIKE :q
                    ORDER BY rolename 
                    LIMIT 10
                """), {"q": f"%{q}%"})
                return [{"label": row.rolename, "value": row.rolename} for row in result]
        except SQLAlchemyError as e:
            print(f"Database error: {e}")
            return []

    def add_role(self, role: schemas.RoleCreate, db: Session):
        # Check if role already exists
        if db.execute(text("SELECT 1 FROM role WHERE rolename = :rolename"), 
                      {"rolename": role.rolename}).first():
            return error_response(400, "Role name already exists")

        # Check createdby (user id)
        if not db.execute(text("SELECT 1 FROM \"user\" WHERE userid = :userid"), 
                          {"userid": role.createdby}).first():
            return error_response(400, "Invalid user (createdby)")

        now = datetime.now()
        query = text("""
            INSERT INTO role (rolename, createdby, createddate, isdeleted)
            VALUES (:rolename, :createdby, :createddate, :isdeleted)
            RETURNING roleid
        """)

        try:
            result = db.execute(query, {
                "rolename": role.rolename,
                "createdby": role.createdby,
                "createddate": now,
                "isdeleted": False
            })
            db.commit()
        except Exception as e:
            db.rollback()
            return error_response(500, f"Database error: {str(e)}")

        query = text("SELECT roleid FROM role WHERE rolename = :rolename LIMIT 1")
        row = db.execute(query, {"rolename": role.rolename}).first()

        return success_response(200, {"roleid": row.roleid, "createddate": str(now)})

    def update_role(self, roleid: str, role: schemas.RoleUpdate, db: Session):
        existing_role = db.execute(text("SELECT rolename FROM role WHERE roleid = :roleid"), 
                                   {"roleid": roleid}).first()
        if not existing_role:
            return error_response(404, "Role not found")
        
        update_fields = {}
        now = datetime.now()
        update_fields["roleid"] = roleid
        update_fields["updateddate"] = now
        update_fields["update_roleid"] = roleid

        # Check updatedby (user id)
        if not db.execute(text("SELECT 1 FROM \"user\" WHERE userid = :userid"), 
                          {"userid": role.updatedby}).first():
            return error_response(400, "Invalid user (updatedby)")
        update_fields["updatedby"] = role.updatedby

        old_rolename = existing_role.rolename

        if hasattr(role, 'rolename') and role.rolename and role.rolename != old_rolename:
            duplicate_check = db.execute(
                text("SELECT isdeleted FROM role WHERE rolename = :new_rolename"), 
                {"new_rolename": role.rolename}).first()

            if duplicate_check:
                if not duplicate_check.isdeleted:
                    return error_response(400, "New role name already exists")
        
        if hasattr(role, 'rolename') and role.rolename:
            update_fields["rolename"] = role.rolename
            
        if hasattr(role, 'roledescription') and role.roledescription is not None:
            update_fields["roledescription"] = role.roledescription
            
        if hasattr(role, 'rolestatus') and role.rolestatus is not None:
            update_fields["rolestatus"] = role.rolestatus

        if len(update_fields) <= 3: 
            return error_response(400, "No fields to update")
        
        set_clause = ", ".join([f"{key} = :{key}" for key in update_fields if key != "update_roleid"])
        update_sql = text(f"UPDATE role SET {set_clause} WHERE roleid = :update_roleid")

        try:
            db.execute(update_sql, update_fields)
            db.commit()
            return success_response(200, {"roleid": update_fields.get("roleid", roleid), "updateddate": str(now)})
        except Exception as e:
            db.rollback()
            return error_response(500, f"Database error: {str(e)}")

    def delete_role(self, roleid: str, db: Session):
        if not db.execute(text("SELECT 1 FROM role WHERE roleid = :roleid"), 
                          {"roleid": roleid}).first():
            return error_response(404, "Role not found")

        update_sql = text("UPDATE role SET isdeleted = true WHERE roleid = :roleid")
        
        try:
            db.execute(update_sql, {"roleid": roleid})
            db.commit()
            return success_response(200, {"roleid": roleid})
        except Exception as e:
            db.rollback()
            return error_response(500, f"Database error: {str(e)}")

    def get_role_permissions(self, roleid: int, db: Session):
        try:
            query = text("""
                SELECT DISTINCT
                    rp.roleid,
                    rp.menuid,
                    rp.actionid,
                    m.menuname,
                    m.parentid,
                    m.seq,
                    m.path,
                    m.icon
                FROM rolepermission rp
                JOIN menu m ON rp.menuid = m.menuid
                WHERE rp.roleid = :roleid
                ORDER BY m.seq
            """)
            
            result = db.execute(query, {"roleid": roleid})
            permissions = result.fetchall()
            
            permission_list = []
            for row in permissions:
                actionid_str = str(row.actionid)
                if ',' in actionid_str:
                    actions = [int(x.strip()) for x in actionid_str.split(',') if x.strip().isdigit()]
                else:
                    actions = [int(actionid_str)] if actionid_str.isdigit() else [1]
                
                permission_list.append({
                    "menuid": row.menuid,
                    "menuname": row.menuname,
                    "parentid": row.parentid or "",
                    "seq": row.seq,
                    "path": row.path or "",
                    "icon": row.icon or "",
                    "actionid": actions[0] if actions else 1,
                    "actions": actions 
                })
            
            return {"permissions": permission_list}
            
        except Exception as e:
            print(f"Error in get_role_permissions: {str(e)}")
            return {"permissions": []}

    def update_role_permissions(self, roleid: int, permissions_data: Dict[str, Any], db: Session):
        try:
            print(f"Starting update_role_permissions for roleId: {roleid}")
            print(f" Permissions data: {permissions_data}")
            
            if not db.execute(text("SELECT 1 FROM role WHERE roleid = :roleid"), 
                             {"roleid": roleid}).first():
                raise HTTPException(status_code=404, detail="Role not found")

            menu_query = text("""
                SELECT menuid, parentid, menuname 
                FROM menu 
                ORDER BY seq
            """)
            menus = db.execute(menu_query).fetchall()
            
            menu_parents = {}
            menu_children = {}
            for menu in menus:
                if menu.parentid: 
                    menu_parents[menu.menuid] = menu.parentid
                    if menu.parentid not in menu_children:
                        menu_children[menu.parentid] = []
                    menu_children[menu.parentid].append(menu.menuid)

            delete_sql = text("DELETE FROM rolepermission WHERE roleid = :roleid")
            db.execute(delete_sql, {"roleid": roleid})
            print(f"🗑️ Deleted existing permissions for roleId: {roleid}")

            # เตรียม permissions ใหม่
            permissions = permissions_data.get('permissions', [])
            menus_to_add = {} 
            
            for perm in permissions:
                menuid = perm.get('menuId')
                actions = perm.get('actions', [1])
                if menuid and actions:
                    menus_to_add[menuid] = actions

            parents_to_add = set()
            for menuid in menus_to_add.keys():
                if menuid in menu_parents: 
                    parent_id = menu_parents[menuid]
                    if parent_id not in menus_to_add:
                        parents_to_add.add(parent_id)
                        print(f"Auto-adding parent menu: {parent_id} for child: {menuid}")


            for parent_id in parents_to_add:
                menus_to_add[parent_id] = [1] 

            # Insert permissions
            if menus_to_add:
                for menuid, actions in menus_to_add.items():
                    actions_str = ','.join(map(str, actions))
                    
                    insert_sql = text("""
                        INSERT INTO rolepermission (roleid, menuid, actionid)
                        VALUES (:roleid, :menuid, :actionid)
                    """)
                    
                    db.execute(insert_sql, {
                        "roleid": roleid,
                        "menuid": menuid,
                        "actionid": actions_str
                    })
                    
                    action_type = "auto-added parent" if menuid in parents_to_add else "selected"
                    print(f"Inserted permission ({action_type}): roleId={roleid}, menuId={menuid}, actions={actions_str}")

            db.commit()
            print(f"💾 Successfully updated role permissions for roleId: {roleid}")
            
            return {
                "status": "success",
                "message": "Role permissions updated successfully",
                "roleId": roleid,
                "permissionsCount": len(menus_to_add),
                "autoAddedParents": len(parents_to_add)
            }
            
        except Exception as e:
            db.rollback()
            print(f"❌ Error in update_role_permissions: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to update role permissions: {str(e)}")

    def upload_role(file: UploadFile, db: Session):
       try:
           filename = file.filename.lower()
           file.file.seek(0)



           if filename.endswith(".xlsx") or filename.endswith(".xls"):
               df = pd.read_excel(file.file, engine="openpyxl")
           elif filename.endswith(".csv"):
               df = pd.read_csv(file.file, sep="\t")
           else:
               raise HTTPException(status_code=400, detail="File must be .xlsx or .csv")



           if df.empty:
               raise HTTPException(status_code=400, detail="File is empty")



           schema_query = text("""
               SELECT column_name, udt_name
               FROM information_schema.columns
               WHERE table_name = 'role' AND table_schema = 'public'
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



               roleid = row.get('roleid')
               role_name = row.get('role name')
               description = row.get('Description')
               status_str = row.get('Status')
               status = str(status_str).strip().lower() in ["active", "true", "1"]



               insert_data = {
                   'roleid': roleid,
                   'rolename': role_name,
                   'roledescription': description,
                   'rolestatus': status
               }



               # Validate types
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
                           raise HTTPException(
                               status_code=400,
                               detail=f"Row {i}: Field '{field}' must be of type {expected_type.__name__}, "
                                   f"got '{value}' ({type(value).__name__})"
                           )



               sql_check = text("SELECT 1 FROM role WHERE roleid = :roleid")
               data_check = db.execute(sql_check, {"roleid": insert_data["roleid"]}).first()
               if not data_check:
                   sql_insert = text("""
                       INSERT INTO role (roleid, rolename, roledescription, rolestatus)
                       VALUES (:roleid, :rolename, :roledescription, :rolestatus)
                   """)
                   db.execute(sql_insert, insert_data)
               elif data_check.isdeleted:
                   sql_update = text("""
                       UPDATE role SET
                           rolename = :rolename,
                           roledescription = :roledescription,
                           rolestatus = :rolestatus
                       WHERE roleid = :roleid
                   """)
                   db.execute(sql_update, insert_data)
               else:
                   raise HTTPException(
                       status_code=400,
                       detail=f"Row {i}: Role ID '{insert_data['roleid']}' already exists and is not deleted"
                   )
           db.commit()
           return {"code": 200, "message": "Role uploaded successfully"}



       except Exception as e:
           import traceback
           traceback.print_exc()
           raise HTTPException(status_code=500, detail=str(e))