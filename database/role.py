from database.connect_to_db import engine, Session, text, SQLAlchemyError
from fastapi import HTTPException
import database.schemas as schemas
from datetime import datetime
from fastapi.responses import JSONResponse
from typing import Union, Dict, Any

def error_response(code: int, message: str):
    return JSONResponse(status_code=code, content={"detail": {"error": message}})

def success_response(code: int, content: Union[Dict[str, Any], str]):
    return JSONResponse(status_code=code, content=content)

class RoleDB:
    # ... existing methods ...
    
    def get_roles(self):
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT * FROM role WHERE isdeleted = false ORDER BY rolename"))
                return list(result.mappings())
        except SQLAlchemyError as e:
            print(f"Database error: {e}")
            return []

    def suggest_role_name(self, q: str):
        try:
            with engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT rolename 
                    FROM role 
                    WHERE rolename ILIKE :q AND isdeleted = false 
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
        # Check if role already exists
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

        # Check rolename duplicate (not self)
        old_rolename = existing_role.rolename

        if role.rolename != old_rolename:
            duplicate_check = db.execute(
                text("SELECT isdeleted FROM role WHERE rolename = :new_rolename"), 
                {"new_rolename": role.rolename}).first()

            if duplicate_check:
                if not duplicate_check.isdeleted:
                    return error_response(400, "New role name already exists")

        if hasattr(role, 'rolename') and role.rolename:
            update_fields["rolename"] = role.rolename

        if not update_fields:
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

    # ✅ ย้าย role-permissions methods มาที่นี่
    def get_role_permissions(self, roleid: int, db: Session):
        """
        Get permissions for a specific role
        """
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
                # แปลง actionid string เป็น array of integers
                actionid_str = str(row.actionid)
                if ',' in actionid_str:
                    # "1,2,3,4,5,6" → [1,2,3,4,5,6]
                    actions = [int(x.strip()) for x in actionid_str.split(',') if x.strip().isdigit()]
                else:
                    # "1" → [1]
                    actions = [int(actionid_str)] if actionid_str.isdigit() else [1]
                
                permission_list.append({
                    "menuid": row.menuid,
                    "menuname": row.menuname,
                    "parentid": row.parentid or "",
                    "seq": row.seq,
                    "path": row.path or "",
                    "icon": row.icon or "",
                    "actionid": actions[0] if actions else 1,  # First action as int
                    "actions": actions  # Array of integers
                })
            
            return {"permissions": permission_list}
            
        except Exception as e:
            print(f"Error in get_role_permissions: {str(e)}")
            return {"permissions": []}

    def update_role_permissions(self, roleid: int, permissions_data: Dict[str, Any], db: Session):
        """
        Update permissions for a role using (roleid, menuid) as PK - no permissionid needed
        Auto-add parent menus when child menus are selected
        """
        try:
            print(f"🔄 Starting update_role_permissions for roleId: {roleid}")
            print(f"📋 Permissions data: {permissions_data}")
            
            # ตรวจสอบว่า role มีอยู่จริง
            if not db.execute(text("SELECT 1 FROM role WHERE roleid = :roleid"), 
                             {"roleid": roleid}).first():
                raise HTTPException(status_code=404, detail="Role not found")

            # ดึงข้อมูล menu structure เพื่อหา parent menus
            menu_query = text("""
                SELECT menuid, parentid, menuname 
                FROM menu 
                ORDER BY seq
            """)
            menus = db.execute(menu_query).fetchall()
            
            # สร้าง mapping ของ parent-child relationships
            menu_parents = {}
            for menu in menus:
                if menu.parentid:  # มี parent
                    menu_parents[menu.menuid] = menu.parentid

            # ลบ permissions เก่าทั้งหมดของ role นี้
            delete_sql = text("DELETE FROM rolepermission WHERE roleid = :roleid")
            db.execute(delete_sql, {"roleid": roleid})
            print(f"🗑️ Deleted existing permissions for roleId: {roleid}")

            # เตรียม permissions ใหม่
            permissions = permissions_data.get('permissions', [])
            menus_to_add = set()  # ใช้ set เพื่อหลีกเลี่ยง duplicate
            
            # เพิ่ม menus ที่เลือกและ parent menus ที่จำเป็น
            for perm in permissions:
                menuid = perm.get('menuId')
                if menuid:
                    menus_to_add.add(menuid)
                    
                    # ถ้า menu นี้มี parent ให้เพิ่ม parent ด้วย
                    if menuid in menu_parents:
                        parent_id = menu_parents[menuid]
                        menus_to_add.add(parent_id)
                        print(f"📋 Auto-adding parent menu: {parent_id} for child: {menuid}")

            # Insert permissions
            if menus_to_add:
                for menuid in menus_to_add:
                    # หา actions สำหรับ menu นี้
                    actions = [1]  # default View permission
                    
                    # ถ้าเป็น menu ที่เลือกไว้ ใช้ actions ที่กำหนด
                    for perm in permissions:
                        if perm.get('menuId') == menuid:
                            actions = perm.get('actions', [1])
                            break
                    
                    # ถ้าเป็น parent menu ที่ auto-add ให้เฉพาะ View permission
                    is_auto_added_parent = (
                        menuid not in [p.get('menuId') for p in permissions] and
                        menuid in [menu_parents.get(p.get('menuId')) for p in permissions if p.get('menuId') in menu_parents]
                    )
                    
                    if is_auto_added_parent:
                        actions = [1]  # เฉพาะ View สำหรับ parent ที่ auto-add
                    
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
                    
                    action_type = "auto-added parent" if is_auto_added_parent else "selected"
                    print(f"✅ Inserted permission ({action_type}): roleId={roleid}, menuId={menuid}, actions={actions_str}")

            db.commit()
            print(f"💾 Successfully updated role permissions for roleId: {roleid}")
            
            return {
                "status": "success",
                "message": "Role permissions updated successfully",
                "roleId": roleid,
                "permissionsCount": len(menus_to_add),
                "autoAddedParents": len(menus_to_add) - len(permissions)
            }
            
        except Exception as e:
            db.rollback()
            print(f"❌ Error in update_role_permissions: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to update role permissions: {str(e)}")