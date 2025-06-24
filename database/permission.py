from database.connect_to_db import engine, Session, text, SQLAlchemyError
from fastapi import HTTPException
import database.schemas as schemas

class PermissionDB:
    def add_permission(self, perm: schemas.PermissionCreate, db: Session):
        if db.execute(text("SELECT 1 FROM permission WHERE permissionid = :permissionid"),
                      {"permissionid": perm.permissionid}).first():
            raise HTTPException(status_code=400, detail="Permission ID already exists")

        if not db.execute(text("SELECT 1 FROM menu WHERE menuid = :menuid"),
                          {"menuid": perm.menuid}).first():
            raise HTTPException(status_code=400, detail="Invalid menuid")

        if not db.execute(text("SELECT 1 FROM menuaction WHERE actionid = :actionid"),
                          {"actionid": perm.actionid}).first():
            raise HTTPException(status_code=400, detail="Invalid actionid")

        insert_sql = text("""
            INSERT INTO permission (permissionid, menuid, actionid)
            VALUES (:permissionid, :menuid, :actionid)
        """)

        db.execute(insert_sql, {
            "permissionid": perm.permissionid,
            "menuid": perm.menuid,
            "actionid": perm.actionid
        })
        db.commit()
        return {"status": "Permission added", "permissionId": perm.permissionid}

    def update_permission(self, permissionid: int, perm: schemas.PermissionUpdate, db: Session):
        if not db.execute(text("SELECT 1 FROM permission WHERE permissionid = :permissionid"),
                          {"permissionid": permissionid}).first():
            raise HTTPException(status_code=404, detail="Permission not found")

        update_fields = {}

        if perm.menuid is not None:
            if not db.execute(text("SELECT 1 FROM menu WHERE menuid = :menuid"),
                              {"menuid": perm.menuid}).first():
                raise HTTPException(status_code=400, detail="Invalid menuid")
            update_fields["menuid"] = perm.menuid

        if perm.actionid is not None:
            if not db.execute(text("SELECT 1 FROM menuaction WHERE actionid = :actionid"),
                              {"actionid": perm.actionid}).first():
                raise HTTPException(status_code=400, detail="Invalid actionid")
            update_fields["actionid"] = perm.actionid

        if not update_fields:
            raise HTTPException(status_code=400, detail="No fields to update")

        update_fields["permissionid"] = permissionid
        set_clause = ", ".join([f"{key} = :{key}" for key in update_fields if key != "permissionid"])
        update_sql = text(f"UPDATE permission SET {set_clause} WHERE permissionid = :permissionid")

        db.execute(update_sql, update_fields)
        db.commit()
        return {"status": "Permission updated", "permissionId": permissionid}
    
    @staticmethod
    def delete_permission(permissionid: int, db: Session):
        if not db.execute(text("SELECT 1 FROM permission WHERE permissionid = :permissionid"),
                        {"permissionid": permissionid}).first():
            raise HTTPException(status_code=404, detail="Permission not found")

        update_sql = text("UPDATE permission SET isdeleted = true WHERE permissionid = :permissionid")
        db.execute(update_sql, {"permissionid": permissionid})
        db.commit()

        return {"status": 200, "detail": {"permissionid": permissionid}}



