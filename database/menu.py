from database.connect_to_db import engine, Session, text, SQLAlchemyError
from fastapi import HTTPException
import database.schemas as schemas

class MenuDB:
    def _fetch_all(self, query: str):
        try:
            with engine.connect() as conn:
                result = conn.execute(text(query))
                return [dict(row) for row in result.mappings()]
        except SQLAlchemyError as e:
            print(f"Database error: {e}")
            return []

    def get_menu(self):
        return self._fetch_all("SELECT * FROM menu ORDER BY seq")

    def add_menu(self, menu: schemas.MenuCreate, db: Session):
        # Check if menuid already exists
        if db.execute(text("SELECT 1 FROM menu WHERE menuid = :menuid"), {"menuid": menu.menuid}).first():
            raise HTTPException(status_code=400, detail="Menu ID already exists")

        # Validate parentId if provided
        if menu.parentid:
            if not db.execute(text("SELECT 1 FROM menu WHERE menuid = :pid"), {"pid": menu.parentid}).first():
                raise HTTPException(status_code=400, detail="Invalid parentId")

        # Insert menu
        insert_sql = text("""
            INSERT INTO menu (menuid, parentid, menuname, icon, seq, path)
            VALUES (:menuid, :parentid, :menuname, :icon, :seq, :path)
        """)
        db.execute(insert_sql, {
            "menuid": menu.menuid,
            "parentid": menu.parentid,
            "menuname": menu.menuname,
            "icon": menu.icon,
            "seq": menu.seq,
            "path": menu.path
        })

        db.commit()
        return {"status": "Menu created", "menuId": menu.menuid}

    def update_menu(self, menuid: str, menu: schemas.MenuUpdate, db: Session):
        # Ensure the menu to update exists
        if not db.execute(text("SELECT 1 FROM menu WHERE menuid = :menuid"), {"menuid": menuid}).first():
            raise HTTPException(status_code=404, detail="Menu not found")

        # Validate parentId if provided
        if menu.parentid:
            if not db.execute(text("SELECT 1 FROM menu WHERE menuid = :pid"), {"pid": menu.parentid}).first():
                raise HTTPException(status_code=400, detail="Invalid parentId")

        # Collect update fields
        update_fields = menu.dict(exclude_unset=True, by_alias=True)
        if not update_fields:
            raise HTTPException(status_code=400, detail="No fields to update")

        update_fields["menuid"] = menuid
        set_clause = ", ".join([f"{key} = :{key}" for key in update_fields if key != "menuid"])
        update_sql = text(f"UPDATE menu SET {set_clause} WHERE menuid = :menuid")

        db.execute(update_sql, update_fields)
        db.commit()
        return {"status": "Menu updated", "menuId": menuid}

