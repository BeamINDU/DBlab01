from database.connect_to_db import engine, Session, text, SQLAlchemyError
from datetime import datetime
from fastapi.responses import JSONResponse
import database.schemas as schemas
from typing import Union, Dict, Any

def error_response(code: int, message: str):
    return JSONResponse( status_code=code, content={"detail": {"error": message}} )

def success_response(code: int, content: Union[Dict[str, Any], str]):
    return JSONResponse( status_code=code, content=content)

class ProductDB:
    def _fetch_all(self, query: str):
        try:
            with engine.connect() as conn:
                result = conn.execute(text(query))
                return [dict(row) for row in result.mappings()]
        except SQLAlchemyError as e:
            print(f"Database error: {e}")
            return []

    def get_products(self):
        return self._fetch_all("SELECT * FROM product WHERE isdeleted = false")
    
    def get_product_types(self):
        return self._fetch_all("SELECT * FROM prodtype WHERE isdeleted = false")

class ProductService:
    @staticmethod
    def add_product(product: schemas.ProductCreate, db: Session):
        # Check if user exists
        if not db.execute(text("SELECT 1 FROM \"user\" WHERE userid = :userid"),
                          {"userid": product.createdby}).first():
            return error_response(400, "Invalid user (createdBy)")

        # Check if product already exists
        existing_product = db.execute(
            text("SELECT isdeleted FROM product WHERE prodid = :prodid"),
            {"prodid": product.prodid}
        ).first()

        now = datetime.now()

        if existing_product:
            if not existing_product.isdeleted:  # isdeleted = False
                return error_response(400, "Product ID already exists")

            # If isdeleted = true, restore the old record
            update_sql = text("""
                UPDATE product SET
                    prodname = :prodname,
                    prodtypeid = :prodtypeid,
                    prodserial = :prodserial,
                    prodstatus = :prodstatus,
                    createdby = :createdby,
                    createddate = :createddate,
                    isdeleted = false
                WHERE prodid = :prodid
            """)
            db.execute(update_sql, {
                "prodid": product.prodid,
                "prodname": product.prodname,
                "prodtypeid": product.prodtypeid,
                "prodserial": product.prodserial,
                "prodstatus": bool(product.prodstatus),
                "createdby": product.createdby,
                "createddate": now,
                "updatedby": None  ,
                "updateddate": None  
            })
        else:
            # Insert new record
            insert_sql = text("""
                INSERT INTO product (
                    prodid, prodname, prodtypeid, prodserial, 
                    prodstatus, createdby, createddate, isdeleted
                ) VALUES (
                    :prodid, :prodname, :prodtypeid, :prodserial,
                    :prodstatus, :createdby, :createddate, false
                )
            """)
            db.execute(insert_sql, {
                "prodid": product.prodid,
                "prodname": product.prodname,
                "prodtypeid": product.prodtypeid,
                "prodserial": product.prodserial,
                "prodstatus": bool(product.prodstatus),
                "createdby": product.createdby,
                "createddate": now
            })

        db.commit()
        return success_response(200, {"prodid": product.prodid, "createddate": str(now)})

    @staticmethod
    def update_product(prodid: str, product: schemas.ProductUpdate, db: Session):
      # Check if defect already exists
      if not db.execute(text("SELECT 1 FROM product WHERE prodid = :prodid"), {"prodid": prodid}).first():
          return error_response(404, "Product not found")

      update_fields = {}
      now = datetime.now()
      
      # Check updatedby (user id)
      if product.updatedby:
          if not db.execute(text("SELECT 1 FROM \"user\" WHERE userid = :userid"),
                            {"userid": product.updatedby}).first():
              return error_response(400, "Invalid user (updatedby)")
          update_fields["updatedby"] = product.updatedby

      # Check prodtypeid
      if product.prodtypeid is not None:
          if not db.execute(text("SELECT 1 FROM prodtype WHERE prodtypeid = :prodtypeid"),
                            {"prodtypeid": product.prodtypeid}).first():
              return error_response(400, "Invalid Product Type")
          update_fields["prodtypeid"] = product.prodtypeid

      # Check prodid duplicate (not self)
      if product.prodid and product.prodid != prodid:
          duplicate_check = db.execute(text("""
              SELECT isdeleted FROM product WHERE prodid = :new_prodid
          """), {"new_prodid": product.prodid}).first()

          if duplicate_check:
              if not duplicate_check.isdeleted:
                  return error_response(400, "New Product ID already exists")
              else:
                  # duplicate → delete record where isdeleted = true
                  db.execute(
                        text("UPDATE product SET isdeleted = true WHERE prodid = :new_prodid"),
                        {"new_prodid": product.prodid}
                    )
                  db.commit()

          update_fields["prodid"] = product.prodid

      # field other
      if product.prodname is not None:
          update_fields["prodname"] = product.prodname
      if product.prodserial is not None:
          update_fields["prodserial"] = product.prodserial
      if product.prodstatus is not None:
          update_fields["prodstatus"] = product.prodstatus
      if product.updatedby is not None:
          update_fields["updatedby"] = product.updatedby

      update_fields["updateddate"] = now

      if not update_fields:
          return error_response(400, "No fields to update")

      update_fields["old_prodid"] = prodid
      set_clause = ", ".join([f"{key} = :{key}" for key in update_fields if key != "old_prodid"])

      update_sql = text(f"UPDATE product SET {set_clause} WHERE prodid = :old_prodid")
      db.execute(update_sql, update_fields)
      db.commit()
      return success_response(200, { "prodid": update_fields.get("prodid", prodid), "updateddate": str(now)})

    @staticmethod
    def delete_product(prodid: str, db: Session):
        if not db.execute(text("SELECT 1 FROM product WHERE prodid = :prodid"), {"prodid": prodid}).first():
            return error_response(404, "Product not found")

        db.execute(text("UPDATE product SET isdeleted = true WHERE prodid = :prodid"), {"prodid": prodid})
        db.commit()
        return success_response(200, {"prodid": prodid, "isdeleted": True})
    
    @staticmethod
    def add_prodtype(prodtype: schemas.ProdTypeCreate, db: Session):
        if not db.execute(text("SELECT 1 FROM \"user\" WHERE userid = :userid"),
                          {"userid": prodtype.createdby}).first():
            return error_response(400, "Invalid user (createdBy)")
        
        existing_prodtype = db.execute(
            text("SELECT isdeleted FROM prodtype WHERE prodtypeid = :prodtypeid"),
            {"prodtypeid": prodtype.prodtypeid}
        ).first()

        now = datetime.now()

        if existing_prodtype:
            if not existing_prodtype.isdeleted:
                return error_response(400, "Product Type ID already exists")

            update_sql = text("""
                UPDATE prodtype SET
                    prodtypeid = :prodtypeid,
                    prodtype = :prodtype,
                    proddescription = :proddescription,
                    prodstatus = :prodstatus,
                    createdby = :createdby,
                    createddate = :createddate,
                    isdeleted = false
                WHERE prodtypeid  = :prodtypeid 
            """)
            db.execute(update_sql, {
                "prodtypeid": prodtype.prodtypeid,
                "prodtype": prodtype.prodtype,
                "proddescription": prodtype.proddescription,
                "prodstatus": bool(prodtype.prodstatus),
                "createdby": prodtype.createdby,
                "createddate": now,
                "updatedby": None  ,
                "updateddate": None  
            })
        else:
            insert_sql = text("""
                INSERT INTO prodtype (
                    prodtypeid, prodtype, proddescription, 
                    prodstatus, createdby, createddate
                ) VALUES (
                    :prodtypeid, :prodtype, :proddescription, 
                    :prodstatus, :createdby, :createddate
                )
            """)
            db.execute(insert_sql, {
                "prodtypeid": prodtype.prodtypeid,
                "prodtype": prodtype.prodtype,
                "proddescription": prodtype.proddescription,
                "prodstatus": prodtype.prodstatus,
                "createdby": prodtype.createdby,
                "createddate": now
            })
        db.commit()
        return success_response(200, {"prodid": prodtype.prodtypeid, "createddate": str(now)})

    @staticmethod
    def update_prodtype(prodtypeid: str, prodtype: schemas.ProdTypeUpdate, db: Session):
        if not db.execute(text("SELECT 1 FROM prodtype WHERE prodtypeid = :prodtypeid"),
                          {"prodtypeid": prodtypeid}).first():
            return error_response(404, "Product type not found")

        update_fields = {}
        now = datetime.now()

        if prodtype.updatedby:
            if not db.execute(text("SELECT 1 FROM \"user\" WHERE userid = :userid"),
                              {"userid": prodtype.updatedby}).first():
                return error_response(400, "Invalid user (updatedby)")
            update_fields["updatedby"] = prodtype.updatedby

        
        if prodtype.prodtypeid and prodtype.prodtypeid != prodtypeid:
            duplicate_check = db.execute(text("""
                SELECT isdeleted FROM product WHERE prodtypeid = :new_prodtypeid
            """), {"new_prodtypeid": prodtype.prodtypeid}).first()

            if duplicate_check:
                if not duplicate_check.isdeleted:
                    return error_response(400, "New Product Type ID already exists")
                else:
                    db.execute(
                        text("UPDATE product SET isdeleted = true WHERE prodtypeid = :new_pprodtypeid"),
                        {"new_pprodtypeid": prodtype.prodtypeid}
                    )
                    db.commit()

            update_fields["prodtypeid"] = prodtype.prodtypeid

        if prodtype.prodtype is not None:
            update_fields["prodtype"] = prodtype.prodtype
        if prodtype.proddescription is not None:
            update_fields["proddescription"] = prodtype.proddescription
        if prodtype.prodstatus is not None:
            update_fields["prodstatus"] = prodtype.prodstatus
        if prodtype.updatedby:
            update_fields["updatedby"] = prodtype.updatedby

        update_fields["updateddate"] = now

        if not update_fields:
            return error_response(400, "No fields to update")

        update_fields["old_prodtypeid"] = prodtypeid
        set_clause = ", ".join([f"{key} = :{key}" for key in update_fields if key != "old_prodtypeid"])

        update_sql = text(f"UPDATE prodtype SET {set_clause} WHERE prodtypeid = :old_prodtypeid")
        db.execute(update_sql, update_fields)
        db.commit()
        return success_response(200, {"prodtypeid": update_fields.get("prodtypeid", prodtypeid), "updateddate": str(now)})
    
    @staticmethod
    def delete_producttype(prodtypeid: str, db: Session):
        if not db.execute(text("SELECT 1 FROM prodtype WHERE prodtypeid = :prodtypeid"), {"prodtypeid": prodtypeid}).first():
            return error_response(404, "Product type not found")

        db.execute(text("UPDATE prodtype SET isdeleted = true WHERE prodtypeid = :prodtypeid"), {"prodtypeid": prodtypeid})
        db.commit()
        return success_response(200,{"prodtypeid": prodtypeid, "isdeleted": True})
