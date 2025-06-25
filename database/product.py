from database.connect_to_db import engine, Session, text, SQLAlchemyError
from datetime import datetime
from fastapi.responses import JSONResponse
import database.schemas as schemas
from typing import Union, Dict, Any
from fastapi import UploadFile
import pandas as pd

def error_response(code: int, message: str):
    return JSONResponse( status_code=code, content={"detail": {"error": message}} )

def success_response(code: int, content: Union[Dict[str, Any], str]):
    return JSONResponse( status_code=code, content=content)

class ProductDB:
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

    def get_products(self):
        return self._fetch_all("SELECT * FROM product WHERE isdeleted = false")
    
    def get_product_types(self):
        return self._fetch_all("SELECT * FROM prodtype WHERE isdeleted = false")

    def suggest_product_id(self, q: str):
        rows = self._fetch_all("""
            SELECT DISTINCT prodid FROM product
            WHERE isdeleted = false AND prodstatus = true AND LOWER(prodid) LIKE LOWER(:keyword)
            ORDER BY prodid ASC
            LIMIT 10; """,
            {"keyword": q + "%"}
        )
        return [{"value": row["prodid"], "label": row["prodid"]} for row in rows]
    
    def suggest_product_name(self, q: str):
      rows = self._fetch_all("""
          SELECT DISTINCT prodname FROM product
          WHERE isdeleted = false AND prodstatus = true AND LOWER(prodname) LIKE LOWER(:keyword)
          ORDER BY prodname ASC
          LIMIT 10; """,
          {"keyword": q + "%"}
      )
      return [{"value": row["prodname"], "label": row["prodname"]} for row in rows]
    
    def suggest_serial_no(self, q: str):
        rows = self._fetch_all("""
            SELECT DISTINCT prodserial FROM product
            WHERE isdeleted = false AND prodstatus = true AND LOWER(prodserial) LIKE LOWER(:keyword)
            ORDER BY prodserial ASC
            LIMIT 10; """,
            {"keyword": q + "%"}
        )
        return [{"value": row["prodserial"], "label": row["prodserial"]} for row in rows]

    def suggest_producttype_id(self, q: str):
        rows = self._fetch_all("""
            SELECT DISTINCT prodtypeid FROM prodtype
            WHERE isdeleted = false AND prodstatus = true AND LOWER(prodtypeid) LIKE LOWER(:keyword)
            ORDER BY prodtypeid ASC
            LIMIT 10; """,
            {"keyword": q + "%"}
        )
        return [{"value": row["prodtypeid"], "label": row["prodtypeid"]} for row in rows]
    
    def suggest_producttype_name(self, q: str):
        rows = self._fetch_all("""
            SELECT DISTINCT prodtype FROM prodtype
            WHERE isdeleted = false AND prodstatus = true AND LOWER(prodtype) LIKE LOWER(:keyword)
            ORDER BY prodtype ASC
            LIMIT 10; """,
            {"keyword": q + "%"}
        )
        return [{"value": row["prodtype"], "label": row["prodtype"]} for row in rows]
    

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
      update_fields["prodid"] = product.prodid
      update_fields["updateddate"] = now
      update_fields["update_prodid"] = prodid

      # Check updatedby (user id)
      if not db.execute(text("SELECT 1 FROM \"user\" WHERE userid = :userid"),
                        {"userid": product.updatedby}).first():
          return error_response(400, "Invalid user (updatedby)")
      update_fields["updatedby"] = product.updatedby

      # Check prodtypeid
      if not db.execute(text("SELECT 1 FROM prodtype WHERE prodtypeid = :prodtypeid"),
                        {"prodtypeid": product.prodtypeid}).first():
          return error_response(400, "Invalid Product Type")
      update_fields["prodtypeid"] = product.prodtypeid

      # Check prodid duplicate (not self)
      if product.prodid != prodid:
          duplicate_check = db.execute(
              text("SELECT isdeleted FROM product WHERE prodid = :new_prodid"), 
              {"new_prodid": product.prodid}).first()

          if duplicate_check:
              if not duplicate_check.isdeleted:
                  return error_response(400, "New Product ID already exists")
              else:
                  db.execute(
                        text("UPDATE product SET isdeleted = true WHERE prodid = :old_prodid"),
                        {"old_prodid": prodid}
                    )
                  db.commit()
                  update_fields["update_prodid"] = product.prodid

      # field other
      if product.prodname is not None: update_fields["prodname"] = product.prodname
      if product.prodserial is not None: update_fields["prodserial"] = product.prodserial
      if product.prodstatus is not None: update_fields["prodstatus"] = product.prodstatus
      update_fields["isdeleted"] = False

      if not update_fields:
          return error_response(400, "No fields to update")
      
      set_clause = ", ".join([f"{key} = :{key}" for key in update_fields if key != "update_prodid"])
      update_sql = text(f"UPDATE product SET {set_clause} WHERE prodid = :update_prodid")

      try:
        db.execute(update_sql, update_fields)
        db.commit()
        return success_response(200, { "prodid": update_fields.get("prodid", prodid), "updateddate": str(now)})
      except Exception as e:
        db.rollback()
        return error_response(500, f"Database error: {str(e)}")

    @staticmethod
    def delete_product(prodid: str, db: Session):
        if not db.execute(text("SELECT 1 FROM product WHERE prodid = :prodid"), {"prodid": prodid}).first():
            return error_response(404, "Product not found")

        db.execute(text("UPDATE product SET isdeleted = true WHERE prodid = :prodid"), {"prodid": prodid})
        db.commit()
        return success_response(200, {"prodid": prodid, "isdeleted": True})
    
    @staticmethod
    async def upload_products(uploadby: str, file: UploadFile, db: Session):
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
            product_data = []
            for _, row in df.iterrows():
                product_data.append({
                    "prodid": row.get("Product ID"),
                    "prodname": row.get("Product Name"),
                    "prodtypeid": row.get("Product Type ID"),
                    "prodserial": row.get("Serial No"),
                    "prodstatus": row.get("Status", "Active"),
                    "createdby": uploadby,
                    "createddate": now,
                })

            # SQL สำหรับ insert
            insert_sql = """
                INSERT INTO product (
                    prodid, prodname, prodtypeid, prodserial, prodstatus, createdby, createddate
                )
                VALUES (
                    :prodid, :prodname, :prodtypeid, :prodserial, :prodstatus, :createdby, :createddate
                )
            """

            # ทำ bulk insert
            db.execute(text(insert_sql), product_data)
            db.commit()
            return success_response(200,{"message": f"{len(product_data)} records uploaded successfully!"})

        except Exception as e:
            print(f"Error uploading product: {e}")
            db.rollback()
            raise error_response(500, "Failed to upload product")


class ProductTypeService:
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
        update_fields["prodtypeid"] = prodtypeid
        update_fields["updateddate"] = now
        update_fields["update_prodtypeid"] = prodtypeid

        if not db.execute(text("SELECT 1 FROM \"user\" WHERE userid = :userid"),
                          {"userid": prodtype.updatedby}).first():
            return error_response(400, "Invalid user (updatedby)")
        update_fields["updatedby"] = prodtype.updatedby

        
        if prodtype.prodtypeid != prodtypeid:
            duplicate_check = db.execute(
                text("SELECT isdeleted FROM product WHERE prodtypeid = :new_prodtypeid"), 
                {"new_prodtypeid": prodtype.prodtypeid}
            ).first()

            if duplicate_check:
                if not duplicate_check.isdeleted:
                    return error_response(400, "New Product Type ID already exists")
                else:
                    db.execute(
                        text("UPDATE product SET isdeleted = true WHERE prodtypeid = :old_pprodtypeid"),
                        {"old_pprodtypeid": prodtypeid}
                    )
                    db.commit()
                    update_fields["update_prodtypeid"] = prodtype.prodtypeid

        if prodtype.prodtype is not None: update_fields["prodtype"] = prodtype.prodtype
        if prodtype.proddescription is not None: update_fields["proddescription"] = prodtype.proddescription
        if prodtype.prodstatus is not None: update_fields["prodstatus"] = prodtype.prodstatus
        update_fields["isdeleted"] = False
        
        if not update_fields:
            return error_response(400, "No fields to update")
        
        set_clause = ", ".join([f"{key} = :{key}" for key in update_fields if key != "update_prodtypeid"])
        update_sql = text(f"UPDATE prodtype SET {set_clause} WHERE prodtypeid = :update_prodtypeid")

        try:
          db.execute(update_sql, update_fields)
          db.commit()
          return success_response(200, {"prodtypeid": update_fields.get("prodtypeid", prodtypeid), "updateddate": str(now)})
        except Exception as e:
            db.rollback()
            return error_response(500, f"Database error: {str(e)}")
    
    @staticmethod
    def delete_producttype(prodtypeid: str, db: Session):
        if not db.execute(text("SELECT 1 FROM prodtype WHERE prodtypeid = :prodtypeid"), {"prodtypeid": prodtypeid}).first():
            return error_response(404, "Product type not found")

        db.execute(text("UPDATE prodtype SET isdeleted = true WHERE prodtypeid = :prodtypeid"), {"prodtypeid": prodtypeid})
        db.commit()
        return success_response(200,{"prodtypeid": prodtypeid, "isdeleted": True})

    @staticmethod
    async def upload_product_types(uploadby: str, file: UploadFile, db: Session):
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
            product_types_data = []
            for _, row in df.iterrows():
                product_types_data.append({
                    "prodtypeid": row.get("Product Type ID"),
                    "prodtype": row.get("Product Type"),
                    "proddescription": row.get("Description"),
                    "prodstatus": row.get("Status", "Active"),
                    # "createdby": row.get("Created By", "system"),
                    # "createddate": pd.to_datetime(row.get("Created Date")) if pd.notnull(row.get("Created Date")) else None,
                    # "updatedby": row.get("Updated By", "system"),
                    # "updateddate": pd.to_datetime(row.get("Updated Date")) if pd.notnull(row.get("Updated Date")) else None,
                })

            # SQL สำหรับ insert
            insert_sql = """
                INSERT INTO prodtype (
                    prodtypeid, prodtype, proddescription, prodstatus, createdby, createddate
                )
                VALUES (
                    :prodtypeid, :prodtype, :proddescription, :prodstatus, :createdby, :createddate
                )
            """

            # ทำ bulk insert
            db.execute(text(insert_sql), product_types_data)
            db.commit()
            return success_response(200,{"message": f"{len(product_types_data)} records uploaded successfully!"})

        except Exception as e:
            print(f"Error uploading product types: {e}")
            db.rollback()
            raise error_response(500, "Failed to upload product types")
 
