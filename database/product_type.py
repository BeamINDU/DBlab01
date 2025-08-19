from database.connect_to_db import engine, Session, text, SQLAlchemyError
from datetime import datetime, date
from fastapi.responses import JSONResponse
import database.schemas as schemas
from typing import Union, Dict, Any
from fastapi import UploadFile
import pandas as pd

def error_response(code: int, message: str):
    return JSONResponse( status_code=code, content={"detail": {"error": message}} )

def success_response(code: int, content: Union[Dict[str, Any], str]):
    return JSONResponse( status_code=code, content=content)

class ProductTypeDB:
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

    def get_product_types(self, model: schemas.ProdTypeSearch):
        filters = []
        params = {}
        
        if model.prodtypeid:
            filters.append("prodtypeid ILIKE :prodtypeid")
            params["prodtypeid"] = f"%{model.prodtypeid}%"

        if model.prodtype:
            filters.append("prodtype ILIKE :prodtype")
            params["prodtype"] = f"%{model.prodtype}%"

        if model.prodstatus is not None:
            filters.append("prodstatus = :prodstatus")
            params["prodstatus"] = model.prodstatus

        where_clause = " AND " + " AND ".join(filters) if filters else ""

        query = f"""
            SELECT * FROM prodtype
            WHERE isdeleted = false {where_clause}
            ORDER BY prodtype
        """

        return self._fetch_all(query, params)

    def suggest_producttype_id(self, q: str):
        rows = self._fetch_all("""
            SELECT DISTINCT prodtypeid FROM prodtype
            WHERE isdeleted = false 
              AND prodstatus = true
              AND LOWER(prodtypeid) LIKE LOWER(:keyword)
            ORDER BY prodtypeid ASC
            LIMIT 10; """,
            {"keyword": q + "%"}
        )
        return [{"value": row["prodtypeid"], "label": row["prodtypeid"]} for row in rows]
    
    def suggest_producttype_name(self, q: str):
        rows = self._fetch_all("""
            SELECT DISTINCT prodtype FROM prodtype
            WHERE isdeleted = false 
              AND prodstatus = true
              AND LOWER(prodtype) LIKE LOWER(:keyword)
            ORDER BY prodtype ASC
            LIMIT 10; """,
            {"keyword": q + "%"}
        )
        return [{"value": row["prodtype"], "label": row["prodtype"]} for row in rows]
    

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

                WHERE table_name = 'prodtype' AND table_schema = 'public'

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

                prodId = row.get('Product Type ID')

                prodType = row.get('Product Type')

                description = row.get('Description')

                status = True if row.get('Status') == "Active" else False

                status = str(status).strip().lower() in ["active", "true", "1"]

                insert_data = {

                    "prodtypeid": prodId,

                    "prodtype": prodType,

                    "proddescription": description,

                    "prodstatus": status

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

                sql_check = text("SELECT 1 FROM prodtype WHERE  prodtypeid = :prodtypeid")

                if not db.execute(sql_check, {"prodtypeid": insert_data["prodtypeid"]}).first():

                    sql_insert = text("""

                   INSERT INTO prodtype ( prodtypeid , prodtype, proddescription, prodstatus)

                        VALUES ( :prodtypeid, :prodtype, :proddescription, :prodstatus)

                """)

                    db.execute(sql_insert, insert_data)

                else:

                    sql_update = text("""

                   UPDATE prodtype SET

                            prodtype = :prodtype,

                            proddescription = :proddescription,

                            prodstatus = :prodstatus

                        WHERE prodtypeid = :prodtypeid

                """)

                db.execute(sql_update, insert_data)

                db.commit()

            return success_response(200, {"message": "Products uploaded successfully"})



        except Exception as e:

            import traceback

            traceback.print_exc()

            raise error_response(500, detail=str(e))
 
