from database.connect_to_db import engine, Session, text, SQLAlchemyError
from fastapi import HTTPException
import database.schemas as schemas
from fastapi.responses import JSONResponse
import database.schemas as schemas
from typing import Union, Dict, Any

def error_response(code: int, message: str):
    return JSONResponse( status_code=code, content={"detail": {"error": message}} )

def success_response(code: int, content: Union[Dict[str, Any], str]):
    return JSONResponse( status_code=code, content=content)

class ReportDB:
    def _fetch_all(self, query: str):
        try:
            with engine.connect() as conn:
                result = conn.execute(text(query))
                return [dict(row) for row in result.mappings()]
        except SQLAlchemyError as e:
            print(f"Database error: {e}")
            return []

    def get_defect_summary(self):
        return self._fetch_all("SELECT * FROM defectsummary")
    
    def get_product_defect_results(self):
        return self._fetch_all("SELECT * FROM productdefectresult")

    def add_report_defect(self, item: schemas.ReportDefectCreate, db: Session):
        try:
            db.execute(text("""
                INSERT INTO defectsummary (lotno, producttype, defecttype, total, ok, ng)
                VALUES (:lotno, :producttype, :defecttype, :total, :ok, :ng)
            """), item.dict(by_alias=True))
            db.commit()
            return success_response(200, {"status": "DefectSummary added", "lotNo": item.lotno})
        
        except SQLAlchemyError as e:
            raise error_response(500, str(e))

    def update_report_defect(self, lotno: str, item: schemas.ReportDefectUpdate, db: Session):
        try:
            update_fields = item.dict(exclude_unset=True, by_alias=True)
            if not update_fields:
                raise error_response(400, "No fields to update")
            update_fields["lotno"] = lotno
            set_clause = ", ".join([f"{k} = :{k}" for k in update_fields if k != "lotno"])
            db.execute(text(f"""
                UPDATE defectsummary SET {set_clause} WHERE lotno = :lotno
            """), update_fields)
            db.commit()
            return success_response(200, {"status": "DefectSummary updated", "lotNo": lotno})
        
        except SQLAlchemyError as e:
            raise error_response(500, str(e))

    def add_report_product(self, item: schemas.ReportProductCreate, db: Session):
        try:
            db.execute(text("""
                INSERT INTO productdefectresult (
                    datetime, productid, productname, lotno, status, defecttype, cameraid
                ) VALUES (
                    :datetime, :productid, :productname, :lotno, :status, :defecttype, :cameraid
                )
            """), item.dict(by_alias=True))
            db.commit()
            return success_response(200, {"status": "ProductDefectResult added", "productId": item.productid})
        
        except SQLAlchemyError as e:
            raise error_response(500, str(e))

    def update_report_product(self, productid: str, item: schemas.ReportProductUpdate, db: Session):
        try:
            update_fields = item.dict(exclude_unset=True, by_alias=True)
            if not update_fields:
                raise error_response(400, "No fields to update")
            
            update_fields["productid"] = productid
            set_clause = ", ".join([f"{k} = :{k}" for k in update_fields if k != "productid"])
            db.execute(text(f"""
                UPDATE productdefectresult SET {set_clause} WHERE productid = :productid
            """), update_fields)
            db.commit()
            return success_response(200, {"status": "ProductDefectResult updated", "productId": productid})
        
        except SQLAlchemyError as e:
            raise error_response(500, str(e))

    def add_product_detail(self, item: schemas.ProductDetailCreate, db: Session):
        try:
            db.execute(text("""
                INSERT INTO productdetail (
                    productid, productname, serialno, date, time, lotno,
                    defecttype, cameraid, status, comment
                ) VALUES (
                    :productid, :productname, :serialno, :date, :time, :lotno,
                    :defecttype, :cameraid, :status, :comment
                )
            """), item.dict(exclude={"history"}, by_alias=True))

            for h in item.history:
                db.execute(text("""
                    INSERT INTO history (date, time, updatedby, productid)
                    VALUES (:date, :time, :updatedby, :productid)
                """), {**h.dict(by_alias=True), "productid": item.productid})

            db.commit()
            return success_response(200, {"status": "ProductDetail added", "productId": item.productid})
        
        except SQLAlchemyError as e:
            raise error_response(500, str(e))

