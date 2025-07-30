from database.connect_to_db import engine, Session, text, SQLAlchemyError
from fastapi import HTTPException
import database.schemas as schemas
from fastapi.responses import JSONResponse
import database.schemas as schemas
from typing import Union, Dict, Any
from sqlalchemy import text 
from datetime import datetime, date
from database.images import ImagesService

def error_response(code: int, message: str):
    return JSONResponse( status_code=code, content={"detail": {"error": message}} )

def success_response(code: int, content: Union[Dict[str, Any], str]):
    return JSONResponse( status_code=code, content=content)

class ReportDB:
    def _fetch_one(self, query: str, params: dict):
        try:
            with engine.connect() as conn:
              result = conn.execute(text(query), params)
              return result.mappings().first()
        except SQLAlchemyError as e:
            print(f"Database error: {e}")
            return []

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
    
    #--- Report Product Defect Result -------------------------------------------------------------
    
    def get_report_product_defect_results(self, model: schemas.ReportProductSearch):
        where_filters = []
        having_filters = []
        params = {}

        allowed_order_fields = [ "defecttime", "prodid", "prodname", "prodseq", "cameraid", "cameraname", "prodstatus", "defectdetail" ]

        order_by = model.order_by if model.order_by in allowed_order_fields else "defecttime"
        order_dir = "DESC" if model.order_dir.lower() == "desc" else "ASC"

        # --- Filters (WHERE) ---
        if model.startdate:
            where_filters.append("pr.defecttime >= :startdate")
            params["startdate"] = model.startdate

        if model.enddate:
            where_filters.append("pr.defecttime <= :enddate")
            params["enddate"] = model.enddate

        if model.prodid:
            where_filters.append("pr.prodid ILIKE :prodid")
            params["prodid"] = f"%{model.prodid}%"

        if model.prodname:
            where_filters.append("p.prodname ILIKE :prodname")
            params["prodname"] = f"%{model.prodname}%"

        if model.cameraid:
            where_filters.append("pr.cameraid ILIKE :cameraid")
            params["cameraid"] = f"%{model.cameraid}%"

        if model.cameraname:
            where_filters.append("c.cameraname ILIKE :cameraname")
            params["cameraname"] = f"%{model.cameraname}%"

        # --- Filters (HAVING) ---
        if model.prodstatus:
            having_filters.append("CASE WHEN BOOL_OR(pr.prodstatus = 'NG') THEN 'NG' ELSE 'OK' END = :prodstatus")
            params["prodstatus"] = model.prodstatus

        if model.defecttype:
            having_filters.append("STRING_AGG(d.defecttype, ', ' ORDER BY d.defecttype) ILIKE :defecttype")
            params["defecttype"] = f"%{model.defecttype}%"

        where_clause = "WHERE " + " AND ".join(where_filters) if where_filters else ""
        having_clause = "HAVING " + " AND ".join(having_filters) if having_filters else ""

        # --- Pagination ---
        page = model.page or 1
        page_size = model.pageSize or 10
        offset = (page - 1) * page_size

        # --- Main Query (with LIMIT) ---
        main_query = f"""
            SELECT *
            FROM (
                SELECT
                    ROW_NUMBER() OVER (ORDER BY pr.defecttime) AS runningno,
                    pr.defecttime,
                    pr.prodid,
                    p.prodname,
                    pr.prodseq,
                    pr.cameraid,
                    c.cameraname,
                    pr.imagepath,
                    CASE WHEN BOOL_OR(pr.prodstatus = 'NG') THEN 'NG' ELSE 'OK' END AS prodstatus,
                    STRING_AGG(d.defecttype, ', ' ORDER BY d.defecttype) AS defectdetail,
                    STRING_AGG(pr.comment, ', ' ORDER BY pr.resultid) AS comment
                FROM productdefectresult pr
                LEFT JOIN product p ON p.prodid = pr.prodid
                LEFT JOIN camera c ON c.cameraid = pr.cameraid
                LEFT JOIN defecttype d ON d.defectid = pr.defectid
                {where_clause}
                GROUP BY pr.cameraid, c.cameraname, pr.prodid, p.prodname, pr.prodseq, pr.defecttime, pr.imagepath
                {having_clause}
            ) AS subquery
            ORDER BY {order_by} {order_dir}
            LIMIT :limit OFFSET :offset
        """

        params["limit"] = page_size
        params["offset"] = offset

        # --- Total Count Query ---
        count_query = f"""
            SELECT COUNT(*) FROM (
                SELECT 1
                FROM productdefectresult pr
                LEFT JOIN product p ON p.prodid = pr.prodid
                LEFT JOIN camera c ON c.cameraid = pr.cameraid
                LEFT JOIN defecttype d on d.defectid = pr.defectid
                {where_clause}
                GROUP BY pr.cameraid, c.cameraname, pr.prodid, p.prodname, pr.prodseq, pr.defecttime, pr.imagepath
                {having_clause}
            ) AS count
        """
        # print("SQL Query:", main_query)
        # print("Parameters:", params)

        total = self._fetch_one(count_query, params)["count"]
        items = self._fetch_all(main_query, params)

        return {
            "total": total,
            "items": items
        }

    def report_product_defect_detail(self, model: schemas.ReportProductSearchDetail, db: Session) -> Dict[str, Any]:
        try:
            query = text("""
                SELECT 
                    DATE(pr.defecttime) as defectdate,
                    TO_CHAR(pr.defecttime, 'HH24:MI:SS') as defecttime,
                    pr.prodid,
                    p.prodname, 
                    pr.prodseq,
                    p.prodserial, 
                    p.prodtypeid, 
                    pt.prodtype, 
                    pr.cameraid,
                    c.cameraname,
                    CASE WHEN BOOL_OR(pr.prodstatus = 'NG') THEN 'NG' ELSE 'OK' END AS prodstatus,
                    STRING_AGG(d.defecttype, ', ' ORDER BY d.defecttype) AS defectdetail,
                    pr.imagepath,
                    STRING_AGG(pr.comment, ', ' ORDER BY pr.resultid) AS comment
                FROM productdefectresult pr
                LEFT JOIN product p ON p.prodid = pr.prodid 
                LEFT JOIN prodtype pt ON pt.prodtypeid = p.prodtypeid
                LEFT JOIN defecttype d ON d.defectid = pr.defectid
                LEFT JOIN camera c ON c.cameraid = pr.cameraid
                WHERE pr.prodid = :prodid
                  AND pr.prodseq = :prodseq
                  AND pr.cameraid = :cameraid
                  AND pr.imagepath = :imagepath
                GROUP BY 
                    pr.defecttime, pr.prodid, p.prodname, p.prodserial, 
                    p.prodtypeid, pt.prodtype, pr.cameraid, c.cameraname,
                    pr.imagepath, pr.prodseq, pr.comment
            """)

            params = {
                "prodid": model.prodid,
                "prodseq": model.prodseq,
                "cameraid": model.cameraid,
                "imagepath": model.imagepath
            }

            defect_result = db.execute(query, params).mappings().fetchone()
            if not defect_result:
                return {"error": "No data found"}

            data = dict(defect_result)

            history_query = text("""
                SELECT
                    actiondate,
                    status,
                    "comment",
                    actionby
                FROM productdefectresult_log
                WHERE prodid = :prodid
                  AND prodseq = :prodseq
                  AND cameraid = :cameraid
                  AND imagepath = :imagepath
                ORDER BY actiondate DESC, status, "comment"
            """)

            history_params = {
                "prodid": model.prodid,
                "prodseq": model.prodseq,
                "cameraid": model.cameraid,
                "imagepath": model.imagepath
            }

            history_result = db.execute(history_query, history_params).mappings().all()
            data["history"] = [dict(row) for row in history_result]

            # try:
            #     imagedata = ImagesService().result_image(model.imagepath)
            #     data["image64"] = imagedata.results.image_b64
            # except Exception:
            #     data["image64"] = None

            return data

        except Exception as e:
            return {"error": str(e)}
    
    def update_product_defect_detail(self, item: schemas.ProductDetailUpdate, db: Session):
        try:
            current_query = text("""
                SELECT comment, prodstatus FROM productdefectresult
                WHERE prodid = :prodid
                  AND prodseq = :prodseq
                  AND cameraid = :cameraid
                  AND imagepath = :imagepath
                  AND DATE_TRUNC('second', defecttime) = TIMESTAMP :defecttime
            """)

            current = db.execute(current_query, {
                "prodid": item.prodid,
                "prodseq": item.prodseq,
                "cameraid": item.cameraid,
                "imagepath": item.imagepath,
                "defecttime": item.defecttime
            }).mappings().first()

            if not current:
                return error_response(404, "Record not found")

            # ถ้าไม่มีการเปลี่ยนแปลงค่า comment หรือ prodstatus → ไม่ต้อง update หรือ insert log
            current_comment = current["comment"] or ""
            new_comment = item.comment or ""

            if current_comment == new_comment and current["prodstatus"] == item.prodstatus:
                return success_response(200, {
                    "result": "No changes detected",
                    "prodid": item.prodid,
                    "prodseq": item.prodseq,
                    "cameraid": item.cameraid,
                    "imagepath": item.imagepath,
                    "datetime": item.defecttime,
                })

            # Update
            update_query = text("""
                UPDATE productdefectresult 
                SET comment = :comment, prodstatus = :prodstatus
                WHERE prodid = :prodid
                  AND prodseq = :prodseq
                  AND cameraid = :cameraid
                  AND imagepath = :imagepath
                  AND DATE_TRUNC('second', defecttime) = TIMESTAMP :defecttime
            """)

            update_params = {
                "comment": item.comment,
                "prodstatus": item.prodstatus,
                "prodid": item.prodid,
                "prodseq": item.prodseq,
                "cameraid": item.cameraid,
                "imagepath": item.imagepath,
                "defecttime": item.defecttime
            }

            result = db.execute(update_query, update_params)

            if result.rowcount == 0:
                db.rollback()
                return error_response(404, "No record updated. Please check the input values.")

            # Log only if values changed
            insert_query = text("""
                INSERT INTO productdefectresult_log 
                (prodid, prodseq, cameraid, imagepath, comment, status, actiondate, actionby)
                VALUES (:prodid, :prodseq, :cameraid, :imagepath, :comment, :status, :actiondate, :actionby)
            """)

            log_params = {
                "prodid": item.prodid,
                "prodseq": item.prodseq,
                "cameraid": item.cameraid,
                "imagepath": item.imagepath,
                "comment": item.comment,
                "status": item.prodstatus,
                "actiondate": datetime.now(),
                "actionby": item.actionby
            }

            db.execute(insert_query, log_params)
            db.commit()

            return success_response(200, {
                "result": "ProductDefectResult updated and logged",
                "prodid": item.prodid,
                "prodseq": item.prodseq,
                "cameraid": item.cameraid,
                "imagepath": item.imagepath,
                "datetime": item.defecttime,
            })

        except SQLAlchemyError as e:
            db.rollback()
            return error_response(500, f"Database error: {str(e)}")

   
    #--- Report Defect Summary -------------------------------------------------------------
    
    def get_defect_summary(self, model: schemas.ReportDefectSearch):
        where_filters = []
        params = {}

        allowed_order_fields = [ "prodlot", "prodid", "prodname", "defectid", "defecttype", "totalprod", "totalok", "totalng" ]

        order_by = model.order_by if model.order_by in allowed_order_fields else "prodlot"
        order_dir = "DESC" if model.order_dir.lower() == "desc" else "ASC"
        
        # --- Filters (WHERE) ---
        if model.prodlot:
            where_filters.append("ds.prodlot ILIKE :prodlot")
            params["prodlot"] = f"%{model.prodlot}%"

        if model.prodid:
            where_filters.append("ds.prodid ILIKE :prodid")
            params["prodid"] = f"%{model.prodid}%"

        if model.prodname:
            where_filters.append("p.prodname ILIKE :prodname")
            params["prodname"] = f"%{model.prodname}%"

        if model.defectid:
            where_filters.append("ds.defectid ILIKE :defectid")
            params["defectid"] = f"%{model.defectid}%"

        if model.defecttype:
            where_filters.append("d.defecttype ILIKE :defecttype")
            params["defecttype"] = f"%{model.defecttype}%"

        where_clause = " WHERE " + " AND ".join(where_filters) if where_filters else ""

        # --- Pagination ---
        page = model.page or 1
        page_size = model.pageSize or 10
        offset = (page - 1) * page_size

        # --- Main Query (with LIMIT) ---
        main_query = f"""
            SELECT ds.summaryid, ds.prodlot, ds.prodid, p.prodname, ds.defectid, d.defecttype, ds.totalprod, ds.totalok, ds.totalng
            FROM defectsummary ds
            left JOIN product p ON p.prodid = ds.prodid 
            left JOIN defecttype d on d.defectid = ds.defectid
            {where_clause}
            ORDER BY {order_by} {order_dir}
            LIMIT :limit OFFSET :offset
        """

        params["limit"] = page_size
        params["offset"] = offset

        # --- Total Count Query ---
        count_query = f"""
            SELECT COUNT(*) FROM (
                SELECT 1
                FROM defectsummary ds
                left JOIN product p ON p.prodid = ds.prodid 
                left JOIN defecttype d on d.defectid = ds.defectid
                {where_clause}
            ) AS count
        """
        # print("SQL Query:", main_query)
        # print("Parameters:", params)
        
        total = self._fetch_one(count_query, params)["count"]
        items = self._fetch_all(main_query, params)

        return {
            "total": total,
            "items": items
        }
    
    def suggest_defect_lotno(self, q: str):
        rows = self._fetch_all("""
            SELECT DISTINCT prodlot FROM defectsummary
            WHERE LOWER(prodlot) LIKE LOWER(:keyword)
            ORDER BY prodlot ASC
            LIMIT 10; """,
            {"keyword": q + "%"}
        )
        return [{"value": row["prodlot"], "label": row["prodlot"]} for row in rows]
    
