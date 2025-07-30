from database.connect_to_db import engine, Session, text, SQLAlchemyError
from fastapi import HTTPException
import database.schemas as schemas
from datetime import datetime

class TransactionDB:
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

    def get_transaction(self, model: schemas.TransactionSearch):
        where_filters = []
        params = {}

        allowed_order_fields = [ "actualstartdatetime", "actualenddatetime", "prodlot", "prodid", "prodname", "quantity" ]

        order_by = model.order_by if model.order_by in allowed_order_fields else "actualstartdatetime"
        order_dir = "DESC" if model.order_dir.lower() == "desc" else "ASC"

        if model.startdate:
            where_filters.append("t.actualstartdatetime >= :startdate")
            params["startdate"] = model.startdate

        if model.enddate:
            where_filters.append("t.actualenddatetime <= :enddate")
            params["enddate"] = model.enddate

        if model.prodlot:
            where_filters.append("t.prodlot ILIKE :prodlot")
            params["prodlot"] = f"%{model.prodlot}%"

        if model.prodid:
            where_filters.append("t.prodid ILIKE :prodid")
            params["prodid"] = f"%{model.prodid}%"

        if model.prodname:
            where_filters.append("p.prodname ILIKE :prodname")
            params["prodname"] = f"%{model.prodname}%"

        where_clause = " WHERE " + " AND ".join(where_filters) if where_filters else ""

        # --- Pagination ---
        page = model.page or 1
        page_size = model.pageSize or 10
        offset = (page - 1) * page_size

        # --- Main Query (with LIMIT) ---
        main_query = f"""
            SELECT t.transactionid, t.prodlot, t.prodid, p.prodname, t.actualstartdatetime, t.actualenddatetime, t.quantity
            FROM transactionreport t
            LEFT JOIN product p ON p.prodid = t.prodid 
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
                FROM transactionreport t
                LEFT JOIN product p ON p.prodid = t.prodid 
                {where_clause}
            ) AS count
        """

        print("SQL Query:", main_query)
        # print("Parameters:", params)

        total = self._fetch_one(count_query, params)["count"]
        items = self._fetch_all(main_query, params)

        return {
            "total": total,
            "items": items
        }
    
    def suggest_transaction_lotno(self, q: str):
        rows = self._fetch_all("""
            SELECT DISTINCT prodlot FROM transactionreport
            WHERE LOWER(prodlot) LIKE LOWER(:keyword)
            ORDER BY prodlot ASC
            LIMIT 10; """,
            {"keyword": q + "%"}
        )
        return [{"value": row["prodlot"], "label": row["prodlot"]} for row in rows]
