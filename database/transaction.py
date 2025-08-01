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

        # --- Allowed fields for sorting ---
        allowed_order_fields = [ "actualstartdatetime", "actualenddatetime", "prodlot", "prodid", "prodname", "quantity" ]
        order_by = model.order_by if model.order_by in allowed_order_fields else "actualstartdatetime"
        order_dir = "DESC" if str(model.order_dir).lower() == "desc" else "ASC"

        # --- Helper for adding filters ---
        def add_filter(condition, key, value):
            if value is not None:
                where_filters.append(condition)
                params[key] = value

        add_filter("t.actualstartdatetime >= :startdate", "startdate", model.startdate)
        add_filter("t.actualenddatetime <= :enddate", "enddate", model.enddate)
        add_filter("t.prodlot ILIKE :prodlot", "prodlot", f"%{model.prodlot}%" if model.prodlot else None)
        add_filter("t.prodid ILIKE :prodid", "prodid", f"%{model.prodid}%" if model.prodid else None)
        add_filter("p.prodname ILIKE :prodname", "prodname", f"%{model.prodname}%" if model.prodname else None)

        where_clause = f"WHERE {' AND '.join(where_filters)}" if where_filters else ""

        # --- Pagination ---
        page = max(model.page or 1, 1)
        page_size = min(max(model.pageSize or 10, 1), 100) 
        offset = (page - 1) * page_size
        params["limit"] = page_size
        params["offset"] = offset

        # --- Base Select ---
        base_query = """
            FROM transactionreport t
            LEFT JOIN product p ON p.prodid = t.prodid
        """

        # --- Main Query ---
        main_query = f"""
            SELECT
                t.transactionid, t.prodlot, t.prodid,
                p.prodname, t.actualstartdatetime,
                t.actualenddatetime, t.quantity
            {base_query}
            {where_clause}
            ORDER BY {order_by} {order_dir}
            LIMIT :limit OFFSET :offset
        """

        # --- Count Query ---
        count_query = f"""
            SELECT COUNT(*) AS count
            FROM (
                SELECT 1
                {base_query}
                {where_clause}
            ) AS subquery
        """

        # --- Execute ---
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
