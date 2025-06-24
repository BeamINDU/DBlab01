from database.connect_to_db import engine, Session, text, SQLAlchemyError
from fastapi import HTTPException
import database.schemas as schemas
from datetime import datetime

class TransactionDB:
    def _fetch_all(self, query: str):
        try:
            with engine.connect() as conn:
                result = conn.execute(text(query))
                return [dict(row) for row in result.mappings()]
        except SQLAlchemyError as e:
            print(f"Database error: {e}")
            return []

    def get_transaction(self):
        return self._fetch_all("SELECT * FROM public.transactionreport")

    def add_transaction(self, txn: schemas.TransactionCreate, db: Session):
        if db.execute(text("SELECT 1 FROM transaction WHERE runningno = :runningno"),
                      {"runningno": txn.runningno}).first():
            raise HTTPException(status_code=400, detail="Transaction runningNo already exists")

        insert_sql = text("""
            INSERT INTO transaction (
                runningno, startdate, enddate, lotno, productid, quantity,
                createdby, createddate, updatedby, updateddate
            ) VALUES (
                :runningno, :startdate, :enddate, :lotno, :productid, :quantity,
                :createdby, :createddate, :updatedby, :updateddate
            )
        """)

        db.execute(insert_sql, {
            "runningno": txn.runningno,
            "startdate": txn.startdate,
            "enddate": txn.enddate,
            "lotno": txn.lotno,
            "productid": txn.productid,
            "quantity": txn.quantity,
            "createdby": txn.createdby,
            "createddate": txn.createddate or datetime.now(),
            "updatedby": txn.updatedby,
            "updateddate": txn.updateddate
        })
        db.commit()
        return {"status": "Transaction created", "runningNo": txn.runningno}

    def update_transaction(self, runningno: int, txn: schemas.TransactionUpdate, db: Session):
        if not db.execute(text("SELECT 1 FROM transaction WHERE runningno = :runningno"),
                          {"runningno": runningno}).first():
            raise HTTPException(status_code=404, detail="Transaction not found")

        update_fields = {}

        if txn.startdate: update_fields["startdate"] = txn.startdate
        if txn.enddate: update_fields["enddate"] = txn.enddate
        if txn.lotno: update_fields["lotno"] = txn.lotno
        if txn.productid: update_fields["productid"] = txn.productid
        if txn.quantity is not None: update_fields["quantity"] = txn.quantity
        if txn.updatedby: update_fields["updatedby"] = txn.updatedby

        update_fields["updateddate"] = txn.updateddate or datetime.now()
        update_fields["runningno"] = runningno

        if not update_fields:
            raise HTTPException(status_code=400, detail="No fields to update")

        set_clause = ", ".join([f"{key} = :{key}" for key in update_fields if key != "runningno"])
        update_sql = text(f"UPDATE transaction SET {set_clause} WHERE runningno = :runningno")

        db.execute(update_sql, update_fields)
        db.commit()
        return {"status": "Transaction updated", "runningNo": runningno}

