from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from database import SessionLocal, TransactionDB
from database.schemas import TransactionSearch

router = APIRouter(tags=["Transaction"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/transaction")
def transaction(model: TransactionSearch = Depends()):
    try:
        return TransactionDB().get_transaction(model)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
  
@router.get("/export-transaction")
def export_transaction(model: TransactionSearch = Depends()):
    try:
        return TransactionDB().export_transaction(model)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/suggest-transaction-lotno")
def suggest_transaction_lotno(q: str):
    try:
        return TransactionDB().suggest_transaction_lotno(q)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        