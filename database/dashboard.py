# from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from typing import Optional
from datetime import datetime
import database.schemas as schemas
# from database.connect_to_db import get_db

class DashboardService:
    @staticmethod
    def get_defects_with_ng_gt_zero(start: datetime, end: datetime, db: Session):
        sql = """
        SELECT 
            pdr.prodid,
            pdr.defectid,
            dt.defecttype,
            pdr.cameraid,
            ds.prodlot AS LINE,
            cam.cameraname,
            COALESCE(ds.totalng, 0) AS totalng,
            pdr.defecttime
        FROM productdefectresult pdr
        LEFT JOIN defecttype dt ON pdr.defectid = dt.defectid
        LEFT JOIN camera cam ON pdr.cameraid = cam.cameraid
        LEFT JOIN defectsummary ds ON pdr.prodid = ds.prodid AND pdr.defectid = ds.defectid
        WHERE COALESCE(ds.totalng, 0) > 0
          AND pdr.defecttime BETWEEN :start AND :end
        """
        result = db.execute(text(sql), {
            "start": start,
            "end": end
        }).mappings().fetchall()
        return result

    @staticmethod
    def get_ratio(start: datetime, end: datetime, productname: Optional[str], prodline: Optional[str], cameraid: Optional[str], db: Session):
        sql = """
        SELECT
            p.prodname,
            p.cameraid,
            ds.prodlot as line,
            ds.total_ok,
            ds.total_ng,
            ROUND(ds.total_ok::numeric * 100 / NULLIF(ds.total_ok + ds.total_ng, 0), 2) AS ok_ratio_percent,
            ROUND(ds.total_ng::numeric * 100 / NULLIF(ds.total_ok + ds.total_ng, 0), 2) AS ng_ratio_percent
        FROM (
            SELECT
                prodid,
                prodlot,
                SUM(totalok) AS total_ok,
                SUM(totalng) AS total_ng
            FROM defectsummary
            GROUP BY prodid, prodlot
        ) ds
        INNER JOIN (
            SELECT DISTINCT pd.prodid, p.prodname, pd.cameraid
            FROM productdefectresult pd
            LEFT JOIN product p on p.prodid = pd.prodid
            WHERE pd.defecttime BETWEEN :start AND :end
        ) p ON ds.prodid = p.prodid
        """
        return db.execute(text(sql), {
            "start": start,
            "end": end,
            "prodlot": prodline,
            "productname": productname,
            "cameraid": cameraid
        }).mappings().fetchall()

    @staticmethod
    def ng_distribution(start: datetime, end: datetime, productname: Optional[str], prodline: Optional[str], cameraid: Optional[str], db: Session):
        sql = """
            SELECT 
                d.defecttype,
                p.prodname,
                ds.prodlot as line,
                DATE_TRUNC('hour', pdr.defecttime) AS hour_slot,
                COUNT(*) AS defect_count
            FROM productdefectresult pdr
              LEFT JOIN defectsummary ds ON pdr.prodid = ds.prodid
              LEFT JOIN product p ON p.prodid = pdr.prodid 
              LEFT JOIN defecttype d on d.defectid = pdr.defectid
            WHERE pdr.defecttime BETWEEN :start AND :end
              AND (:productname IS NULL OR p.prodname = :productname)
              AND (:prodline IS NULL OR ds.prodlot = :prodline)
              AND (:cameraid IS NULL OR pdr.cameraid = :cameraid)
            GROUP BY d.defecttype, hour_slot, p.prodname, ds.prodlot
            ORDER BY hour_slot, d.defecttype;
        """
        return db.execute(text(sql), {
            "start": start,
            "end": end,
            "productname": productname,
            "prodline": prodline,
            "cameraid": cameraid
        }).mappings().fetchall()

    @staticmethod
    def top_5_defects(start: datetime, end: datetime, productname: Optional[str], prodline: Optional[str], cameraid: Optional[str], db: Session):
        sql = """
        SELECT
            d.defecttype,
            ds.prodlot AS line,
            COUNT(*) AS quantity,
            ARRAY_AGG(pdr.defecttime ORDER BY pdr.defecttime) AS all_defect_times
        FROM productdefectresult pdr
        LEFT JOIN defectsummary ds ON pdr.prodid = ds.prodid 
        LEFT JOIN product p ON p.prodid = pdr.prodid 
        LEFT JOIN defecttype d on d.defectid = pdr.defectid
            WHERE pdr.defecttime BETWEEN :start AND :end
              AND (:productname IS NULL OR p.prodname = :productname)
              AND (:prodlot IS NULL OR ds.prodlot = :prodlot)
              AND (:cameraid IS NULL OR pdr.cameraid = :cameraid)
            GROUP BY d.defecttype, ds.prodlot
            ORDER BY quantity DESC
            LIMIT 5;
        """
        return db.execute(text(sql), {
            "start": start,
            "end": end,
            "prodlot": prodline,
            "productname": productname,
            "cameraid": cameraid
        }).mappings().fetchall()

    @staticmethod
    def top_5_trends(start: datetime, end: datetime, db: Session):
        sql = """
        SELECT 
            d.defecttype,
            ds.prodlot as line,
            DATE_TRUNC('hour', pdr.defecttime) AS hour_slot,
            COUNT(*) AS quantity
        FROM productdefectresult pdr
	        LEFT JOIN defectsummary ds ON pdr.prodid = ds.prodid 
	        LEFT JOIN defecttype d on d.defectid = pdr.defectid
        WHERE pdr.defecttime BETWEEN :start AND :end
	        AND d.defecttype IN (
	            SELECT d2.defecttype
	            FROM productdefectresult pdr2
		            LEFT JOIN defectsummary ds2 ON pdr2.prodid = ds2.prodid 
		            LEFT JOIN defecttype d2 on d2.defectid = pdr2.defectid
	            WHERE pdr2.defecttime BETWEEN :start AND :end
	            GROUP BY d2.defecttype
	            ORDER BY COUNT(*) DESC
	            LIMIT 5
	        )
        GROUP BY d.defecttype, hour_slot, ds.prodlot
        ORDER BY hour_slot, d.defecttype;
        """
        result = db.execute(text(sql), {
            "start": start,
            "end": end
        }).mappings().fetchall()
        return result
    
    @staticmethod
    def get_total_products(start: datetime, end: datetime, productname: Optional[str], prodline: Optional[str], cameraid: Optional[str], db: Session):
        sql = """
        SELECT 
            COUNT(DISTINCT pdr.prodid) as total_products
        FROM productdefectresult pdr
        LEFT JOIN defectsummary ds ON pdr.prodid = ds.prodid
        LEFT JOIN product p ON p.prodid = pdr.prodid 
        WHERE pdr.defecttime BETWEEN :start AND :end
          AND (:productname IS NULL OR p.prodname = :productname)
          AND (:prodline IS NULL OR ds.prodlot = :prodline)
          AND (:cameraid IS NULL OR pdr.cameraid = :cameraid)
        """
        result = db.execute(text(sql), {
            "start": start,
            "end": end,
            "prodline": prodline,
            "productname": productname,
            "cameraid": cameraid
        }).mappings().fetchone()
        
        # Return ในรูปแบบ array เพื่อให้ตรงกับ frontend expectation
        return [{"total_products": result["total_products"] if result else 0}]
    
    @staticmethod
    def get_lines_list(db: Session):
        """ดึงรายการ production lines สำหรับ dropdown filter"""
        sql = """
        SELECT DISTINCT 
            ds.prodlot as id,
            ds.prodlot as name
        FROM public.defectsummary ds
        WHERE ds.prodlot IS NOT NULL 
        ORDER BY ds.prodlot
        """
        result = db.execute(text(sql)).mappings().fetchall()
        return [{"id": row["id"], "name": row["name"]} for row in result]

    @staticmethod
    def get_products_list(db: Session):
        """ดึงรายการ products สำหรับ dropdown filter"""
        sql = """
        SELECT DISTINCT 
            p.prodname as id,
            p.prodname as name
        FROM productdefectresult pdr
        LEFT JOIN product p ON p.prodid = pdr.prodid 
        WHERE p.prodname IS NOT NULL 
        ORDER BY p.prodname
        """
        result = db.execute(text(sql)).mappings().fetchall()
        return [{"id": row["id"], "name": row["name"]} for row in result]

    @staticmethod
    def get_cameras_list(db: Session):
        """ดึงรายการ cameras สำหรับ dropdown filter"""
        sql = """
        SELECT DISTINCT 
            c.cameraid as id,
            COALESCE(c.cameraname, c.cameraid) as name
        FROM public.camera c
        WHERE c.cameraid IS NOT NULL 
        ORDER BY c.cameraid
        """
        result = db.execute(text(sql)).mappings().fetchall()
        return [{"id": row["id"], "name": row["name"]} for row in result]
    # ----------------------------------------------------------------------------------
    
    @staticmethod
    def test_top_5_trends(filter: schemas.DashboardFilter, db: Session):
        sql = """
        SELECT 
            d.defecttype,
            ds.prodlot as line,
            DATE_TRUNC('hour', pdr.defecttime) AS hour_slot,
            COUNT(*) AS quantity
        FROM productdefectresult pdr
        LEFT JOIN defectsummary ds ON pdr.prodid = ds.prodid 
        LEFT JOIN defecttype d on d.defectid = pdr.defectid
        WHERE pdr.defecttime BETWEEN :start AND :end
        AND d.defecttype IN (
            SELECT pdr2.defecttype
            FROM productdefectresult pdr2
            LEFT JOIN defectsummary ds2 ON pdr2.prodid = ds2.prodid 
            LEFT JOIN defecttype d2 on d2.defectid = pdr2.defectid
            WHERE d2.defecttime BETWEEN :start AND :end
            GROUP BY d2.defecttype
            ORDER BY COUNT(*) DESC
            LIMIT 5
        )
        GROUP BY d.defecttype, hour_slot, ds.prodlot
        ORDER BY hour_slot, d.defecttype;
        """
        result = db.execute(text(sql), {
            "start": filter.start,
            "end": filter.end
        }).mappings().fetchall()
        return result
