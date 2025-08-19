from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from typing import Optional
from datetime import datetime
import database.schemas as schemas


class DashboardService:
    

    @staticmethod
    def _convert_month_to_name(month_str: Optional[str]) -> Optional[str]:
        """แปลง month number ('05') เป็น month name ('May')"""
        if not month_str:
            return None
        
        month_mapping = {
            '01': 'January', '02': 'February', '03': 'March',
            '04': 'April', '05': 'May', '06': 'June',
            '07': 'July', '08': 'August', '09': 'September',
            '10': 'October', '11': 'November', '12': 'December'
        }
        return month_mapping.get(month_str)


    @staticmethod
    def get_total_products(start: datetime, end: datetime, productname: Optional[str], prodline: Optional[str], 
                          cameraid: Optional[str], month: Optional[str], year: Optional[str], db: Session):
        month_name = DashboardService._convert_month_to_name(month)
        
        sql = """
        select
            count(distinct p.prodid) as total_products
        from
            productdefectresult p
        left join planning pl
        on
            p.prodid = pl.prodid
        where
            (:prodid IS NULL OR p.prodid = :prodid)
            and (:cameraid IS NULL OR p.cameraid = :cameraid)
            and (:prodline IS NULL OR pl.prodline = :prodline)
            and ((p.defecttime between :startdatetime and :enddatetime)
                or ((TO_CHAR(p.defecttime, 'FMMonth') ilike :month)
                    AND (extract(year from p.defecttime) = :year))
            )
        """
        
        result = db.execute(text(sql), {
            "prodid": productname,
            "cameraid": cameraid,
            "prodline": prodline,
            "startdatetime": start,
            "enddatetime": end,
            "month": month_name,
            "year": int(year) if year else None
        }).mappings().fetchone()
        
        return [{"total_products": result["total_products"] if result else 0}]

    # Card 2: OK/NG Ratio
    @staticmethod
    def get_ratio(start: datetime, end: datetime, productname: Optional[str], prodline: Optional[str], 
                  cameraid: Optional[str], month: Optional[str], year: Optional[str], db: Session):
        month_name = DashboardService._convert_month_to_name(month)
        
        sql = """
        select
            z.status,
            COUNT(*),
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) over (), 2) as percent_of_total
        from
            (
            select
                pr.defecttime,
                pr.prodid,
                p.prodname,
                pr.prodseq,
                pr.cameraid,
                pr.imagepath,
                case
                    when BOOL_OR(pr.prodstatus = 'NG') then 'NG'
                    else 'OK'
                end as status,
                CONCAT_WS(E'\\n',
                    CONCAT('OK - ', STRING_AGG(pr.defectid, ', ' order by pr.resultid) filter (where pr.prodstatus = 'OK')),
                    CONCAT('NG - ', STRING_AGG(pr.defectid, ', ' order by pr.resultid) filter (where pr.prodstatus = 'NG'))
                ) as defect_summary,
                STRING_AGG(pr.comment, ', ' order by pr.resultid) as comment
            from
                productdefectresult pr
            left join product p on
                p.prodid = pr.prodid
            left join planning pl on
                pl.prodid = pr.prodid
            where
                (:prodid IS NULL OR p.prodid = :prodid)
                and (:cameraid IS NULL OR pr.cameraid = :cameraid)
                and (:prodline IS NULL OR pl.prodline = :prodline)
                and (
                    (pr.defecttime between :startdatetime and :enddatetime)
                    or (
                        TO_CHAR(pr.defecttime, 'FMMonth') ilike :month
                        AND extract(year from pr.defecttime) = :year
                    )
                )
            group by
                pr.cameraid,
                pr.prodid,
                p.prodname,
                pr.prodseq,
                pr.defecttime,
                pr.imagepath
        ) z
        group by
            z.status
        """
        
        result = db.execute(text(sql), {
            "prodid": productname,
            "cameraid": cameraid,
            "prodline": prodline,
            "startdatetime": start,
            "enddatetime": end,
            "month": month_name,
            "year": int(year) if year else None
        }).mappings().fetchall()
        

        ok_data = next((row for row in result if row["status"] == "OK"), None)
        ng_data = next((row for row in result if row["status"] == "NG"), None)
        
        return [{
            "prodname": productname or "All Products",
            "cameraid": cameraid or "All Cameras",
            "prodlot": prodline or "All Lines",
            "line": prodline or "All Lines",
            "total_ok": ok_data["count"] if ok_data else 0,
            "total_ng": ng_data["count"] if ng_data else 0,
            "ok_ratio_percent": ok_data["percent_of_total"] if ok_data else 0,
            "ng_ratio_percent": ng_data["percent_of_total"] if ng_data else 0
        }]

    # Card 3: Trend of Top 5 Defect Types
    @staticmethod
    def top_5_trends(start: datetime, end: datetime, productname: Optional[str], prodline: Optional[str], 
                     cameraid: Optional[str], month: Optional[str], year: Optional[str], db: Session):
        month_name = DashboardService._convert_month_to_name(month)
        
        sql = """
        select
            defectid,
            DATE_TRUNC('hour', defecttime) as hour_group,
            COUNT(*) as ng_count
        from
            productdefectresult pr
        left join planning pl on
            pl.prodid = pr.prodid
        where
            pr.prodstatus = 'NG'
            and (:cameraid IS NULL OR pr.cameraid = :cameraid)
            and (:prodid IS NULL OR pr.prodid = :prodid)
            and (:prodline IS NULL OR pl.prodline = :prodline)
            and (
                pr.defecttime between :startdatetime and :enddatetime
                or (
                    TO_CHAR(pr.defecttime, 'FMMonth') ilike :month
                    AND extract(year from pr.defecttime) = :year
                )
            )
            and pr.defectid in (
            select
                defectid
            from
                (
                select
                    defectid,
                    COUNT(*) as cnt
                from
                    productdefectresult pr2
                left join planning pl2 on
                    pl2.prodid = pr2.prodid
                where
                    pr2.prodstatus = 'NG'
                    and (:cameraid IS NULL OR pr2.cameraid = :cameraid)
                    and (:prodid IS NULL OR pr2.prodid = :prodid)
                    and (:prodline IS NULL OR pl2.prodline = :prodline)
                    and (
                        pr2.defecttime between :startdatetime and :enddatetime
                        or (
                            TO_CHAR(pr2.defecttime, 'FMMonth') ilike :month
                            AND extract(year from pr2.defecttime) = :year
                        )
                    )
                group by
                    defectid
                order by
                    cnt desc
                limit 5
                ) as top5
            )
        group by
            defectid,
            hour_group
        order by
            defectid,
            hour_group
        """
        
        result = db.execute(text(sql), {
            "prodid": productname,
            "cameraid": cameraid,
            "prodline": prodline,
            "startdatetime": start,
            "enddatetime": end,
            "month": month_name,
            "year": int(year) if year else None
        }).mappings().fetchall()
        
        return [{"defecttype": row["defectid"], "line": prodline or "All Lines", 
                "hour_slot": row["hour_group"].isoformat() if row["hour_group"] else "", 
                "quantity": row["ng_count"]} for row in result]

    # Card 4: Top 5 Most Frequent Defect Types
    @staticmethod
    def top_5_defects(start: datetime, end: datetime, productname: Optional[str], prodline: Optional[str], 
                      cameraid: Optional[str], month: Optional[str], year: Optional[str], db: Session):
        month_name = DashboardService._convert_month_to_name(month)
        
        sql = """
        select
            defectid,
            pl.prodline,
            COUNT(*) as ng_count
        from
            productdefectresult pr
        left join planning pl on
            pl.prodid = pr.prodid
        where
            pr.prodstatus = 'NG'
            and (:cameraid IS NULL OR pr.cameraid = :cameraid)
            and (:prodid IS NULL OR pr.prodid = :prodid)
            and (:prodline IS NULL OR pl.prodline = :prodline)
            and (
                pr.defecttime between :startdatetime and :enddatetime
                or (
                    TO_CHAR(pr.defecttime, 'FMMonth') ilike :month
                    or extract(year from pr.defecttime) = :year
                )
            )
            and pr.defectid in (
            select
                defectid
            from
                (
                select
                    defectid,
                    COUNT(*) as cnt
                from
                    productdefectresult pr2
                left join planning pl2 on
                    pl2.prodid = pr2.prodid
                where
                    pr2.prodstatus = 'NG'
                    and (:cameraid IS NULL OR pr2.cameraid = :cameraid)
                    and (:prodid IS NULL OR pr2.prodid = :prodid)
                    and (:prodline IS NULL OR pl2.prodline = :prodline)
                    and (
                        pr2.defecttime between :startdatetime and :enddatetime
                        or (
                            TO_CHAR(pr2.defecttime, 'FMMonth') ilike :month
                            or extract(year from pr2.defecttime) = :year
                        )
                    )
                group by
                    defectid
                order by
                    cnt desc
                limit 5
                ) as top5
            )
        group by
            defectid,
            pl.prodline
        order by
            defectid,
            pl.prodline
        """
        
        result = db.execute(text(sql), {
            "prodid": productname,
            "cameraid": cameraid,
            "prodline": prodline,
            "startdatetime": start,
            "enddatetime": end,
            "month": month_name,
            "year": int(year) if year else None
        }).mappings().fetchall()
        
        return [{"defecttype": row["defectid"], "line": row["prodline"] or "Unknown", 
                "quantity": row["ng_count"], "all_defect_times": []} for row in result]
    
    # Card 5: Defect Distribution by Hour
    @staticmethod
    def ng_distribution(start: datetime, end: datetime, productname: Optional[str], prodline: Optional[str], 
                        cameraid: Optional[str], month: Optional[str], year: Optional[str], db: Session):
        month_name = DashboardService._convert_month_to_name(month)
        
        sql = """
        select
            pr.prodid as prodid,
            p.prodname,
            DATE_TRUNC('hour', pr.defecttime) as hour_group,
            COUNT(*) as ng_count
        from
            productdefectresult pr
        left join planning pl on
            pl.prodid = pr.prodid
        left join product p on
            pl.prodid = p.prodid
        where
            pr.prodstatus = 'NG'
            and (:cameraid IS NULL OR pr.cameraid = :cameraid)
            and (:prodid IS NULL OR pr.prodid = :prodid)
            and (:prodline IS NULL OR pl.prodline = :prodline)
            and (
                pr.defecttime between :startdatetime and :enddatetime
                or (
                    TO_CHAR(pr.defecttime, 'FMMonth') ilike :month
                    AND extract(year from pr.defecttime) = :year
                )
            )
            and pr.defectid in (
            select
                defectid
            from
                (
                select
                    defectid,
                    COUNT(*) as cnt
                from
                    productdefectresult pr2
                left join planning pl2 on
                    pl2.prodid = pr2.prodid
                where
                    pr2.prodstatus = 'NG'
                    and (:cameraid IS NULL OR pr2.cameraid = :cameraid)
                    and (:prodid IS NULL OR pr2.prodid = :prodid)
                    and (:prodline IS NULL OR pl2.prodline = :prodline)
                    and (
                        pr2.defecttime between :startdatetime and :enddatetime
                        or (
                            TO_CHAR(pr2.defecttime, 'FMMonth') ilike :month
                            AND extract(year from pr2.defecttime) = :year
                        )
                    )
                group by
                    defectid
                order by
                    cnt desc
                limit 5
                ) as top5
            )
        group by
            pr.prodid,
            p.prodname,
            hour_group
        order by
            pr.prodid,
            hour_group
        """
        
        result = db.execute(text(sql), {
            "prodid": productname,
            "cameraid": cameraid,
            "prodline": prodline,
            "startdatetime": start,
            "enddatetime": end,
            "month": month_name,
            "year": int(year) if year else None
        }).mappings().fetchall()
        
        return [{
            "defecttype": row["prodid"] or "Unknown",
            "prodname": row["prodname"] or "Unknown",
            "line": prodline or "All Lines",
            "hour_slot": row["hour_group"].isoformat() if row["hour_group"] else "",
            "defect_count": row["ng_count"]
        } for row in result]



    # Card 6: Trend of Top 5 Detection
    @staticmethod
    def get_defects_with_ng_gt_zero(start: datetime, end: datetime, productname: Optional[str], 
                                    prodline: Optional[str], cameraid: Optional[str], month: Optional[str], 
                                    year: Optional[str], db: Session):
        month_name = DashboardService._convert_month_to_name(month)
        
        sql = """
        SELECT
            pr.prodid,
            pr.defectid,
            pr.cameraid,
            pr.defecttime,
            cm.cameraname,
            pl.prodline as line,
            COUNT(*) as totalng
        FROM productdefectresult pr
        LEFT JOIN planning pl ON pl.prodid = pr.prodid
        LEFT JOIN camera cm ON cm.cameraid = pr.cameraid
        WHERE pr.prodstatus = 'NG'
        AND (:productname IS NULL OR pr.prodid = :productname)
        AND (:prodline IS NULL OR pl.prodline = :prodline)
        AND (:cameraid IS NULL OR pr.cameraid = :cameraid)
        AND (
            (pr.defecttime BETWEEN :startdatetime AND :enddatetime)
            OR (
                (:month IS NULL OR TO_CHAR(pr.defecttime, 'FMMonth') ILIKE :month)
                AND (:year IS NULL OR EXTRACT(year FROM pr.defecttime) = :year)
            )
        )
        GROUP BY 
            pr.prodid,
            pr.defectid, 
            pr.cameraid,
            pr.defecttime,
            cm.cameraname,
            pl.prodline
        ORDER BY totalng DESC
        """
        
        result = db.execute(text(sql), {
            "productname": productname,
            "cameraid": cameraid,
            "prodline": prodline,
            "startdatetime": start,
            "enddatetime": end,
            "month": month_name,
            "year": int(year) if year else None
        }).mappings().fetchall()
        
        return [{
            "prodid": row["prodid"] or "Unknown",
            "defectid": row["defectid"] or "Unknown", 
            "defecttype": row["defectid"] or "Unknown",  # defecttype = defectid ใน case นี้
            "cameraid": row["cameraid"] or "Unknown",
            "line": row["line"] or "All Lines",
            "cameraname": row["cameraname"] or "Unknown Camera",
            "totalng": row["totalng"] or 0,
            "defecttime": row["defecttime"].isoformat() if row["defecttime"] else ""
        } for row in result]


    # Dropdown Lists
    @staticmethod
    def get_products_list(db: Session):
        sql = """
        SELECT DISTINCT 
            p.prodid as id,
            p.prodid as name
        FROM productdefectresult pdr
        LEFT JOIN product p ON p.prodid = pdr.prodid 
        WHERE p.prodid IS NOT NULL 
        ORDER BY p.prodid
        """
        result = db.execute(text(sql)).mappings().fetchall()
        return [{"id": row["id"], "name": row["name"]} for row in result]

    @staticmethod
    def get_cameras_list(db: Session):
        """ดึงรายการ cameras สำหรับ dropdown filter"""
        sql = """
        SELECT DISTINCT 
            c.cameraid as id,
            c.cameraid as name
        FROM camera c
        WHERE c.cameraid IS NOT NULL 
        ORDER BY c.cameraid
        """
        result = db.execute(text(sql)).mappings().fetchall()
        return [{"id": row["id"], "name": row["name"]} for row in result]

    @staticmethod
    def get_lines_list(db: Session):
        """ดึงรายการ production lines สำหรับ dropdown filter"""
        sql = """
        SELECT DISTINCT 
            pl.prodline as id,
            pl.prodline as name
        FROM planning pl
        WHERE pl.prodline IS NOT NULL 
        ORDER BY pl.prodline
        """
        result = db.execute(text(sql)).mappings().fetchall()
        return [{"id": row["id"], "name": row["name"]} for row in result]