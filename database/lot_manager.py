import asyncio
import pytz
from pytz import UTC
import logging
from datetime import datetime, timedelta
from sqlalchemy import text
from database.connect_to_db import SessionLocal

logging.basicConfig(level=logging.INFO)

bangkok = pytz.timezone("Asia/Bangkok")

# Shared buffer for today lots
lots_today = []

async def reload_lots_today():
    """
    Manually reload lots_today and close any expired lots.
    Call this after user updates planning in the database.
    """
    global lots_today
    lots_today = load_today_lots()
    now = datetime.now(bangkok)
    for lot in lots_today:
        logging.info(f"[RELOAD DEBUG] {lot['prodlot']}: now={now}, end={lot['enddatetime']}")
    await close_all_expired_lots()
    logging.info(f"[LOT MANAGER] Manually reloaded lots_today at {datetime.now(bangkok)}")

async def manage_lot_by_planid_and_action(planid: str, action: str):
    """
    Run start or end management for all lots with given planid and action ('start'/'stop').
    """
    action = action.lower()
    for lot in lots_today:
        if str(lot['planid']) == planid:
            if action == "start":
                if not lot["started"]:
                    logging.info(f"[LOT MANAGER] Manually starting lot {lot['prodlot']} for planid={planid}")
                    await handle_lot_start(lot)
                    lot["started"] = True
            elif action in ["stop", "end"]:
                if not lot["ended"]:
                    logging.info(f"[LOT MANAGER] Manually ending lot {lot['prodlot']} for planid={planid}")
                    await handle_lot_end(lot)
                    lot["ended"] = True
            else:
                logging.warning(f"[LOT MANAGER] Unknown action '{action}' for planid={planid}")
            # logging.info(f"[RELOAD LOTS] lots today: {lots_today} action={action} planid={planid}")

async def manage_all_lots():
    """
    Run start/end management for all lots.
    """
    now = datetime.now(bangkok)
    for lot in lots_today:
        # Start lot if within time window and not started
        if (
            lot['startdatetime'] is not None and
            (lot['enddatetime'] is None or now < lot['enddatetime']) and
            now >= lot['startdatetime'] and
            not lot["started"]
        ):
            logging.info(f"[LOT MANAGER] Starting lot {lot['prodlot']}")
            await handle_lot_start(lot)
            lot["started"] = True

        # End lot if past enddatetime and not ended
        if (
            lot['enddatetime'] is not None and
            now > lot['enddatetime'] and
            not lot["ended"]
        ):
            logging.info(f"[LOT MANAGER] Ending lot {lot['prodlot']}")
            await handle_lot_end(lot)
            lot["ended"] = True

def load_today_lots():
    """
    Load all lots for today. Use only planningseq.actualstartdatetime and actualenddatetime.
    If no planningseq row, the lot is ignored for today.
    """
    db = SessionLocal()
    try:
        now = datetime.now(bangkok)
        today = now.date()
        tomorrow = today + timedelta(days=1)

        # Main SQL: Use ONLY actualstartdatetime/actualenddatetime from planningseq.
        sql = """
            SELECT
                p.planid,
                s.actualstartdatetime,
                s.actualenddatetime,
                p.prodid,
                pr.prodname,
                pr.prodserial,
                pr.createddate,
                p.prodlot,
                p.prodline,
                p.quantity
            FROM planning p
            JOIN product pr ON p.prodid = pr.prodid
            JOIN (
                SELECT DISTINCT ON (planid, prodid, prodlot, prodline)
                    planid,
                    prodid,
                    prodlot,
                    prodline,
                    seq_no,
                    actualstartdatetime,
                    actualenddatetime
                FROM planningseq
                WHERE actualstartdatetime IS NOT NULL
                ORDER BY planid, prodid, prodlot, prodline, seq_no DESC
            ) s
              ON p.planid = s.planid
             AND p.prodid = s.prodid
             AND p.prodlot = s.prodlot
             AND p.prodline = s.prodline
            WHERE p.isdeleted = false
              AND s.actualstartdatetime >= :today 
          	  and s.actualenddatetime IS NULL
             
            ORDER BY s.actualstartdatetime
        """

        rows = db.execute(
            text(sql), {"today": today, "tomorrow": tomorrow}
        ).mappings().all()

        lots = []
        for r in rows:
            cam_rows = db.execute(
                text("SELECT cameraid FROM cameramodelprodapplied WHERE prodid=:prodid AND appliedstatus=true"),
                {"prodid": r["prodid"]}
            ).mappings().all()
            cameraids = [c["cameraid"] for c in cam_rows]

            dt_start = r.get("actualstartdatetime")
            dt_end = r.get("actualenddatetime")  # None means open lot

            converted_start = dt_start.replace(tzinfo=UTC).astimezone(bangkok) if dt_start else None
            converted_end = dt_end.replace(tzinfo=UTC).astimezone(bangkok) if dt_end else None

            if converted_start is None:
                continue  # Should not happen, but just in case

            # Defensive: skip invalid where end < start
            if converted_end and converted_end < converted_start:
                logging.warning(
                    f"[LOT MANAGER] Skipped lot with end < start: prodlot={r['prodlot']}, start={converted_start}, end={converted_end}")
                continue

            lots.append({
                "planid": r["planid"],
                "prodlot": r["prodlot"],
                "prodid": r["prodid"],
                "startdatetime": converted_start,
                "enddatetime": converted_end,
                "quantity": r["quantity"],
                "prodname": r["prodname"],
                "prodserial": r["prodserial"],
                "createddate": r["createddate"],
                "prodline": r.get("prodline"),
                "started": False,
                "ended": False,
                "cameraids": cameraids,
                # Only actual* datetimes now.
            })
            logging.info(
                f"[LOT MANAGER] Lot loaded: prodlot={r['prodlot']} now={now}, "
                f"start={converted_start} end={converted_end} cameraids={cameraids}"
            )
        return lots
    finally:
        db.close()

def update_and_insert_defectsummary(prodlot: str):
    db = SessionLocal()
    try:
        # --- UPDATE ---
        db.execute(text("""
            UPDATE defectsummary ds
            SET
                totalok = agg.totalok,
                totalng = agg.totalng,
                totalprod = agg.totalprod
            FROM (
                WITH prodseq_summary AS (
                    SELECT 
                        p.prodlot,
                        pr.prodid,
                        pr.prodseq,
                        MAX(CASE WHEN pr.prodstatus = 'NG' THEN 1 ELSE 0 END) = 1 AS has_ng
                    FROM productdefectresult pr
                    JOIN planning p ON pr.prodid = p.prodid
                        AND pr.defecttime AT TIME ZONE 'Asia/Bangkok' AT TIME ZONE 'UTC' BETWEEN p.startdatetime AND p.enddatetime
                    WHERE p.prodlot = :prodlot
                    GROUP BY p.prodlot, pr.prodid, pr.prodseq
                ),
                defect_ng AS (
                    SELECT
                        p.prodlot,
                        pr.defectid,
                        pr.prodid,
                        pr.prodseq,
                        MAX(CASE WHEN pr.prodstatus = 'NG' THEN 1 ELSE 0 END) = 1 AS has_ng
                    FROM productdefectresult pr
                    JOIN planning p ON pr.prodid = p.prodid
                        AND pr.defecttime AT TIME ZONE 'Asia/Bangkok' AT TIME ZONE 'UTC' BETWEEN p.startdatetime AND p.enddatetime
                    WHERE p.prodlot = :prodlot
                    GROUP BY p.prodlot, pr.defectid, pr.prodid, pr.prodseq
                ),
                prodid_ok_total AS (
                    SELECT
                        s.prodlot,
                        s.prodid,
                        COUNT(*) AS totalprod,
                        COUNT(*) FILTER (WHERE has_ng = false) AS totalok
                    FROM prodseq_summary s
                    GROUP BY s.prodlot, s.prodid
                )
                SELECT
                    d.prodlot,
                    d.defectid,
                    d.prodid,
                    COUNT(*) FILTER (WHERE d.has_ng = true) AS totalng,
                    t.totalok,
                    t.totalprod
                FROM defect_ng d
                JOIN prodid_ok_total t ON d.prodlot = t.prodlot AND d.prodid = t.prodid
                GROUP BY d.prodlot, d.defectid, d.prodid, t.totalok, t.totalprod
            ) agg
            WHERE ds.prodlot = agg.prodlot
              AND ds.defectid = agg.defectid
              AND ds.prodid = agg.prodid;
        """), {"prodlot": prodlot})

        # --- INSERT ---
        db.execute(text("""
            INSERT INTO defectsummary (prodlot, totalok, totalng, totalprod, defectid, prodid)
            WITH prodseq_summary AS (
                SELECT 
                    p.prodlot,
                    pr.prodid,
                    pr.prodseq,
                    MAX(CASE WHEN pr.prodstatus = 'NG' THEN 1 ELSE 0 END) = 1 AS has_ng
                FROM productdefectresult pr
                JOIN planning p ON pr.prodid = p.prodid
                    AND pr.defecttime AT TIME ZONE 'Asia/Bangkok' AT TIME ZONE 'UTC' BETWEEN p.startdatetime AND p.enddatetime
                WHERE p.prodlot = :prodlot
                GROUP BY p.prodlot, pr.prodid, pr.prodseq
            ),
            defect_ng AS (
                SELECT
                    p.prodlot,
                    pr.defectid,
                    pr.prodid,
                    pr.prodseq,
                    MAX(CASE WHEN pr.prodstatus = 'NG' THEN 1 ELSE 0 END) = 1 AS has_ng
                FROM productdefectresult pr
                JOIN planning p ON pr.prodid = p.prodid
                    AND pr.defecttime AT TIME ZONE 'Asia/Bangkok' AT TIME ZONE 'UTC' BETWEEN p.startdatetime AND p.enddatetime
                WHERE p.prodlot = :prodlot
                GROUP BY p.prodlot, pr.defectid, pr.prodid, pr.prodseq
            ),
            prodid_ok_total AS (
                SELECT
                    s.prodlot,
                    s.prodid,
                    COUNT(*) AS totalprod,
                    COUNT(*) FILTER (WHERE has_ng = false) AS totalok
                FROM prodseq_summary s
                GROUP BY s.prodlot, s.prodid
            )
            SELECT
                d.prodlot,
                t.totalok,
                COUNT(*) FILTER (WHERE d.has_ng = true) AS totalng,
                t.totalprod,
                d.defectid,
                d.prodid
            FROM defect_ng d
            JOIN prodid_ok_total t ON d.prodlot = t.prodlot AND d.prodid = t.prodid
            GROUP BY d.prodlot, d.defectid, d.prodid, t.totalok, t.totalprod
            HAVING NOT EXISTS (
                SELECT 1 FROM defectsummary ds
                WHERE ds.prodlot = d.prodlot
                  AND ds.defectid = d.defectid
                  AND ds.prodid = d.prodid
            );
        """), {"prodlot": prodlot})

        db.commit()
        logging.info(f"[DEFECTSUMMARY] Aggregated for prodlot={prodlot}")

    except Exception as e:
        logging.error(f"[ERROR] update_and_insert_defectsummary: {e}")
    finally:
        db.close()

async def handle_lot_start(lot):
    """Insert into transactionreport when lot starts if not already exists and camera mapping is valid."""
    db = SessionLocal()
    try:
        # Check camera mapping
        cam_sql = text("""
            SELECT 1
            FROM cameramodelprodapplied cm
            JOIN planning p ON p.prodid = cm.prodid
            WHERE p.prodlot = :prodlot
              AND cm.appliedstatus = true
            LIMIT 1
        """)
        cam_result = db.execute(cam_sql, {"prodlot": lot["prodlot"]}).first()
        if not cam_result:
            logging.info(f"[LOT MANAGER] No camera mapping, skip start for prodlot={lot['prodlot']}")
            return

        # Check already inserted
        exist_sql = text("""
            SELECT 1 FROM transactionreport
            WHERE prodlot = :prodlot
            LIMIT 1
        """)
        already = db.execute(exist_sql, {"prodlot": lot["prodlot"]}).first()
        if already:
            logging.info(f"[LOT MANAGER] Already started for prodlot={lot['prodlot']}")
            return

        # Use latest planning row for prodid if duplicates
        planning_sql = text("""
            SELECT
                p.planid,
                p.prodid,
                s.actualstartdatetime,
                s.actualenddatetime,
                p.startdatetime AS planstartdate,
                p.enddatetime AS planenddate
            FROM planning p
            LEFT JOIN (
                SELECT DISTINCT ON (planid, prodid, prodlot, prodline)
                    planid,
                    prodid,
                    prodlot,
                    prodline,
                    seq_no,
                    actualstartdatetime,
                    actualenddatetime
                FROM planningseq
                ORDER BY planid, prodid, prodlot, prodline, seq_no DESC
            ) s ON
                p.planid = s.planid AND
                p.prodid = s.prodid AND
                p.prodlot = s.prodlot AND
                p.prodline = s.prodline
            WHERE p.prodlot = :prodlot
            ORDER BY COALESCE(s.actualstartdatetime, p.startdatetime) DESC
            LIMIT 1
        """)
        p = db.execute(planning_sql, {"prodlot": lot["prodlot"]}).mappings().first()
        if not p:
            logging.info(f"[LOT MANAGER] Planning not found for prodlot={lot['prodlot']}")
            return
        transaction_id = f"T_{lot['prodlot']}"
        now = datetime.now(bangkok)
        insert_sql = text("""
            INSERT INTO transactionreport (
                transactionid,
                actualstartdatetime,
                prodlot,
                prodid
            ) VALUES (
                :transactionid,
                :actualstartdatetime,
                :prodlot,
                :prodid
            )
        """)
        db.execute(insert_sql, {
            "transactionid": transaction_id,
            "actualstartdatetime": now,
            "prodlot": lot["prodlot"],
            "prodid": p['prodid']
        })
        db.commit()
        logging.info(f"[LOT MANAGER] Inserted transaction for prodlot={lot['prodlot']}")

    except Exception as e:
        logging.error(f"[ERROR] handle_lot_start: {e}")
    finally:
        db.close()

async def handle_lot_end(lot):
    """Set end datetime for transactionreport, update defectsummary. Do not touch quantity."""
    db = SessionLocal()
    try:
        tr_sql = text("""
            SELECT transactionid, prodlot, prodid
            FROM transactionreport
            WHERE prodlot = :prodlot
              AND actualenddatetime IS NULL
            LIMIT 1
        """)
        tr = db.execute(tr_sql, {"prodlot": lot["prodlot"]}).mappings().first()

        if not tr:
            tr_any = db.execute(text("""
                SELECT transactionid, prodlot, prodid
                FROM transactionreport
                WHERE prodlot = :prodlot
                LIMIT 1
            """), {"prodlot": lot["prodlot"]}).mappings().first()
            if not tr_any:
                transaction_id = f"T_{lot['prodlot']}"
                db.execute(text("""
                    INSERT INTO transactionreport (
                        transactionid,
                        actualstartdatetime,
                        prodlot,
                        prodid
                    ) VALUES (
                        :transactionid,
                        :actualstartdatetime,
                        :prodlot,
                        :prodid
                    )
                """), {
                    "transactionid": transaction_id,
                    "actualstartdatetime": lot["startdatetime"],
                    "prodlot": lot["prodlot"],
                    "prodid": lot["prodid"]
                })
                db.commit()
                logging.info(f"[LOT MANAGER] Inserted late transaction for prodlot={lot['prodlot']}")
                tr = {"transactionid": transaction_id, "prodid": lot["prodid"]}
            else:
                logging.info(f"[LOT MANAGER] Already closed transaction for prodlot={lot['prodlot']}")
                return

        transactionid = tr['transactionid']
        now = datetime.now(bangkok)

        # set actualenddatetime
        update_sql = text("""
            UPDATE transactionreport
            SET actualenddatetime = :now
            WHERE transactionid = :transactionid
        """)
        db.execute(update_sql, {
            "now": now,
            "transactionid": transactionid
        })
        db.commit()
        logging.info(f"[LOT MANAGER] Closed lot={lot['prodlot']} (set end time)")

        # Update defectsummary for this lot
        update_and_insert_defectsummary(lot["prodlot"])

    except Exception as e:
        logging.error(f"[ERROR] handle_lot_end: {e}")
    finally:
        db.close()

async def lot_management_loop():
    global lots_today
    while True:
        now = datetime.now(bangkok)
        for lot in lots_today:
            # Start lot if within time window and not started
            if (
                lot['startdatetime'] is not None and
                (lot['enddatetime'] is None or now < lot['enddatetime']) and
                now >= lot['startdatetime'] and
                not lot["started"]
            ):
                logging.info(f"[LOT MANAGER] Starting lot {lot['prodlot']}")
                await handle_lot_start(lot)
                lot["started"] = True

            # End lot if past enddatetime and not ended
            if (
                lot['enddatetime'] is not None and
                now > lot['enddatetime'] and
                not lot["ended"]
            ):
                logging.info(f"[LOT MANAGER] Ending lot {lot['prodlot']}")
                await handle_lot_end(lot)
                lot["ended"] = True

        await asyncio.sleep(1)

async def daily_reload_loop():
    """Reload lots_today at midnight Bangkok time."""
    global lots_today
    while True:
        now = datetime.now(bangkok)
        tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        seconds_until_midnight = (tomorrow - now).total_seconds()
        logging.info(f"[LOT MANAGER] Sleeping {seconds_until_midnight:.0f}s until midnight for reload")
        await asyncio.sleep(seconds_until_midnight)
        lots_today = load_today_lots()
        await close_all_expired_lots() 
        logging.info(f"[LOT MANAGER] Reloaded lots_today for {tomorrow.date()}")

async def close_all_expired_lots():
    now = datetime.now(bangkok)
    logging.info(f"[CLOSE EXPIRED] Called at {now}, {len(lots_today)} lots in cache. ")
    for lot in lots_today:
        # Only check expiry if enddatetime is not None
        if lot['enddatetime'] is not None and now > lot['enddatetime']:
            logging.info(f"[CLOSE EXPIRED] Closing expired lot {lot['prodlot']} at {now}")
            db = SessionLocal()
            try:
                # 1. If transactionreport is still open, close it.
                tr = db.execute(text("""
                    SELECT transactionid FROM transactionreport
                    WHERE prodlot = :prodlot AND actualenddatetime IS NULL
                    LIMIT 1
                """), {"prodlot": lot["prodlot"]}).mappings().first()
                if tr:
                    await handle_lot_end(lot)
                    lot["ended"] = True
                # 2. Always update defectsummary for this lot (even if already closed)
                update_and_insert_defectsummary(lot["prodlot"])
                logging.info(f"[LOT MANAGER] Defectsummary updated for ended lot {lot['prodlot']}")
            finally:
                db.close()

async def lot_management_tasks():
    global lots_today
    lots_today = load_today_lots()          
    await close_all_expired_lots()         
    await asyncio.gather(
        lot_management_loop(),
        daily_reload_loop(),
    )


