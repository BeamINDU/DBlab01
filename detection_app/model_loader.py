import psycopg2
from ultralytics import YOLO
from pathlib import Path
from datetime import datetime
import pytz

DB_HOST = "127.0.0.1"
DB_PORT = 5432
DB_NAME = "mydb"
DB_USER = "user"
DB_PASS = "password"

detection_models = {}       # modelversionid -> model
function_models = {}        # (modelversionid, functionid) -> model

def get_bangkok_date():
    """Return today's date (YYYY-MM-DD) in Asia/Bangkok timezone."""
    bangkok = pytz.timezone("Asia/Bangkok")
    return datetime.now(bangkok).strftime("%Y-%m-%d")

def get_detection_model_pairs():
    """
    Return list of (modelversionid, modelid) for detection models needed for today.
    Uses actualstartdatetime and actualenddatetime if available, otherwise falls back to startdatetime and enddatetime.
    """
    today = get_bangkok_date()
    query = f"""
         WITH latest_seq AS (
            SELECT DISTINCT ON (p.planid, p.prodid, p.prodlot, p.prodline)
                p.planid, p.prodid, p.prodlot, p.prodline,
                s.seq_no, s.actualstartdatetime, s.actualenddatetime,
                p.startdatetime AS planstartdate, p.enddatetime AS planenddate
            FROM public.planning p
            LEFT JOIN public.planningseq s
                ON p.planid = s.planid AND p.prodid = s.prodid AND p.prodlot = s.prodlot AND p.prodline = s.prodline
            WHERE p.isdeleted = false
            ORDER BY p.planid, p.prodid, p.prodlot, p.prodline, s.seq_no DESC
        )
        SELECT DISTINCT mv.modelversionid, mv.modelid
        FROM public.camera c
        LEFT JOIN public.cameramodelprodapplied cm ON cm.cameraid = c.cameraid
        LEFT JOIN latest_seq l ON cm.prodid = l.prodid
        LEFT JOIN public.modelversion mv ON cm.modelversionid = mv.modelversionid
        WHERE
          (
            (l.actualstartdatetime IS NOT NULL
             AND (l.actualenddatetime IS NULL OR l.actualenddatetime >= '{today}'::date))
            OR (l.actualstartdatetime IS NULL
                AND l.planstartdate <= '{today}'::date + INTERVAL '1 day'
                AND l.planenddate >= '{today}'::date)
          )
          AND mv.modelstatus = 'Using'
          AND c.camerastatus = true
          AND cm.appliedstatus = true
    """
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        return [(int(row[0]), int(row[1])) for row in rows if row[0] is not None and row[1] is not None]
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def get_detection_model_path(modelversionid, modelid):
    BASE_DIR = Path(__file__).resolve().parent
    return BASE_DIR / "models" / str(modelversionid) / str(modelid) / "best.pt"

def get_function_model_pairs():
    """
    Return list of (modelversionid, functionid) for function models needed for today.
    """
    today = get_bangkok_date()
    query = f"""
    WITH latest_seq AS (
          SELECT DISTINCT ON (p.planid, p.prodid, p.prodlot, p.prodline)
            p.planid, p.prodid, p.prodlot, p.prodline,
            s.seq_no, s.actualstartdatetime, s.actualenddatetime,
            p.startdatetime AS planstartdate, p.enddatetime AS planenddate
          FROM public.planning p
          LEFT JOIN public.planningseq s
            ON p.planid = s.planid AND p.prodid = s.prodid AND p.prodlot = s.prodlot AND p.prodline = s.prodline
          WHERE p.isdeleted = false
          ORDER BY p.planid, p.prodid, p.prodlot, p.prodline, s.seq_no DESC
        )
        SELECT DISTINCT mf.modelversionid, mf.functionid
        FROM public.camera c
        LEFT JOIN public.cameramodelprodapplied cm ON cm.cameraid = c.cameraid
        LEFT JOIN latest_seq l ON cm.prodid = l.prodid
        LEFT JOIN public.modelversion mv ON cm.modelversionid = mv.modelversionid
        LEFT JOIN public.modelfunction mf ON mf.modelversionid = mv.modelversionid
        WHERE
          (
            (l.actualstartdatetime IS NOT NULL
             AND (l.actualenddatetime IS NULL OR l.actualenddatetime >= '{today}'::date))
            OR (l.actualstartdatetime IS NULL
                AND l.planstartdate <= '{today}'::date + INTERVAL '1 day'
                AND l.planenddate >= '{today}'::date)
          )
          AND mv.modelstatus = 'Using'
          AND c.camerastatus = true
          AND cm.appliedstatus = true
    """
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        return [(int(row[0]), int(row[1])) for row in rows if row[0] is not None and row[1] is not None]
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def get_function_model_path(modelversionid, modelid, functionid):
    BASE_DIR = Path(__file__).resolve().parent
    return BASE_DIR / "models" / str(modelversionid) / str(modelid) / str(functionid) / "best.pt"

def load_all_models():
    # Detection
    detection_models.clear()
    pairs = get_detection_model_pairs()
    print("Loading detection models for:", pairs)
    for modelversionid, modelid in pairs:
        model_path = get_detection_model_path(modelversionid, modelid)
        if model_path.exists():
            try:
                detection_models[modelversionid] = YOLO(str(model_path))
                print(f"Loaded detection model ({modelversionid}): {model_path}")
            except Exception as e:
                print(f"ERROR: Could not load detection model ({modelversionid}): {e}")
        else:
            print(f"WARNING: Detection model file {model_path} does not exist for ({modelversionid})")

    # Function
    function_models.clear()
    function_pairs = get_function_model_pairs()
    print("Loading function models for:", function_pairs)
    for modelversionid, functionid in function_pairs:
        model_path = get_function_model_path(modelversionid, modelid, functionid)
        if model_path.exists():
            try:
                function_models[(modelversionid, functionid)] = YOLO(str(model_path))
                print(f"Loaded function model ({modelversionid}, {functionid}): {model_path}")
            except Exception as e:
                print(f"ERROR: Could not load function model ({modelversionid}, {functionid}): {e}")
        else:
            print(f"WARNING: Function model file {model_path} does not exist for ({modelversionid}, {functionid})")

def get_detection_model(modelversionid):
    return detection_models.get(int(modelversionid))

def get_function_model(modelversionid, functionid):
    return function_models.get((int(modelversionid), int(functionid)))

