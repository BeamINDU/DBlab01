from database.connect_to_db import Session, text
from pathlib import Path
from datetime import datetime
import base64

def get_live_inspection_data(camera_id, db: Session):
    # --- Run model query (same as before) ---
    sql_models = text("""
        SELECT
            m.modelid AS "modelId",
            m.modelname AS "modelName",
            m.modeldescription AS "description",
            STRING_AGG(f.functionname, ', ') AS "function",
            lv.modelstatus AS "statusName",
            lv.versionno AS "currentVersion",
            --  lv.trainpercent AS "currentStep",
            m.createddate AS "createdDate",
            m.createdby AS "createdBy",
            lv.updateddate AS "updatedDate",
            lv.updatedby AS "updatedBy"
            FROM public.model m
            LEFT JOIN (
            SELECT mv1.*
            FROM public.modelversion mv1
            JOIN (
                SELECT modelid, MAX(versionno) AS max_version
                FROM public.modelversion
                GROUP BY modelid
            ) mv2 ON mv1.modelid = mv2.modelid AND mv1.versionno = mv2.max_version
            ) lv ON m.modelid = lv.modelid
            LEFT JOIN public.modelfunction mf ON m.modelid = mf.modelid
            LEFT JOIN public.function f ON mf.functionid = f.functionid
            WHERE m.isdeleted = false
            GROUP BY
            m.modelid,
            m.modelname,
            m.modeldescription,
            lv.modelstatus,
            lv.versionno,
            lv.trainpercent,
            m.createddate,
            m.createdby,
            lv.updateddate,
            lv.updatedby
            ORDER BY m.modelid;
    """)
    result_models = db.execute(sql_models).mappings()
    model_rows = []
    for row in result_models:
        row_dict = dict(row)
        for key, value in row_dict.items():
            if isinstance(value, datetime):
                row_dict[key] = value.strftime("%Y-%m-%d %H:%M:%S")
        model_rows.append(row_dict)

    # --- Use cameraId in defect+planning query ---
    sql_defect = text("""
        SELECT 
            pdr.resultid,
            pr.prodname AS "product_name",
            cm.cameraid,
            cm.cameraname, 
            pr.prodid AS "productId",
            pr.prodname AS "productName",
            pr.prodserial AS "serialNo",
            p.prodlot AS "lotNo",
            p.prodline AS "location",
            dt.defecttype,
            dt.defectdescription,
            pdr.imagepath,
            pdr.prodstatus,
            pt.prodtypeid AS "productTypeId",
            pt.prodtype AS "productTypeName",
            p.startdatetime AS "startdatetime",
            p.quantity AS "totalPlanning"
        FROM productdefectresult pdr
        LEFT JOIN camera cm ON pdr.cameraid = cm.cameraid
        LEFT JOIN defecttype dt ON pdr.defectid = dt.defectid
        LEFT JOIN product pr ON pdr.prodid = pr.prodid
        LEFT JOIN image i ON pdr.imagepath = i.imagepath 
        LEFT JOIN planning p ON p.prodid = pr.prodid 
        LEFT JOIN prodtype pt ON pt.prodtypeid = pr.prodtypeid 
        WHERE pdr.prodstatus IS NOT NULL AND pr.isdeleted = FALSE
          AND pdr.cameraid = :camera_id
        LIMIT 1
    """)
    row = db.execute(sql_defect, {"camera_id": camera_id}).mappings().first()

    defect_data = None
    if row:
        image_base64 = None
        if row["imagepath"]:
            raw_path = row["imagepath"].strip()
            image_path = Path(raw_path)
            if image_path.exists():
                with image_path.open("rb") as img_file:
                    image_base64 = base64.b64encode(img_file.read()).decode("utf-8")
        defect_data = {
            "liveStream": image_base64,
            "location": row["location"],
            "cameraId": row["cameraid"],
            "cameraName": row["cameraname"],
            "status": str(row["prodstatus"]),
            "productId": row["productId"],
            "productName": row["productName"],
            "productTypeId": row["productTypeId"],
            "productTypeName": row["productTypeName"],
            "serialNo": row["serialNo"],
            "defectType": row["defecttype"],
            "productDateTime": row["startdatetime"].strftime("%Y-%m-%d %H:%M:%S") if row["startdatetime"] else None,
            "lotNo": row["lotNo"],
            "totalNG": 1000,
            "totalPlanning": row["totalPlanning"],
            "actualPlanning": 2400
        }
    else:
        defect_data = {"error": "No defect + planning found"}

    # return the result as would have been sent to websocket
    return {
        "status": 200,
        "models": model_rows,
        "defect": defect_data
    }

