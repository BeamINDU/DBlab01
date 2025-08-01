from database.connect_to_db import engine, Session, text, SQLAlchemyError
from fastapi import FastAPI, UploadFile, File, Form
from pathlib import Path
import shutil
import database.schemas as schemas
from datetime import datetime
from fastapi.responses import JSONResponse
import database.schemas as schemas
from typing import Optional, Union, Dict, Any, List
import os
from sqlalchemy.orm import Session
from sqlalchemy import text
import base64
import json

app = FastAPI()

def error_response(code: int, message: str):
    return JSONResponse( status_code=code, content={"detail": {"error": message}} )

def success_response(code: int, content: Union[Dict[str, Any], str]):
    return JSONResponse( status_code=code, content=content)

UPLOAD_FOLDER = "dataset" 

# Case
# 1. ถ้า add new model, status = Processing
# ถ้า edit version ที่มีอยู่แล้ว จะต้องส้ราง version ใหม่ (version ใหม่ status = Processing)
# ถ้าตอน step 4 กด Finish ใช้ version ใหม่ที่เพิ่งสร้าง อันใหม่ status = Using และ version เก่า update status=Ready
# ถ้าเทรน version ใหม่ แต่ไม่ใช้ ไปใช้ version เก่า status อันที่ทำอยู่ = Ready, version ที่ใช้ = Using

class DetectionModelDB:
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

    def suggest_modelname(self, q: str):
        rows = self._fetch_all("""
            SELECT DISTINCT modelname FROM modelversion
            WHERE LOWER(modelname) LIKE LOWER(:keyword)
            ORDER BY modelname ASC
            LIMIT 10; """,
            {"keyword": q + "%"}
        )
        return [{"value": row["modelname"], "label": row["modelname"]} for row in rows]
    
    def suggest_function(self, q: str):
        rows = self._fetch_all("""
            SELECT DISTINCT functionname FROM function
            WHERE LOWER(functionname) LIKE LOWER(:keyword)
            ORDER BY functionname ASC
            LIMIT 10; """,
            {"keyword": q + "%"}
        )
        return [{"value": row["functionname"], "label": row["functionname"]} for row in rows]

    def get_label_class(self, modelversionid: int):
        return self._fetch_all("SELECT * FROM labelclass WHERE modelversionid= :modelversionid", {"modelversionid": modelversionid})
    
    def get_functions(self):
        return self._fetch_all("SELECT * FROM function")
    
    def get_versions(self, modelid: int):
      return self._fetch_all("SELECT versionno, modelversionid FROM modelversion WHERE modelid = :modelid ORDER BY versionno DESC", {"modelid": modelid})
    
    def get_model_functions(self, modelversionid: int):
        return self._fetch_all("SELECT * FROM modelfunction WHERE modelversionid = :modelversionid", {"modelversionid": modelversionid})
    
    def get_model_images(self, modelversionid: int):
      rows = self._fetch_all(
          "SELECT * FROM image WHERE modelversionid = :modelversionid",
          {"modelversionid": modelversionid}
      )

      image_data = []
      for row in rows:
          relative_path = Path(row["imagepath"])
          file_path = Path(UPLOAD_FOLDER) / relative_path

          image_data.append({
              "imageid": row["imageid"],
              "imagename": row["imagename"],
              "size": row["size"],
              "width": row["width"],
              "height": row["height"],
              "annotate": row["annotate"],
              "imagepath": f'dataset/{row["imagepath"]}',
              "file": str(file_path.resolve()),
          })

      return image_data

    def get_model_camera(self, modelversionid: int):
        return self._fetch_one("SELECT * FROM cameramodelprodapplied WHERE modelversionid = :modelversionid", {"modelversionid": modelversionid})

    def get_model_version(self, modelversionid: int):
        return self._fetch_one("""
            SELECT mv.*
            FROM modelversion mv
            WHERE mv.modelversionid = :modelversionid
        """, {"modelversionid": modelversionid})
    
    def get_model_last_version(self, modelid: int):
        return self._fetch_one("""
            SELECT 
              m.modelid,
              mv.modelname,
              mv.modeldescription,
              STRING_AGG(DISTINCT f.functionname, ', ') AS functionname,
              mv.modelversionid,
              mv.versionno,
              mv.modelstatus,
              mv.currentstep,
              mv.createdby,
              mv.createddate
            FROM model m
            JOIN (
                SELECT *
                FROM modelversion
                WHERE modelid = :modelid
                ORDER BY versionno DESC
                LIMIT 1
            ) mv ON m.modelid = mv.modelid
            LEFT JOIN modelfunction mf ON mv.modelversionid = mf.modelversionid
            LEFT JOIN function f ON mf.functionid = f.functionid
            WHERE m.modelid = :modelid
            GROUP BY 
                m.modelid, mv.modelname, mv.modeldescription,
                mv.modelversionid, mv.versionno, mv.modelstatus, 
                mv.currentstep, mv.createdby, mv.createddate
        """, {"modelid": modelid})

    def get_next_version(self, modelid: int):
      result = self._fetch_all(
          "SELECT versionno, modelversionid FROM modelversion WHERE modelid = :modelid ORDER BY versionno DESC",
          {"modelid": modelid}
      )
      version_list = [row['versionno'] for row in result if row['versionno'] is not None]
      next_version = (max(version_list) if version_list else 0) + 1
      version_list.insert(0, next_version)
      return version_list

class DetectionModelService:
    @staticmethod
    def update_labelclass(modelversionid: int, models: List[schemas.LabelClassUpdate], db: Session):
        inserted_or_updated = []

        for model in models:
            if model.classid  and model.classid  > 0:
                # UPDATE
                db.execute(text("""
                    UPDATE labelclass
                    SET classname = :classname
                    WHERE classid = :classid
                """), {
                    "classname": model.classname,
                    "classid": model.classid 
                })
            else:
                # INSERT
                result = db.execute(text("""
                    INSERT INTO labelclass (modelversionid, classname)
                    VALUES (:modelversionid, :classname)
                    RETURNING classid
                """), {
                    "modelversionid": modelversionid,
                    "classname": model.classname
                })
                model.classid = result.scalar()

            inserted_or_updated.append({
                "classid": model.classid,
                "classname": model.classname
            })

        db.commit()
        return success_response(200, inserted_or_updated)

    @staticmethod
    def delete_labelclass(classid: int, db: Session):
        result = db.execute(text("""
            DELETE FROM labelclass
            WHERE classid = :classid
        """), {"classid": classid})

        if result.rowcount == 0:
            raise error_response(404, "Label class not found")

        db.commit()
        return {"status": "success", "message": f"Deleted label class {classid}"}

    @staticmethod
    def detection_model(model: schemas.DetectionModelSearch, db: Session):
        filters = []
        params = {}

        if model.modelname:
            filters.append("mv.modelname ILIKE :modelname")
            params["modelname"] = f"%{model.modelname}%"

        if model.versionno:
            filters.append("mv.versionno::TEXT ILIKE :versionno")
            params["versionno"] = f"%{model.versionno}%"

        if model.function:
            filters.append("mv.functions ILIKE :functions")
            params["functions"] = f"%{model.function}%"

        if model.modelstatus is not None:
            filters.append("mv.modelstatus = :modelstatus")
            params["modelstatus"] = model.modelstatus

        where_clause = " WHERE " + " AND ".join(filters) if filters else ""

        query = f"""
            SELECT 
                mv.modelversionid,
                mv.modelid,
                mv.modelname,
                mv.modeldescription,
                STRING_AGG(DISTINCT f.functionname, ', ') AS functionname,
                mv.versionno,
                mv.modelstatus,
                mv.currentstep,
                mv.createdby,
                mv.createddate,
                mv.updatedby,
                mv.updateddate
            FROM modelversion mv
            LEFT JOIN model m ON m.modelid = mv.modelid
            LEFT JOIN modelfunction mf ON mv.modelversionid = mf.modelversionid
            LEFT JOIN function f ON mf.functionid = f.functionid
            {where_clause}
            GROUP BY 
                mv.modelversionid, mv.modelid, mv.modelname, mv.modeldescription,
                mv.versionno, mv.modelstatus, mv.currentstep, mv.createdby, mv.createddate,
                mv.updatedby, mv.updateddate
            ORDER BY mv.modelname DESC
        """

        # print("SQL Query:", query)
        # print("Parameters:", params)
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            return list(result.mappings())

    @staticmethod
    def model_detail(modelversionid: int, db: Session):
        sql = text("""
          SELECT 
            m.modelid,
            mv.modelname,
            mv.modeldescription,
            ARRAY_AGG(DISTINCT f.functionid) AS functions,
            mv.modelversionid,
            mv.versionno,
            mv.modelstatus,
            mv.currentstep,
            mv.trainpercent,
            mv.testpercent,
            mv.valpercent,
            mv.epochs
          FROM model m
          JOIN modelversion mv ON m.modelid = mv.modelid
          LEFT JOIN modelfunction mf ON mf.modelversionid = mv.modelversionid
          LEFT JOIN function f ON f.functionid = mf.functionid
          WHERE mv.modelversionid = :modelversionid
          GROUP BY 
              m.modelid, mv.modelname, mv.modeldescription,
              mv.modelversionid, mv.versionno, mv.modelstatus, 
              mv.currentstep, mv.trainpercent, 
              mv.testpercent, mv.valpercent, mv.epochs
        """)

        row = db.execute(sql, {"modelversionid": modelversionid}).mappings().first()
        if not row:
            return error_response(404, f"Model version {modelversionid} not found")

        result = {
            k: v.isoformat() if isinstance(v, datetime) else v
            for k, v in dict(row).items()
        }
        return success_response(200, result)
       
    @staticmethod
    def add_model(model: schemas.DetectionModelCreate, db: Session):
        now = datetime.now()
        
        # Check if user exists
        if not db.execute(text("SELECT 1 FROM \"user\" WHERE userid = :userid"),
                          {"userid": model.createdby}).first():
            return error_response(400, "Invalid user (createdBy)")
        
        # Check prodid
        if model.prodid is not None:
            if not db.execute(text("SELECT 1 FROM product WHERE prodid = :prodid"),
                              {"prodid": model.prodid}).first():
                return error_response(400, "Invalid Product ID")

        # Insert into 'model'
        insert_model_sql = text("""
            INSERT INTO model (
                createdby, createddate
            ) VALUES (
                :createdby, :createddate
            )
            RETURNING modelid
        """)
        model_result = db.execute(insert_model_sql, {
            "createdby": model.createdby,
            "createddate": now
        })
        modelid = model_result.scalar()

        # Insert into 'modelversion'
        insert_version_sql = text("""
            INSERT INTO modelversion (
                modelid, modelname, modeldescription, versionno,
                modelstatus, currentstep, createdby, createddate
            ) VALUES (
                :modelid, :modelname, :modeldescription, :versionno,
                :modelstatus, :currentstep, :createdby, :createddate
            )
            RETURNING modelversionid
        """)
        version_result = db.execute(insert_version_sql, {
            "modelid": modelid,
            "modelname": model.modelname,
            "modeldescription": model.modeldescription,
            "versionno": 1,
            "modelstatus": "Processing",
            "currentstep": 0,
            "createdby": model.createdby,
            "createddate": now
        })
        modelversionid = version_result.scalar()

        # Insert into 'cameramodelprodapplied'
        insert_prodid_sql = text("""
            INSERT INTO cameramodelprodapplied (
                modelversionid, prodid, appliedstatus
            ) VALUES (
                :modelversionid, :prodid, :appliedstatus
            )
        """)
        db.execute(insert_prodid_sql, {
            "modelversionid": modelversionid,
            "appliedstatus": False,
            "prodid": model.prodid,
        })

        # ===== Return Section =====
        joined_sql = text("""
            SELECT 
              m.modelid,
              mv.modelname,
              STRING_AGG(DISTINCT f.functionname, ', ') AS functionname,
              mv.modelversionid,
              mv.versionno,
              mv.modelstatus,
              mv.currentstep,
              mv.createdby,
              mv.createddate
            FROM modelversion mv
            left JOIN model m ON m.modelid = mv.modelid
            LEFT JOIN modelfunction mf ON mv.modelversionid = mf.modelversionid
            LEFT JOIN function f ON mf.functionid = f.functionid
            WHERE mv.modelversionid = :modelversionid
            GROUP BY 
                m.modelid, mv.modelname, mv.modelversionid, 
                mv.versionno, mv.modelstatus, mv.currentstep, 
                mv.createdby, mv.createddate
            """)
        
        db.commit()
        row = db.execute(joined_sql, {"modelversionid": modelversionid}).mappings().first()

        if row is None:
            return error_response(404, "Model not found")

        def serialize_row(row):
            return {
                k: v.isoformat() if isinstance(v, datetime) else v
                for k, v in dict(row).items()
            }
        return success_response(200, serialize_row(row))

    @staticmethod
    def duplicate_model(model: schemas.DetectionModelDuplicate, db: Session):
        try:
          now = datetime.now()

          # 1. Validate user
          if not db.execute(text("SELECT 1 FROM \"user\" WHERE userid = :userid"),
                            {"userid": model.createdby}).first():
              return error_response(400, "Invalid user (createdBy)")

          # 2. Get modelid and last versionno
          model_result = db.execute(text("""
              SELECT mv.modelid, mv.versionno
              FROM modelversion mv
              WHERE mv.modelid = (
                  SELECT modelid
                  FROM modelversion
                  WHERE modelversionid = :modelversionid
              )
              ORDER BY mv.versionno DESC
              LIMIT 1
          """), {"modelversionid": model.modelversionid}).mappings().first()

          if not model_result:
              return error_response(400, f"Model version ID {model.modelversionid} not found.")

          modelid = model_result["modelid"]
          versionno = model_result["versionno"] + 1

          # Insert new modelversion (clone from old version)
          insert_version_sql = text("""
              INSERT INTO modelversion (
                  modelid, modelname, modeldescription,
                  trainpercent, testpercent, valpercent, epochs,                  
                  versionno, modelstatus, currentstep, createdby, createddate
              )
              SELECT
                  mv.modelid,
                  mv.modelname, 
                  mv.modeldescription,
                  mv.trainpercent, 
                  mv.testpercent, 
                  mv.valpercent, 
                  mv.epochs,                    
                  :versionno,
                  :modelstatus,
                  :currentstep,
                  :createdby,
                  :createddate
              FROM modelversion mv
              WHERE mv.modelversionid = :modelversionid
              RETURNING modelversionid
          """)

          version_result = db.execute(insert_version_sql, {
              "modelversionid": model.modelversionid,
              "versionno": versionno,
              "modelstatus": "Processing",
              "currentstep": 1,
              "createdby": model.createdby,
              "createddate": now
          })

          modelversionid = version_result.scalar()

          # 4. Insert modelfunction (clone from previous version)
          insert_modelfunction_sql = text("""
              INSERT INTO modelfunction (
                  modelversionid, functionid
              )
              SELECT 
                  :new_modelversionid,
                  mf.functionid
              FROM modelfunction mf
              WHERE mf.modelversionid = :old_modelversionid
          """)

          db.execute(insert_modelfunction_sql, {
              "new_modelversionid": modelversionid,
              "old_modelversionid": model.modelversionid,
              "createdby": model.createdby,
              "createddate": now
          })

          # 5. Insert cameramodelprodapplied (clone all prod/camera for version)
          insert_cameramodelprodapplied_sql = text("""
              INSERT INTO cameramodelprodapplied (
                  modelversionid, cameraid, prodid, appliedstatus
              )
              SELECT 
                  :new_modelversionid,
                  cm.cameraid,
                  cm.prodid,
                  :appliedstatus
              FROM cameramodelprodapplied cm
              WHERE cm.modelversionid = :old_modelversionid
          """)

          db.execute(insert_cameramodelprodapplied_sql, {
              "new_modelversionid": modelversionid,
              "old_modelversionid": model.modelversionid,
              "appliedstatus": False
          })

          db.commit()

          # 6. Return success
          return success_response(200, {"modelversionid": modelversionid})
        except Exception as e:
          db.rollback()
          raise e

    @staticmethod
    def delete_model(modelid: int, db: Session):
        if not db.execute(text("SELECT 1 FROM model WHERE modelid = :modelid"), {"modelid": modelid}).first():
            return error_response(404, "Model not found")

        db.execute(text("UPDATE model SET isdeleted = true WHERE modelid = :modelid"), {"modelid": modelid})
        db.commit()
        return success_response(200, {"message": "Model marked as deleted", "modelid": modelid, "isdeleted": True})
    
    @staticmethod
    def delete_modelversion(modelversionid: int, db: Session):
        
        modelversion_record = db.execute(
              text("SELECT modelstatus FROM modelversion WHERE modelversionid = :modelversionid"),
              {"modelversionid": modelversionid}
          ).first()
        
        if not modelversion_record:
              return error_response(404, "Model version not found")

        modelstatus = modelversion_record.modelstatus
        if modelstatus == "Using":
            return error_response(404, "This model is already use.")
        
        db.execute(text("DELETE FROM cameramodelprodapplied WHERE modelversionid = :modelversionid"), {"modelversionid": modelversionid})
        db.execute(text("DELETE FROM labelclass WHERE modelversionid = :modelversionid"), {"modelversionid": modelversionid})
        db.execute(text("DELETE FROM modelfunction WHERE modelversionid = :modelversionid"), {"modelversionid": modelversionid})
        db.execute(text("DELETE FROM image WHERE modelversionid = :modelversionid"), {"modelversionid": modelversionid})
        db.execute(text("DELETE FROM modelversion WHERE modelversionid = :modelversionid"), {"modelversionid": modelversionid})
        
        db.commit()
        return success_response(200, {
            "message": "Model version deleted successfully",
            "modelversionid": modelversionid,
            "isdeleted": True
        })
    
    @staticmethod
    def delete_image(imageid: int, db: Session):
        db.execute(text("DELETE FROM image WHERE imageid = :imageid"), {"imageid": imageid})
        db.commit()
        return success_response(200, {"message": "Imageid as deleted", "imageid": imageid, "isdeleted": True})
    
    @staticmethod
    def update_model_step1(modelversionid: int, model: schemas.DetectionModelUpdateStep1, db: Session):
        now = datetime.now()
        
        modelversion = db.execute(text("""
            SELECT modelid, modelstatus, versionno FROM modelversion WHERE modelversionid = :modelversionid
        """), {"modelversionid": modelversionid}).first()

        if not modelversion:
            return error_response(404, "Model version not found")

        # if modelversion.modelstatus == 'Processing':
        versionno = modelversion.versionno
        new_functions = set(model.functions or [])

        existing_rows = db.execute(text("""
            SELECT functionid FROM modelfunction WHERE modelversionid = :modelversionid
        """), {"modelversionid": modelversionid}).fetchall()
        existing_functions = set(row[0] for row in existing_rows)

        to_insert = new_functions - existing_functions
        to_delete = existing_functions - new_functions

        for functionid in to_insert:
            db.execute(text("""
                INSERT INTO modelfunction (modelversionid, functionid)
                VALUES (:modelversionid, :functionid)
            """), {"modelversionid": modelversionid, "functionid": functionid})

        for functionid in to_delete:
            db.execute(text("""
                DELETE FROM modelfunction
                WHERE modelversionid = :modelversionid AND functionid = :functionid
            """), {"modelversionid": modelversionid, "functionid": functionid})

        db.execute(text("""
            UPDATE modelversion
            SET currentstep = :currentstep,
                updatedby = :updatedby,
                updateddate = :updateddate
            WHERE modelversionid = :modelversionid
        """), {
            "currentstep": 1,
            "updatedby": model.updatedby,
            "updateddate": now,
            "modelversionid": modelversionid
        })

        # else:
        #     cameramodelprodapplied = db.execute(text("""
        #         SELECT prodid FROM cameramodelprodapplied WHERE modelversionid = :modelversionid
        #     """), {"modelversionid": modelversionid}).first()

        #     latest_version = db.execute(text("""
        #         SELECT MAX(versionno) FROM modelversion WHERE modelid = :modelid
        #     """), {"modelid": model.modelid}).scalar()

        #     new_versionno = (latest_version or 0) + 1
        #     versionno = new_versionno
        #     prodid = cameramodelprodapplied.prodid
        #     new_functions = set(model.functions or [])

        #     # Insert new 'modelversion'
        #     insert_version_sql = text("""
        #         INSERT INTO modelversion (
        #             modelid, versionno, modelstatus,
        #             currentstep, createdby, createddate
        #         ) VALUES (
        #             :modelid, :versionno, :modelstatus,
        #             :currentstep, :createdby, :createddate
        #         )
        #         RETURNING modelversionid
        #     """)
        #     version_result = db.execute(insert_version_sql, {
        #         "modelid": model.modelid,
        #         "versionno": new_versionno,
        #         "modelstatus": "Processing",
        #         "currentstep": 2,
        #         "createdby": model.updatedby,
        #         "createddate": now
        #     })
        #     modelversionid = version_result.scalar()

        #     # Insert new 'modelfunction'
        #     for functionid in new_functions:
        #         db.execute(text("""
        #             INSERT INTO modelfunction (modelversionid, functionid)
        #             VALUES (:modelversionid, :functionid)
        #         """), {"modelversionid": modelversionid, "functionid": functionid})

        #     # Insert new 'cameramodelprodapplied'
        #     db.execute(text("""
        #         INSERT INTO cameramodelprodapplied (
        #             modelversionid, prodid, appliedstatus
        #         ) VALUES (
        #             :modelversionid, :prodid, :appliedstatus
        #         )
        #     """), {
        #         "modelversionid": modelversionid,
        #         "prodid": prodid,
        #         "appliedstatus": False
        #     })

        db.commit()
        return success_response(200, { "modelversionid": modelversionid, "versionno": versionno })
 
    @staticmethod
    def update_model_step2(modelversionid: int, model: schemas.DetectionModelUpdateStep2, db: Session):
      now = datetime.now()
      
      # Update 'cameramodelprodapplied'
      # db.execute(text("""
      #     UPDATE cameramodelprodapplied
      #     SET prodid = :prodid,
      #         appliedstatus = :appliedstatus
      #     WHERE modelversionid = :modelversionid
      # """), {
      #     "prodid": model.prodid,
      #     "appliedstatus": False,
      #     "modelversionid": modelversionid
      # })

      # Update 'modelversion'
      db.execute(text("""
          UPDATE modelversion
          SET modelname = :modelname,
              modeldescription = :modeldescription,
              trainpercent = :trainpercent,
              testpercent = :testpercent,
              valpercent = :valpercent,
              epochs = :epochs,
              currentstep = :currentstep,
              updatedby = :updatedby,
              updateddate = :updateddate
          WHERE modelversionid = :modelversionid
      """), {
          "modelname": model.modelname,
          "modeldescription": model.modeldescription,
          "trainpercent": model.trainpercent,
          "testpercent": model.testpercent,
          "valpercent": model.valpercent,
          "epochs": model.epochs,
          "currentstep": 2,
          "updatedby": model.updatedby,
          "updateddate": now,
          "modelversionid": modelversionid
      })
      
      db.commit()
      return success_response(200, {"modelversionid": modelversionid})
    
    @staticmethod
    def update_model_step3(modelversionid: int, model: schemas.DetectionModelUpdateStep3, db: Session):
        now = datetime.now()

        # Update 'modelversion'
        db.execute(text("""
            UPDATE modelversion
            SET currentstep = :currentstep,
                updatedby = :updatedby,
                updateddate = :updateddate
            WHERE modelversionid = :modelversionid
        """), {
            "currentstep": 3,
            "updatedby": model.updatedby,
            "updateddate": now,
            "modelversionid": modelversionid
        })
        
        db.commit()
        return success_response(200, {"modelversionid": modelversionid })

    @staticmethod
    def update_model_step4(modelversionid: int, model: schemas.DetectionModelUpdateStep4, db: Session):
      now = datetime.now()

      # Update 'currentstep'
      db.execute(text("""
          UPDATE modelversion
          SET modelstatus = :modelversion,
              currentstep = :currentstep,
              updatedby = :updatedby,
              updateddate = :updateddate
          WHERE modelversionid = :modelversionid
      """), {
          "modelversion": 'Ready',
          "currentstep": 4,
          "updatedby": model.updatedby,
          "updateddate": now,
          "modelversionid": modelversionid
      })

      db.commit()
      return success_response(200, {"modelversionid": modelversionid})

    @staticmethod
    def upload_image_file(
        modelversionid: int,
        modelid: str,
        updatedby: str,
        annotate,
        imageid: Optional[int],
        file: Optional[File],
        size: Optional[int],
        width: Optional[int],
        height: Optional[int],
        db: Session
    ) -> str:
        try:
            image_data = {}

            # Parse annotation safely
            if annotate in ('', "null", None, {}):
                annotate_data = []
            else:
                annotate_data = annotate

            if imageid is None and file is not None:
                # Insert new image
                folder = f"{modelid}/{modelversionid}"
                folder_path = Path(UPLOAD_FOLDER) / folder
                folder_path.mkdir(parents=True, exist_ok=True)

                file_path = folder_path / file.filename

                with file_path.open("wb") as buffer:
                    shutil.copyfileobj(file.file, buffer)

                imagepath = f"{folder}/{file.filename}"
                fullpath = str(file_path.resolve())

                result = db.execute(text("""
                    INSERT INTO image (
                        modelversionid, imagename, imagepath, annotate, size, width, height
                    ) VALUES (
                        :modelversionid, :imagename, :imagepath, :annotate, :size, :width, :height
                    )
                    RETURNING imageid
                """), {
                    "modelversionid": modelversionid,
                    "imagename": file.filename,
                    "imagepath": imagepath,
                    "annotate": annotate_data,
                    "size": size,
                    "width": width,
                    "height": height,
                })
                imageid = result.scalar()

                image_data = {
                    "imageid": imageid,
                    "imagename": file.filename,
                    "imagepath": f'dataset/{imagepath}',
                    "fullpath": fullpath,
                    "size": size,
                    "width": width,
                    "height": height,
                } 

            elif imageid is not None:
                
                # Update only annotation
                db.execute(text("""
                    UPDATE image
                    SET annotate = :annotate
                    WHERE imageid = :imageid
                """), {
                    "imageid": imageid,
                    "annotate": annotate_data
                })

                # Select imagename and imagepath for response
                image_row = db.execute(text("""
                    SELECT imagename, imagepath
                    FROM image
                    WHERE imageid = :imageid
                """), {"imageid": imageid}).fetchone()

                if image_row:
                    imagename, imagepath = image_row
                    fullpath = str((Path(UPLOAD_FOLDER) / imagepath).resolve())

                    image_data = {
                        "imageid": imageid,
                        "imagename": imagename,
                        "imagepath": f'dataset/{imagepath}',
                        "fullpath": fullpath,
                        "size": size,
                        "width": width,
                        "height": height,
                    }
                else:
                    raise ValueError(f"No image found with imageid={imageid}")

            else:
                raise ValueError("File must be provided when inserting new image.")

            db.commit()
            return success_response(200, image_data)

        except Exception as e:
            print(f"Error saving file: {e}")
            db.rollback()
            raise e

    # @staticmethod
    # def upload_base64_image(model: schemas.DetectionModelImage, db: Session):
    #     try:
    #         image_data = []
    #         folder = f"{model.modelid}/{model.modelversionid}"
    #         folder_path = Path(UPLOAD_FOLDER) / folder
    #         folder_path.mkdir(parents=True, exist_ok=True)

    #         file_path = folder_path / model.filename
    #         # print(f"Saving to: {file_path.resolve()}")

    #         # Save image to disk
    #         image_bytes = base64.b64decode(model.base64)
    #         with file_path.open("wb") as f:
    #             f.write(image_bytes)

    #         imagepath_str = str(file_path.resolve().as_posix())
    #         file_path = Path(imagepath_str) 
    #         # "file": str(file_path.resolve()),

    #         imagepath = f"{folder}/{model.filename}"
    #         fullpath = str(file_path.resolve())

    #         image_data.append({
    #             "imagename": model.filename,
    #             "imagepath": f'dataset/{imagepath}',
    #             "fullpath": fullpath
    #         })

    #         return success_response(200, image_data)
    #     except Exception as e:
    #         print(f"Error saving file: {e}")
    #         db.rollback()
    #         raise e
    

