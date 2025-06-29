from database.connect_to_db import engine, Session, text, SQLAlchemyError
from fastapi import FastAPI, UploadFile, File, Form
from pathlib import Path
import shutil
import database.schemas as schemas
from datetime import datetime
from fastapi.responses import JSONResponse
import database.schemas as schemas
from typing import Union, Dict, Any, List
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

    def get_label_class(self):
        return self._fetch_all("SELECT * FROM labelclass")
    
    def get_functions(self):
        return self._fetch_all("SELECT * FROM function")
    
    def get_versions(self, modelid: int):
      result = self._fetch_all(
          "SELECT versionno FROM modelversion WHERE modelid = :modelid ORDER BY versionno DESC",
          {"modelid": modelid}
      )
      version_list = [row['versionno'] for row in result if row['versionno'] is not None]
      next_version = (max(version_list) if version_list else 0) + 1
      version_list.insert(0, next_version)
      return version_list
    
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
              "imagepath": f'dataset/{row["imagepath"]}',
              "file": str(file_path.resolve()),
              "annotate": row["annotate"],
          })

      return image_data

    def get_model_camera(self, modelversionid: int):
        return self._fetch_one("SELECT * FROM cameramodelprodapplied WHERE modelversionid = :modelversionid", {"modelversionid": modelversionid})

    def get_model_version(self, modelversionid: int):
        return self._fetch_one("""
            SELECT mv.*, m.modelname, m.modeldescription, c.prodid, c.cameraid
            FROM modelversion mv
            LEFT JOIN model m ON mv.modelid = m.modelid
            LEFT JOIN cameramodelprodapplied c on c.modelversionid  =  mv.modelversionid
            WHERE mv.modelversionid = :modelversionid
        """, {"modelversionid": modelversionid})
    
    def suggest_modelname(self, q: str):
        rows = self._fetch_all("""
            SELECT DISTINCT modelname FROM model
            WHERE isdeleted = false AND LOWER(modelname) LIKE LOWER(:keyword)
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


class DetectionModelService:
    
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
                modelname, modeldescription,
                createdby, createddate
            ) VALUES (
                :modelname, :modeldescription,
                :createdby, :createddate
            )
            RETURNING modelid
        """)
        model_result = db.execute(insert_model_sql, {
            "modelname": model.modelname,
            "modeldescription": model.modeldescription,
            "createdby": model.createdby,
            "createddate": now
        })
        modelid = model_result.scalar()

        # Insert into 'modelversion'
        insert_version_sql = text("""
            INSERT INTO modelversion (
                modelid, versionno, modelstatus,
                currentstep, createdby, createddate
            ) VALUES (
                :modelid, :versionno, :modelstatus,
                :currentstep, :createdby, :createddate
            )
            RETURNING modelversionid
        """)
        version_result = db.execute(insert_version_sql, {
            "modelid": modelid,
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
              m.modelname,
              cmp.prodid,
              m.modeldescription,
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
            LEFT JOIN cameramodelprodapplied cmp ON cmp.modelversionid = mv.modelversionid
            WHERE m.modelid = :modelid
            GROUP BY 
                m.modelid, m.modelname, m.modeldescription,
                mv.modelversionid, mv.versionno, mv.modelstatus, 
                mv.currentstep, mv.createdby, mv.createddate,
                cmp.prodid
            """)
        
        db.commit()
        row = db.execute(joined_sql, {"modelid": modelid}).mappings().first()
        if row is None:
            return error_response(404, "Model not found")

        def serialize_row(row):
            return {
                k: v.isoformat() if isinstance(v, datetime) else v
                for k, v in dict(row).items()
            }
        return success_response(200, serialize_row(row))

    @staticmethod
    def delete_model(modelid: int, db: Session):
        if not db.execute(text("SELECT 1 FROM model WHERE modelid = :modelid"), {"modelid": modelid}).first():
            raise error_response(404, "Model not found")

        db.execute(text("UPDATE model SET isdeleted = true WHERE modelid = :modelid"), {"modelid": modelid})
        db.commit()
        return success_response(200, {"message": "Model marked as deleted", "modelid": modelid, "isdeleted": True})
    
    @staticmethod
    def delete_image(imageid: int, db: Session):
        db.execute(text("DELETE FROM image WHERE imageid = :imageid"), {"imageid": imageid})
        db.commit()
        return success_response(200, {"message": "Imageid as deleted", "imageid": imageid, "isdeleted": True})
    
    @staticmethod
    def model_detail(modelversionid: int, db: Session):
        sql = text("""
          SELECT 
            m.modelid,
            m.modelname,
            cmp.prodid,
            m.modeldescription,
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
          
          JOIN (
              SELECT *
              FROM modelversion
              WHERE modelid = :modelid
              ORDER BY versionno DESC
              LIMIT 1
          ) mv ON m.modelid = mv.modelid
          LEFT JOIN modelfunction mf ON mv.modelversionid = mf.modelversionid
          LEFT JOIN function f ON mf.functionid = f.functionid
          LEFT JOIN cameramodelprodapplied cmp ON cmp.modelversionid = mv.modelversionid
          WHERE m.modelversionid = :modelversionid
          GROUP BY 
              m.modelid, m.modelname, m.modeldescription,
              mv.modelversionid, mv.versionno, mv.modelstatus, 
              mv.currentstep, cmp.prodid, mv.trainpercent, 
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
    def detection_model(db: Session):
      sql = text("""
          SELECT 
			        mv.modelversionid,
              mv.modelid,
              cmp.prodid,
              m.modelname,
              m.modeldescription,
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
          LEFT JOIN cameramodelprodapplied cmp ON cmp.modelversionid = mv.modelversionid
          GROUP BY 
              mv.modelversionid, m.modelid, cmp.prodid, m.modelname, m.modeldescription,
              mv.versionno, mv.modelstatus, mv.currentstep, mv.createdby, mv.createddate,
              mv.updatedby, mv.updateddate
      """)

      result = db.execute(sql).mappings().all()

      def serialize_datetime(row: dict) -> dict:
          return {
              key: (value.isoformat() if isinstance(value, datetime) else value)
              for key, value in dict(row).items()
          }

      # แปลง RowMapping เป็น dict และแปลง datetime เป็น string
      data = [serialize_datetime(row) for row in result]

      return success_response(200, {"data": data})
        
    @staticmethod
    def update_model_step1(modelversionid: int, model: schemas.DetectionModelUpdateStep1, db: Session):
        now = datetime.now()
        
        modelversion = db.execute(text("""
            SELECT modelid, modelstatus, versionno FROM modelversion WHERE modelversionid = :modelversionid
        """), {"modelversionid": modelversionid}).first()

        if not modelversion:
            return error_response(404, "Model version not found")

        if modelversion.modelstatus == 'Processing':
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

        else:
            cameramodelprodapplied = db.execute(text("""
                SELECT prodid FROM cameramodelprodapplied WHERE modelversionid = :modelversionid
            """), {"modelversionid": modelversionid}).first()

            latest_version = db.execute(text("""
                SELECT MAX(versionno) FROM modelversion WHERE modelid = :modelid
            """), {"modelid": model.modelid}).scalar()

            new_versionno = (latest_version or 0) + 1
            versionno = new_versionno
            prodid = cameramodelprodapplied.prodid
            new_functions = set(model.functions or [])

            # Insert new 'modelversion'
            insert_version_sql = text("""
                INSERT INTO modelversion (
                    modelid, versionno, modelstatus,
                    currentstep, createdby, createddate
                ) VALUES (
                    :modelid, :versionno, :modelstatus,
                    :currentstep, :createdby, :createddate
                )
                RETURNING modelversionid
            """)
            version_result = db.execute(insert_version_sql, {
                "modelid": model.modelid,
                "versionno": new_versionno,
                "modelstatus": "Processing",
                "currentstep": 2,
                "createdby": model.updatedby,
                "createddate": now
            })
            modelversionid = version_result.scalar()

            # Insert new 'modelfunction'
            for functionid in new_functions:
                db.execute(text("""
                    INSERT INTO modelfunction (modelversionid, functionid)
                    VALUES (:modelversionid, :functionid)
                """), {"modelversionid": modelversionid, "functionid": functionid})

            # Insert new 'cameramodelprodapplied'
            db.execute(text("""
                INSERT INTO cameramodelprodapplied (
                    modelversionid, prodid, appliedstatus
                ) VALUES (
                    :modelversionid, :prodid, :appliedstatus
                )
            """), {
                "modelversionid": modelversionid,
                "prodid": prodid,
                "appliedstatus": False
            })

        db.commit()
        return success_response(200, { "modelversionid": modelversionid, "versionno": versionno })
 
    @staticmethod
    def update_model_step2(modelversionid: int, model: schemas.DetectionModelUpdateStep2, db: Session):
      now = datetime.now()
      
      # Update 'cameramodelprodapplied'
      db.execute(text("""
          UPDATE cameramodelprodapplied
          SET cameraid = :cameraid,
              prodid = :prodid,
              appliedstatus = :appliedstatus
          WHERE modelversionid = :modelversionid
      """), {
          "cameraid": model.cameraid,
          "prodid": model.prodid,
          "appliedstatus": False,
          "modelversionid": modelversionid
      })

      # Update 'model'
      db.execute(text("""
          UPDATE model
          SET modelname = :modelname,
              modeldescription = :modeldescription,
              updatedby = :updatedby,
              updateddate = :updateddate
          WHERE modelid = :modelid
      """), {
          "modelname": model.modelname,
          "modeldescription": model.modeldescription,
          "updatedby": model.updatedby,
          "updateddate": now,
          "modelid": model.modelid
      })

      # Update 'modelversion'
      db.execute(text("""
          UPDATE modelversion
          SET trainpercent = :trainpercent,
              testpercent = :testpercent,
              valpercent = :valpercent,
              epochs = :epochs,
              currentstep = :currentstep,
              updatedby = :updatedby,
              updateddate = :updateddate
          WHERE modelversionid = :modelversionid
      """), {
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

      # Update 'cameramodelprodapplied'
      db.execute(text("""
          UPDATE cameramodelprodapplied
          SET appliedstatus = :appliedstatus,
              applieddate = :applieddate,
              appliedby = :appliedby
          WHERE modelversionid = :modelversionid
      """), {
          "appliedstatus": True,
          "applieddate": now,
          "appliedby": model.updatedby,
          "modelversionid": modelversionid
      })

      # Update 'modelversion' ปัจจุบันให้เป็น "Using"
      db.execute(text("""
          UPDATE modelversion
          SET versionno = :versionno,
              currentstep = :currentstep,
              updatedby = :updatedby,
              updateddate = :updateddate,
              modelstatus = :modelstatus
          WHERE modelversionid = :modelversionid
      """), {
          "versionno": model.versionno,
          "currentstep": 4,
          "modelstatus": "Using",
          "updatedby": model.updatedby,
          "updateddate": now,
          "modelversionid": modelversionid
      })

      # Update 'modelversion' อื่นที่มี modelid เดียวกัน แต่ไม่ใช่ตัวปัจจุบัน ให้เป็น "Ready"
      db.execute(text("""
          UPDATE modelversion
          SET modelstatus = 'Ready'
          WHERE modelid = :modelid
            AND modelstatus != 'Processing'
            AND modelversionid != :modelversionid          
      """), {
          "modelid": model.modelid,
          "modelversionid": modelversionid
      })

      db.commit()
      return success_response(200, {"modelversionid": modelversionid})

    @staticmethod
    def upload_image_file(modelversionid: int, prodid: str, cameraid: str, modelid: str, updatedby: str, annotate, file: File, db: Session) -> str:
        try:
            image_data = []
            folder = f"{prodid}/{cameraid}/{modelversionid}"
            folder_path = Path(UPLOAD_FOLDER) / folder
            folder_path.mkdir(parents=True, exist_ok=True)

            file_path = folder_path / file.filename
            # print(f"Saving to: {file_path.resolve()}")

            # Save image to disk
            with file_path.open("wb") as buffer:
              shutil.copyfileobj(file.file, buffer)

            imagepath = f"{folder}/{file.filename}"
            fullpath = str(file_path.resolve())

            # Check annotate
            if annotate in ('', "", 'null', None, {}):
                annotate_data = []
            else:
                annotate_data = annotate
              
            # Insert 'image'
            result = db.execute(text("""
                INSERT INTO image (
                    modelversionid, imagename, imagepath, annotate
                ) VALUES (
                    :modelversionid, :imagename, :imagepath, :annotate
                )
                RETURNING imageid
            """), {
                "modelversionid": modelversionid,
                "imagename": file.filename,
                "imagepath": imagepath,
                "annotate": json.dumps(annotate_data)
            })

            imageid = result.scalar()

            image_data.append({
                "imageid": imageid,
                "imagename": file.filename,
                "imagepath": f'dataset/{imagepath}',
                "file": fullpath
            })

            db.commit()
            return success_response(200, image_data)
        except Exception as e:
            print(f"Error saving file: {e}")
            return ""

    @staticmethod
    def upload_base64_image(model: schemas.DetectionModelImage, db: Session):
        try:
          image_data = []
          folder = f"{model.prodid}/{model.cameraid}/{model.modelversionid}"
          folder_path = Path(UPLOAD_FOLDER) / folder
          folder_path.mkdir(parents=True, exist_ok=True)

          file_path = folder_path / model.filename
          # print(f"Saving to: {file_path.resolve()}")

          # Save image to disk
          image_bytes = base64.b64decode(model.base64)
          with file_path.open("wb") as f:
              f.write(image_bytes)

          imagepath_str = str(file_path.resolve().as_posix())
          file_path = Path(imagepath_str) 
          # "file": str(file_path.resolve()),

          imagepath = f"{folder}/{model.filename}"
          fullpath = str(file_path.resolve())

          # Check annotate
          if model.annotate in ('', "", 'null', None, {}):
              annotate_data = []
          else:
              annotate_data = model.annotate

          # Insert 'image'
          result = db.execute(text("""
              INSERT INTO image (
                  modelversionid, imagename, imagepath, annotate
              ) VALUES (
                  :modelversionid, :imagename, :imagepath, :annotate
              )
              RETURNING imageid
          """), {
              "modelversionid": model.modelversionid,
              "imagename": model.filename,
              "imagepath": imagepath,
              "annotate": json.dumps(annotate_data)
          })

          imageid = result.scalar()

          image_data.append({
              "imageid": imageid,
              "imagename": model.filename,
              "imagepath": f'dataset/{imagepath}',
              "file": fullpath
          })

          db.commit()
          return success_response(200, image_data)
        except Exception as e:
            print(f"Error saving file: {e}")
            return ""
    

