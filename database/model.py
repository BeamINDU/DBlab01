from database.connect_to_db import engine, Session, text, SQLAlchemyError
from fastapi import UploadFile, File, Form
import database.schemas as schemas
from datetime import datetime
from fastapi.responses import JSONResponse
import database.schemas as schemas
from typing import Union, Dict, Any, List
import os
import shutil
from sqlalchemy.orm import Session
from sqlalchemy import text

def error_response(code: int, message: str):
    return JSONResponse( status_code=code, content={"detail": {"error": message}} )

def success_response(code: int, content: Union[Dict[str, Any], str]):
    return JSONResponse( status_code=code, content=content)

UPLOAD_FOLDER = "uploads/images" 


class DetectionModelDB:
    def _fetch_all(self, query: str, params: dict = None):
        try:
            with engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                return [dict(row) for row in result.mappings()]
        except SQLAlchemyError as e:
            print(f"Database error: {e}")
            return []

    def get_function(self):
        return self._fetch_all("SELECT * FROM function")
    
    def get_label_class(self):
        return self._fetch_all("SELECT * FROM labelclass")
    
    def get_model_function(self, modelid: int):
        return self._fetch_all("SELECT * FROM modelfunction WHERE modelid = :modelid", {"modelid": modelid})
    
    def get_model_version(self, modelid: int):
        return self._fetch_all("SELECT * FROM modelversion WHERE modelid = :modelid", {"modelid": modelid})
    
    def get_model_image(self, modelversionid: int):
        return self._fetch_all("SELECT * FROM image WHERE modelversionid = :modelversionid", {"modelversionid": modelversionid})

    def get_model_camera(self, modelversionid: int):
        return self._fetch_all("SELECT * FROM cameramodelprodapplied WHERE modelversionid = :modelversionid", {"modelversionid": modelversionid})


class DetectionModelService:
    
    @staticmethod
    def save_image_file(file: UploadFile, folder: str) -> str:
        folder_path = os.path.join(UPLOAD_FOLDER, folder)
        os.makedirs(folder_path, exist_ok=True)

        file_path = os.path.join(folder_path, file.filename)
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        return file_path
    
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
                modelversionid, modelversionid
                prodid, prodid
            ) VALUES (
                :modelversionid, :modelversionid
                :prodid, :prodid
            )
            RETURNING modelid
        """)
        db.execute(insert_prodid_sql, {
            "modelversionid": modelversionid,
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
            LEFT JOIN modelfunction mf ON m.modelid = mf.modelid
            LEFT JOIN function f ON mf.functionid = f.functionid
            JOIN (
                SELECT *
                FROM modelversion
                WHERE modelid = :modelid
                ORDER BY versionno DESC
                LIMIT 1
            ) mv ON m.modelid = mv.modelid
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
    def get_model_detail(modelversionid: int, db: Session):
        sql = text("""
           SELECT 
            m.modelid,
            mv.modelversionid,
            mv.modelstatus,
            mv.versionno,
            mv.currentstep,
            cmp.prodid,
            m.modelname,
            m.modeldescription,
            ARRAY_AGG(DISTINCT f.functionid) AS functionids,
            mv.trainpercent,
            mv.testpercent,
            mv.valpercent,
            mv.epochs
        FROM modelversion mv
        JOIN model m ON mv.modelid = m.modelid
        LEFT JOIN modelfunction mf ON m.modelid = mf.modelid
        LEFT JOIN cameramodelprodapplied cmp ON cmp.modelversionid = mv.modelversionid
        LEFT JOIN function f ON mf.functionid = f.functionid
        WHERE mv.modelversionid = :modelversionid
        GROUP BY 
            m.modelid,
            mv.modelversionid,
            mv.versionno,
            mv.modelstatus,
            mv.currentstep,
            cmp.prodid,
            m.modelname,
            m.modeldescription,
            mv.trainpercent,
            mv.testpercent,
            mv.valpercent,
            mv.epochs;
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
    def get_detection_model(db: Session):
      sql = text("""
          WITH latest_versions AS (
              SELECT mv.*
              FROM modelversion mv
              INNER JOIN (
                  SELECT modelid, MAX(versionno) AS max_version
                  FROM modelversion
                  GROUP BY modelid
              ) latest ON mv.modelid = latest.modelid AND mv.versionno = latest.max_version
          )
          SELECT 
              m.modelid,
              cmp.prodid,
              m.modelname,
              m.modeldescription,
              STRING_AGG(DISTINCT f.functionname, ', ') AS functionname,
              lv.modelversionid,
              lv.versionno,
              lv.modelstatus,
              lv.currentstep,
              lv.createdby,
              lv.createddate,
              lv.updatedby,
              lv.updateddate
          FROM model m
          LEFT JOIN modelfunction mf ON m.modelid = mf.modelid
          LEFT JOIN function f ON mf.functionid = f.functionid
          LEFT JOIN latest_versions lv ON m.modelid = lv.modelid
          LEFT JOIN cameramodelprodapplied cmp ON cmp.modelversionid = lv.modelversionid
          GROUP BY 
              m.modelid, cmp.prodid, m.modelname, m.modeldescription,
              lv.modelversionid, lv.versionno, lv.modelstatus,
              lv.currentstep, lv.createdby, lv.createddate,
              lv.updatedby, lv.updateddate
          ORDER BY m.modelid
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

      # modelfunction
      new_functions = set(model.functions or [])

      # ดึง functionid เดิมจาก DB
      existing_rows = db.execute(text("""
          SELECT functionid FROM modelfunction WHERE modelid = :modelid
      """), {"modelid": model.modelid}).fetchall()
      existing_functions = set(row[0] for row in existing_rows)

      # หา function ที่ต้อง insert
      to_insert = new_functions - existing_functions

      # หา function ที่ต้อง delete
      to_delete = existing_functions - new_functions

      # Insert ใหม่
      for functionid in to_insert:
          db.execute(text("""
              INSERT INTO modelfunction (modelid, functionid)
              VALUES (:modelid, :functionid)
          """), {"modelid": model.modelid, "functionid": functionid})

      # ลบ function ที่ไม่ได้เลือกแล้ว
      for functionid in to_delete:
          db.execute(text("""
              DELETE FROM modelfunction
              WHERE modelid = :modelid AND functionid = :functionid
          """), {"modelid": model.modelid, "functionid": functionid})

      # Update modelversion
      db.execute(text("""
          UPDATE modelversion
          SET currentstep = :currentstep,
              updatedby = :updatedby,
              updateddate = :updateddate
          WHERE modelversionid = :modelversionid
      """), {
          "currentstep": "1",
          "updatedby": model.updatedby,
          "updateddate": now,
          "modelversionid": modelversionid
      })
      db.commit()
      return success_response(200, {"modelversionid": modelversionid, "updateddate": str(now)})
    
    @staticmethod
    def update_model_step2(modelversionid: int, modelid: int, updatedby: str, files: List[UploadFile], db: Session):
        now = datetime.now()
        image_data = []
        folder = f"modelversion_{modelversionid}"

        # Insert image
        for file in files:
            # Save file
            image_path = DetectionModelService.save_image_file(file, folder)

            # Insert with RETURNING imageid
            result = db.execute(text("""
                INSERT INTO image (
                    imagepath, folder, modelversionid, imagename
                ) VALUES (
                    :imagepath, :folder, :modelversionid, :imagename
                )
                RETURNING imageid
            """), {
                "imagepath": image_path,
                "folder": folder,
                "modelversionid": modelversionid,
                "imagename": file.filename,
            })

            imageid = result.scalar()

            image_data.append({
                "imageid": imageid,
                "imagename": file.filename,
                "imagepath": image_path
            })
    
        # Update modelversion
        db.execute(text("""
            UPDATE modelversion
            SET currentstep = :currentstep,
                updatedby = :updatedby,
                updateddate = :updateddate
            WHERE modelversionid = :modelversionid
        """), {
            "currentstep": "2",
            "updatedby": updatedby,
            "updateddate": now,
            "modelversionid": modelversionid
        })
        
        db.commit()
        return success_response(200, {
            "modelversionid": modelversionid,
            "images": image_data
        })

    @staticmethod
    def update_model_step3(modelversionid: int, model: schemas.DetectionModelUpdateStep3, db: Session):
      now = datetime.now()

      # Update model
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

      # Update modelversion
      db.execute(text("""
          UPDATE modelversion
          SET trainpercent = :trainpercent,
              valpercent = :valpercent,
              valpercent = :valpercent,
              epochs = :epochs,
              currentstep = :currentstep,
              updatedby = :updatedby,
              updateddate = :updateddate,
              modelstatus = :modelstatus
          WHERE modelversionid = :modelversionid
      """), {
          "trainpercent": model.trainpercent,
          "valpercent": model.valpercent,
          "valpercent": model.valpercent,
          "epochs": model.epochs,
          "currentstep": "3",
          "updatedby": model.updatedby,
          "updateddate": now,
          "modelversionid": modelversionid
      })
      return success_response(200, {"modelversionid": modelversionid, "updateddate": str(now)})
    
    @staticmethod
    def update_model_step4(modelversionid: int, model: schemas.DetectionModelUpdateStep4, db: Session):
      now = datetime.now()

      # Update cameramodelprodapplied
      db.execute(text("""
          UPDATE cameramodelprodapplied
          SET cameraid = :cameraid,
              status = :status,
              applieddate = :applieddate,
              appliedby = :appliedby
          WHERE modelversionid = :modelversionid
      """), {
          "cameraid": model.cameraid,
          "status": "active",
          "applieddate": now,
          "appliedby": model.updatedby,
          "modelversionid": modelversionid
      })

      # Update modelversion ปัจจุบันให้เป็น "Using"
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
          "currentstep": "4",
          "modelstatus": "Using",
          "updatedby": model.updatedby,
          "updateddate": now,
          "modelversionid": modelversionid
      })

      # Update modelversion อื่นที่มี modelid เดียวกัน แต่ไม่ใช่ตัวปัจจุบัน ให้เป็น "Ready"
      db.execute(text("""
          UPDATE modelversion
          SET modelstatus = 'Ready'
          WHERE modelid = :modelid
            AND modelversionid != :modelversionid
      """), {
          "modelid": model.modelid,
          "modelversionid": modelversionid
      })
      db.commit()
      return success_response(200, {"modelversionid": modelversionid, "updateddate": str(now)})



  
      



# Case
# 1. ถ้า add new model, status = Processing
# ถ้า edit version ที่มีอยู่แล้ว จะต้องส้ราง version ใหม่ (version ใหม่ status = Processing)
# ถ้าตอน step 4 กด Finish ใช้ version ใหม่ที่เพิ่งสร้าง อันใหม่ status = Using และ version เก่า update status=Ready
# ถ้าเทรน version ใหม่ แต่ไม่ใช้ ไปใช้ version เก่า status อันที่ทำอยู่ = Ready, version ที่ใช้ = Using