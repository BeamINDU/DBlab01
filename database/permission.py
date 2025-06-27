from database.connect_to_db import engine, Session, text, SQLAlchemyError
from sqlalchemy import text
from fastapi.responses import JSONResponse
from typing import Union, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import text

def error_response(code: int, message: str):
    return JSONResponse( status_code=code, content={"detail": {"error": message}} )

def success_response(code: int, content: Union[Dict[str, Any], str]):
    return JSONResponse( status_code=code, content=content)

class PermissionDB:
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


    def login(self, username: str, password: str, db: Session):
        result = self._fetch_one("""
            SELECT 
                u.userid AS id, 
                u.userid, 
                u.username, 
                u.ufname || ' ' || u.ulname AS fullname, 
                u.email  
            FROM "user" u 
            WHERE LOWER(u.username) = LOWER(:username)
              AND LOWER(u.upassword) = LOWER(:password)
            LIMIT 1 """, 
            {"username": username, "password": password})

        if result:
            return success_response(200, {
                    "id": result.id,
                    "userid": result.userid,
                    "username": result.username,
                    "fullname": result.fullname,
                    "email": result.email,
                })
        else:
            return error_response(401, "Invalid credentials")
        
    def user_permission(self, userid: str, db: Session):
        sql = text("""
            WITH user_roles AS (
                SELECT roleid
                FROM userrole
                WHERE userid = :userid
            ),
            role_permissions_expanded AS (
                SELECT
                    rp.roleid,
                    rp.menuid,
                    (string_to_array(rp.actionid, ',')::int[]) AS action_array
                FROM rolepermission rp
                JOIN user_roles ur ON ur.roleid = rp.roleid
            ),
            unnested_actions AS (
                SELECT
                    rpe.roleid,
                    rpe.menuid,
                    UNNEST(rpe.action_array) AS actionid
                FROM role_permissions_expanded rpe
            ),
            permissions AS (
                SELECT
                    m.menuid,
                    m.parentid,
                    m.menuname,
                    m.icon,
                    m.seq,
                    m."path",
                    ARRAY_AGG(DISTINCT ua.actionid ORDER BY ua.actionid) AS actions
                FROM unnested_actions ua
                LEFT JOIN menu m ON m.menuid = ua.menuid
                GROUP BY m.menuid, m.parentid, m.menuname, m.icon, m.seq, m."path"
                ORDER BY m.seq
            ),
            has_li000 AS (
                SELECT 1 AS ok
                FROM permissions
                WHERE menuid = 'LI000'
                  AND 1 = ANY(actions)
            ),
            cameras AS (
                SELECT
                    c.cameraid,
                    c.cameraname,
                    c.cameralocation,
                    '/live/' || c.cameraid AS path
                FROM camera c
                WHERE c.camerastatus = true
                  AND c.isdeleted = false
                ORDER BY c.cameraname
            ),
            locations AS (
                SELECT DISTINCT
                    cameralocation,
                    0 AS loc_seq
                FROM cameras
                ORDER BY cameralocation
            ),
            camera_list AS (
                SELECT
                    c.*,
                    ROW_NUMBER() OVER (PARTITION BY c.cameralocation ORDER BY c.cameraname) AS cam_seq
                FROM cameras c
            )

            SELECT
                menuid,
                parentid,
                menuname,
                icon,
                seq,
                path,
                actions
            FROM permissions

            UNION ALL

            SELECT
                l.cameralocation AS menuid,
                'LI000' AS parentid,
                l.cameralocation AS menuname,
                '' AS icon,
                l.loc_seq AS seq,
                '' AS path,
                ARRAY[1]::integer[] AS actions
            FROM locations l
            JOIN has_li000 h ON TRUE

            UNION ALL

            SELECT
                cl.cameraid AS menuid,
                cl.cameralocation AS parentid,
                cl.cameraname AS menuname,
                '' AS icon,
                cl.cam_seq AS seq,
                cl.path AS path,
                ARRAY[1]::integer[] AS actions
            FROM camera_list cl
            JOIN has_li000 h ON TRUE
        """)

        result = db.execute(sql, {"userid": userid})
        rows = result.fetchall()

        return [dict(row._mapping) for row in rows]

  