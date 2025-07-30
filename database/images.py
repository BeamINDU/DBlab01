from database.connect_to_db import Session
from fastapi import FastAPI, Query
from typing import List
import boto3
import time
from datetime import datetime

app = FastAPI()
 
athena_client = boto3.client('athena', region_name='ap-southeast-1')
s3_client = boto3.client('s3')
 
database_name = 'takumi_inspection_db'
table_name = 'product_inspections_auto'
output_location = 's3://takumi-product-inspection/'

class ImagesService:
    @staticmethod
    def result_image(filename: str, defecttime: datetime):

        sql_query = f"""
            SELECT * FROM {database_name}.{table_name}
            WHERE filename = '{filename}'
            AND year = '{defecttime.year}'
            AND month = '{str(defecttime.month).zfill(2)}' 
            AND day = '{str(defecttime.day).zfill(2)}' 
            AND hour = '{str(defecttime.hour).zfill(2)}'
        """
    
        try:
            response_start = athena_client.start_query_execution(
                QueryString=sql_query,
                QueryExecutionContext={'Database': database_name},
                ResultConfiguration={'OutputLocation': output_location}
            )
            query_execution_id = response_start['QueryExecutionId']
    
            while True:
                response_get_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                status = response_get_status['QueryExecution']['Status']['State']
    
                if status == 'SUCCEEDED':
                    break
                elif status in ['FAILED', 'CANCELLED']:
                    reason = response_get_status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                    return {"status": status, "reason": reason}
                time.sleep(2)
    
            response_get_results = athena_client.get_query_results(
                QueryExecutionId=query_execution_id,
                MaxResults=1000
            )
    
            column_info = response_get_results['ResultSet']['ResultSetMetadata']['ColumnInfo']
            column_names = [col['Name'] for col in column_info]
    
            rows = response_get_results['ResultSet']['Rows']
    
            results = []
            for row in rows[1:]:  # ข้าม header
                values = [col.get('VarCharValue', '') for col in row['Data']]
                results.append(dict(zip(column_names, values)))
    
            return {
                "filename": filename,
                "count": len(results),
                "results": results
            }
    
        except Exception as e:
            return {"error": str(e)}