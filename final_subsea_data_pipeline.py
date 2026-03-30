"""
Author: 66Degrees/Subsea7
Date: March 2026
Description: ETL pipeline to migrate HSE Intelligence data from Azure Databricks 
             SQL Warehouse to Google BigQuery with robust connection handling.
Version: 1.1
"""

import logging
import os
from xmlrpc import client
import requests
import pandas as pd
from databricks import sql
from dotenv import load_dotenv
from google.cloud import bigquery

# --- Configuration & Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("databricks_to_bq.log")
    ]
)
logger = logging.getLogger(__name__)

# Security bypasses for specific network environments
os.environ['DATABRICKS_SDK_INSECURE'] = 'true'
os.environ['PYTHONHTTPSVERIFY'] = '0'

load_dotenv()

def main():
    # Load Environment Variables
    db_host = os.getenv("databricks_server_host")
    db_path = os.getenv("databricks_http_path")
    db_id = os.getenv("databricks_sp_id")
    db_secret = os.getenv("databricks_sp_secret")
    db_tenant = os.getenv("databricks_tenant_id")
    db_scope = os.getenv("databricks_app_scope")
    
    bq_project = os.getenv("bq_project")
    bq_dataset = os.getenv("bq_dataset")
    bq_table = os.getenv("bq_table")

    conn = None  

    try:
        # 1. Acquire Azure AD token
        logger.info("Acquiring Azure AD token for Databricks...")
        token_url = f"https://login.microsoftonline.com/{db_tenant}/oauth2/v2.0/token"
        token_resp = requests.post(token_url, data={
            "grant_type": "client_credentials",
            "client_id": db_id,
            "client_secret": db_secret,
            "scope": f"{db_scope}/.default"
        })
        token_resp.raise_for_status()
        access_token = token_resp.json()["access_token"]

        # 2. Databricks Connection
        logger.info(f"Connecting to Databricks SQL Warehouse...")
        conn = sql.connect(
            server_hostname=db_host, 
            http_path=db_path, 
            access_token=access_token,
            auth_type="access_token"
        )
        
        with conn.cursor() as cursor:
            query = """
    SELECT 
        Case_Number, 
        Case_Date, 
        Case_Type, 
        Title, 
        Description, 
        Cause_Description, 
        Country, 
        Shore, 
        Location, 
        Location_Full_Path, 
        Project, 
        Project_Full_Path, 
        Unit_In_Charge, 
        Unit_In_Charge_Full_Path, 
        Person_In_Charge,
        CAST(Causes as STRING) AS Causes,
        CAST(Actual_Loss as STRING) AS Actual_Loss,
        Cast(Potential_Loss as STRING) AS Potential_Loss
    FROM dp_prod.synergi.gd_gcp_hse_intelligence
    """
            logger.info(f"Executing query: {query}")
            cursor.execute(query)
            
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        if rows:
            df = pd.DataFrame(rows, columns=columns)
            logger.info(f"Retrieved {len(df)} rows. Starting BigQuery load.")
            client = bigquery.Client(project=bq_project)


            logger.info("Starting BigQuery Audit...")
            client.query(f"CALL `{bq_project}.{bq_dataset}.create_synegi_audit_log`()").result()

            update_query = f"""
                UPDATE `{bq_project}.subsea_hseq_analytics_dev.synergi_daily_data_load_status`
                SET row_count = {len(df)} 
                WHERE complete_flag ='N'
                and job_name ='synergi_data_load'
                """

            # 3. BigQuery Load
            table_id = f"{bq_project}.{bq_dataset}.{bq_table}"
            #client = bigquery.Client(project=bq_project)
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                autodetect=True
            )

            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()  
            
            logger.info(f"SUCCESS: Loaded {len(df)} rows into {table_id}")
            logger.info("Closing BigQuery Audit (Success)...")


           
            client.query(update_query).result()
            client.query(f"CALL `{bq_project}.{bq_dataset}.close_synegi_audit_log`()").result()

            

        else:
            logger.warning("No data found to migrate.")

    except Exception as e:
        logger.error("Pipeline failed:", exc_info=True)
        client.query(f"CALL `{bq_project}.{bq_dataset}.close_synegi_audit_log`").result()

    
    finally:
        if conn:
            conn.close()
            logger.info("Databricks connection closed.")


if __name__ == "__main__":
    main()