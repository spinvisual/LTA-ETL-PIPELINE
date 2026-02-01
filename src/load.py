import pandas as pd
import logging
import os # The "Pocket" - used to store the location of your ID card.
from google.cloud import bigquery

# --- THE SHIPPER: This sends our finished table to the Cloud ---
def load_to_bigquery(df): # We only need to pass 'df' here now
    """
    Takes our tidy table and uploads it to Google BigQuery.
    """
    # 0. THE ID CARD: Tell the computer where your secret key is.
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service_account.json"

    # 1. THE ADDRESS: These are the "Shelf" details for your Warehouse.
    project_id = "lta-etl"  # <--- REPLACE with your actual Project ID
    dataset_id = "lta_bus_data"
    table_id = "arrivals_history"
    
    try:
        # 2. THE CONNECTION: "Log in" to Google Cloud.
        client = bigquery.Client(project=project_id)
        
        # 3. THE FULL ADDRESS: Putting the shelf details together.
        full_address = f"{project_id}.{dataset_id}.{table_id}"
        
        # 4. THE RULE: "Add to the list" (WRITE_APPEND) so we keep history.
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
        )
        
        logging.info(f"Shipping {len(df)} rows of data to the Cloud...")
        
        # 5. THE ACTION: Move the data.
        job = client.load_table_from_dataframe(df, full_address, job_config=job_config)
        job.result()  # Wait for the delivery to finish.
        
        logging.info("Success! Data is now safe in the Cloud Warehouse.")
        return True
        
    except Exception as e:
        # If the delivery truck crashes, tell us why.
        logging.error(f"Upload failed: {str(e)}")
        return False