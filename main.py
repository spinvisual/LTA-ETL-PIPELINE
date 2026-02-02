# --- THE TOOLS: Bringing in our workers ---
import logging             # The "Diary" - records everything that happens[cite: 53].
from datetime import datetime  # The "Calendar" - tells the code what today's date is.
from src.extract import fetch_bus_arrival  # The "Hook" - grabs data from the LTA[cite: 13, 252].
from src.transform import transform_bus_data  # The "Cleaner" - tidies up messy data into a table.
from src.load import load_to_bigquery  # The "Loader" - sends the table to Google Cloud[cite: 14, 310].

# --- THE SETUP: Naming our diary file ---
# Creates a filename like 'pipeline_20260201.log' so each day has its own record.
log_filename = f"logs/pipeline_{datetime.now().strftime('%Y%m%d')}.log"

# Tells the diary how to look (Time | How Serious | What Happened).
logging.basicConfig(
    level=logging.INFO, # Record standard info (ignore tiny details).
    format='%(asctime)s | %(levelname)s | %(message)s', 
    handlers=[
        logging.FileHandler(log_filename), # Save it to the /logs folder forever.
        logging.StreamHandler()            # Also show it on my screen right now.
    ]
)

# --- THE WORKFLOW: Step-by-step instructions ---
def run_pipeline():
    logging.info("--- Starting the Bus App! ---")
    
    # The 5-digit code for the bus stop we are checking[cite: 243, 253].
    bus_stop = "44259" 
    
    try:
        # STEP 1: GET THE DATA
        # Ask the LTA server for bus arrivals at our stop[cite: 242, 252].
        raw_data = fetch_bus_arrival(bus_stop)
        
        # If we successfully got an answer from the LTA...
        if raw_data:
            # STEP 2: CLEAN THE DATA
            logging.info("Step 2: Tidying up the data.")
            # Turn that big block of text into a neat table.
            df = transform_bus_data(raw_data)
            
            # If the table isn't empty (meaning buses are actually coming)...
            if df is not None and not df.empty:
                # Show me a "sneak peek" of the first few rows.
                print("\n--- Here is what's coming: ---")
                print(df) # Show the whole table on the screen.
                print("------------------------------\n")
                
                logging.info(f"Done! We have {len(df)} buses ready to show.")

                # --- SHIP TO THE CLOUD ---
                logging.info("Step 3: Shipping data to BigQuery...")
                # We call the loader function to move the table to Google Cloud.
                success = load_to_bigquery(df)
                
                if success:
                    logging.info("SUCCESS: Data is now safe in the Cloud!")
                else:
                    logging.error("The delivery to the Cloud failed.")

            else:
                # If no buses are on the road (like at 3 AM)[cite: 248, 249, 395].
                logging.warning("No buses found. They might be sleeping!")
        else:
            logging.warning("Could not reach the LTA server.")
            
    except Exception as e:
        # If the whole program breaks, write down why so I can fix it.
        logging.error(f"Something went wrong: {str(e)}")

# --- THE START BUTTON ---
if __name__ == "__main__":
    # Only run the code if I click "Play" on this specific file.
    run_pipeline()

# --- This is the "Doorbell" for Google Cloud ---
@functions_framework.http
def cloud_entry_point(request):
    """
    This function is what Google Cloud Functions will look for.
    The 'request' parameter is mandatory for web-triggered functions.
    """
    run_pipeline()
    return "Pipeline finished successfully!", 200