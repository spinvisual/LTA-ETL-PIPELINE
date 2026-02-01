import pandas as pd  # The "Table Maker" - lets us organize data into rows and columns.
import logging        # The "Diary" - lets us record if the cleaning process went well.

def transform_bus_data(raw_json):
    """
    Takes the messy computer-code from LTA and turns it into a clean table.
    Ref: LTA User Guide Page 15 [cite: 255]
    """
    # CHECK: If the data is empty or missing the 'Services' list, stop here.
    if not raw_json or 'Services' not in raw_json:
        logging.warning("Transform: No services found in data.")
        return None

    # GRAB: Get the 5-digit bus stop code from the top of the data[cite: 257].
    bus_stop_code = raw_json.get('BusStopCode')
    # SETUP: Create an empty list to hold our finished rows.
    flattened_data = []

    # LOOP: Go through every bus service (like 12, 24, 31) found at the stop[cite: 258].
    for service in raw_json['Services']:
        # ID: Get the bus number[cite: 260].
        service_no = service.get('ServiceNo')
        
        # FOCUS: Look specifically at the very next bus coming[cite: 262].
        next_bus = service.get('NextBus', {})
        
        # --- TIME CLEANER: Adding the space you asked for ---
        # Get the arrival time (looks like 2026-02-01T16:41:48)[cite: 243, 266].
        raw_eta = next_bus.get('EstimatedArrival', '')
        
        if raw_eta:
            # SIMPLE TAG: Swap the 'T' for a ' ' and keep only the first 19 characters.
            clean_eta = raw_eta.replace('T', ' ')[:19] 
        else:
            clean_eta = "No Data" # Use this if the bus isn't on the road[cite: 398].

        # ROW MAKER: Create one neat line of data for this specific bus.
        arrival_row = {
            "bus_stop_code": bus_stop_code, # Where the bus is stopping[cite: 257].
            "service_no": service_no,       # The bus number[cite: 260].
            "operator": service.get('Operator'), # The company (e.g., SBST, GAS)[cite: 243, 261].
            "estimated_arrival": clean_eta,      # The arrival time with a nice space.
            "load": next_bus.get('Load'),        # How crowded: Seats (SEA) or Standing (SDA)[cite: 243, 271].
            "bus_type": next_bus.get('Type')     # Type: Single (SD) or Double (DD) decker[cite: 243, 273].
        }
        # ADD: Put this neat line into our collection.
        flattened_data.append(arrival_row)

    # TABLE: Turn our collection of lines into a full Pandas table.
    df = pd.DataFrame(flattened_data)
    
    # --- THE ORGANIZER: Sorting by time ---
    # We tell the table to sort itself by the 'estimated_arrival' column.
    # 'ascending=True' means the soonest bus will be at the very top.
    df = df.sort_values(by='estimated_arrival', ascending=True)
    
    # RECORD: Write in the diary how many buses we successfully tidied up.
    logging.info(f"Transform: Flattened and sorted {len(df)} bus services.")
    
    # DONE: Hand back the finished table to main.py.
    return df