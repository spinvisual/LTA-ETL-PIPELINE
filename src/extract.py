import os
import requests
from dotenv import load_dotenv

load_dotenv()

def fetch_bus_arrival(bus_stop_code):
    """
    Fetches real-time bus arrival data based on LTA User Guide v6.6.
    Ref: Section 2.1 (Page 14)
    """
    api_key = os.getenv("LTA_API_KEY")
    
    # Updated to Version 3 URL from the Guide 
    url = "https://datamall2.mytransport.sg/ltaodataservice/v3/BusArrival"
    
    # Parameter must be 'BusStopCode' 
    params = {"BusStopCode": bus_stop_code}
    
    headers = {
        "AccountKey": api_key,
        "accept": "application/json" # [cite: 59]
    }
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data. Status Code: {response.status_code}")
        return None

if __name__ == "__main__":
    # Test with a real stop code from the guide example [cite: 253]
    test_data = fetch_bus_arrival("44259") 
    print(test_data)