import os
import json
import logging
import pandas as pd
import gspread
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Fetch credentials and Sheet ID from environment variables
SHEET_ID = "1IUChF0UFKMqVLxTI69lXBi-g48f-oTYqI1K9miipKgY"

def authenticate_google_sheets():
    """Authenticate and return Google Sheets client."""
    credentials_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')  # JSON string
    if not credentials_json:
        raise ValueError("GOOGLE_SHEETS_CREDENTIALS environment variable is not set.")

    credentials_info = json.loads(credentials_json)
    credentials = Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )

    try:
        client = gspread.authorize(credentials)
        logging.info("Google Sheets authentication successful.")
        return client
    except Exception as e:
        logging.error(f"Google Sheets authentication failed: {e}")
        raise

# Flatten any nested structures for uploading to Google Sheets
def flatten_data(data):
    """Flatten nested data (list or dictionary) to strings."""
    if isinstance(data, dict):
        return {key: str(value) for key, value in data.items()}  # Convert dict to string
    elif isinstance(data, list):
        return [str(item) if not isinstance(item, (dict, list)) else str(item) for item in data]  # Flatten lists
    return data  # If data is already simple, return it unchanged

def upload_to_google_sheets(sheet_id, tab_name, dataframe):
    """Upload the provided dataframe to a Google Sheet."""
    client = authenticate_google_sheets()
    sheet = client.open_by_key(sheet_id)

    # Try to find the worksheet or create a new one
    try:
        worksheet = sheet.worksheet(tab_name)
        worksheet.clear()  # Clear existing data
        logging.info(f"Worksheet '{tab_name}' found, cleared existing data.")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=tab_name, rows=str(len(dataframe) + 1), cols=str(len(dataframe.columns)))
        logging.info(f"Worksheet '{tab_name}' not found. Created a new one.")

    # Flatten the entire dataframe (one pass)
    dataframe = dataframe.apply(lambda x: flatten_data(x))  # New line using lambda

    # Update worksheet with DataFrame data
    worksheet.update([dataframe.columns.values.tolist()] + dataframe.values.tolist())
    logging.info(f"Data uploaded to '{tab_name}' successfully.")

# Function to fetch data from NSE and save to CSV
def fetch_nse_data_with_retry(url, retries=3, delay=5):
    """Fetch data from NSE API with retries in case of failure."""
    import requests
    import time

    attempt = 0
    while attempt < retries:
        try:
            # Initialize the session and set headers
            session = requests.Session()
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
                "Referer": "https://www.nseindia.com",
            }
            session.headers.update(headers)

            # First, access the general page to get cookies
            logging.info("Accessing main NSE page to fetch cookies...")
            session.get("https://www.nseindia.com/market-data/pre-open-market-cm-and-emerge-market", timeout=10)

            # Now, access the FO data API
            logging.info(f"Fetching data from: {url}")
            response = session.get(url, timeout=10)

            # Check if the response is valid
            if response.status_code == 200:
                logging.info(f"Data fetched successfully: {type(response)}")
                return response.json()  # Return the JSON response
            else:
                logging.error(f"Failed to fetch data. Status code: {response.status_code}")
                raise ValueError(f"Failed to fetch data from {url}. Status code: {response.status_code}")

        except Exception as e:
            attempt += 1
            logging.error(f"Attempt {attempt} failed: {e}")
            if attempt < retries:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logging.error(f"Failed after {retries} attempts.")
                raise

# Fetch FO data from NSE
url_fo = "https://www.nseindia.com/api/market-data-pre-open?key=FO"
nse_data = fetch_nse_data_with_retry(url_fo)

if nse_data and "data" in nse_data:
    # Initialize lists for storing the cleaned data
    cleaned_data = []
    preopen_summary = {}

    # Extract the summary for Advance, Declines, Unchanged
    if "advances" in nse_data:
        preopen_summary["Advances"] = nse_data["advances"]
    if "declines" in nse_data:
        preopen_summary["Declines"] = nse_data["declines"]
    if "unchanged" in nse_data:
        preopen_summary["Unchanged"] = nse_data["unchanged"]

    # Process each item in the "data" list
    for item in nse_data["data"]:
        # Extract metadata
        symbol = item["metadata"]["symbol"]
        identifier = item["metadata"]["identifier"]
        purpose = item["metadata"]["purpose"]
        last_price = item["metadata"]["lastPrice"]
        change = item["metadata"]["change"]
        p_change = item["metadata"]["pChange"]
        previous_close = item["metadata"]["previousClose"]
        final_quantity = item["metadata"]["finalQuantity"]
        total_turnover = item["metadata"]["totalTurnover"]
        market_cap = item["metadata"]["marketCap"]
        year_high = item["metadata"]["yearHigh"]
        year_low = item["metadata"]["yearLow"]
        iep = item["metadata"]["iep"]
        chart_today_path = item["metadata"]["chartTodayPath"]

        # Extract pre-open market data
        pre_open_data = item["detail"]["preOpenMarket"]["preopen"]
        for pre_open in pre_open_data:
            price = pre_open["price"]
            buy_qty = pre_open["buyQty"]
            sell_qty = pre_open["sellQty"]
            
            # Append the data as a row in the cleaned data list
            cleaned_data.append({
                "symbol": symbol,
                "identifier": identifier,
                "purpose": purpose,
                "lastPrice": last_price,
                "change": change,
                "pChange": p_change,
                "previousClose": previous_close,
                "finalQuantity": final_quantity,
                "totalTurnover": total_turnover,
                "marketCap": market_cap,
                "yearHigh": year_high,
                "yearLow": year_low,
                "iep": iep,
                "chartTodayPath": chart_today_path,
                "preOpenPrice": price,
                "buyQty": buy_qty,
                "sellQty": sell_qty
            })
    
    # Convert cleaned data to DataFrame
    df = pd.DataFrame(cleaned_data)
    
    # Upload the detailed data to Google Sheets
    # Remove duplicates based on 'symbol', 'preOpenPrice', 'buyQty', and 'sellQty'
    # Remove duplicates based on 'symbol', 'preOpenPrice', 'buyQty', and 'sellQty'
    df = df.drop_duplicates(subset=['symbol', 'pChange', 'lastPrice', 'previousClose'], keep='first')
    
    # Save the cleaned data to a CSV file without duplicates
    df.to_csv("preopen_data.csv", index=False)
    logging.info("Preopen data saved to 'preopen_data.csv' without duplicates.")

    
    # Upload the cleaned data to Google Sheets
    upload_to_google_sheets(SHEET_ID, "Preopen", df)

    # Save the Preopen data to a CSV file
    df.to_csv("preopen.csv", index=False)
    logging.info("Preopen data saved to 'preopen.csv'.")

    # Convert the summary data to DataFrame for Advances, Declines, Unchanged
    preopen_summary_df = pd.DataFrame([preopen_summary])

    # Upload the summary data to Google Sheets
    upload_to_google_sheets(SHEET_ID, "FO Preopen Data", preopen_summary_df)

    # Save the FO Preopen data to a CSV file
    preopen_summary_df.to_csv("FO Preopen Data.csv", index=False)
    logging.info("FO Preopen data saved to 'FO Preopen Data.csv'.")

else:
    logging.error("No 'data' found in the response.")
