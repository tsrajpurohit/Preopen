import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from nsepython import *
import logging
import time
import os
import json

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Authenticate and connect to Google Sheets
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

def save_dataframe_to_csv(dataframe, file_name):
    """Save the provided DataFrame to a CSV file."""
    try:
        os.makedirs("output", exist_ok=True)  # Create 'output' directory if it doesn't exist
        file_path = os.path.join("output", file_name)
        dataframe.to_csv(file_path, index=False)
        logging.info(f"Data saved to CSV file: {file_path}")
    except Exception as e:
        logging.error(f"Error saving data to CSV file '{file_name}': {e}")

def validate_and_convert_to_dataframe(data, tab_name):
    """Ensure data is in DataFrame format, or convert it."""
    logging.info(f"Validating data for {tab_name}, type: {type(data)}")

    if isinstance(data, pd.DataFrame):
        logging.info(f"{tab_name} data is already a DataFrame.")
        return data
    elif isinstance(data, tuple):
        logging.warning(f"{tab_name} data returned as tuple, examining content: {data}")
        if len(data) > 0 and isinstance(data[0], pd.DataFrame):
            return data[0]  # Extract the DataFrame from the tuple
        else:
            logging.warning(f"Unable to extract DataFrame from tuple for {tab_name}. Skipping.")
            return None
    elif isinstance(data, list):
        try:
            return pd.DataFrame(data)
        except Exception as e:
            logging.error(f"Error converting {tab_name} data to DataFrame: {e}")
            return None
    elif isinstance(data, dict):
        try:
            return pd.DataFrame([data])  # Convert dict to DataFrame (single-row)
        except Exception as e:
            logging.error(f"Error converting dictionary data for {tab_name} to DataFrame: {e}")
            return None
    elif isinstance(data, str):
        try:
            return pd.read_json(data)
        except Exception as e:
            logging.error(f"Error converting string data for {tab_name} to DataFrame: {e}")
            return None
    else:
        logging.warning(f"Unexpected data format for {tab_name}. Skipping.")
        return None

def fetch_nse_data_with_retry(fetch_function, *args, retries=3, delay=5):
    """Fetch data from NSE API with retries in case of failure."""
    attempt = 0
    while attempt < retries:
        try:
            logging.info(f"Fetching data with args: {args}")
            result = fetch_function(*args)  # Do not pass 'pandas' here
            logging.info(f"Data fetched successfully: {type(result)}")
            return result
        except Exception as e:
            attempt += 1
            logging.error(f"Attempt {attempt} failed: {e}")
            if attempt < retries:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logging.error(f"Failed after {retries} attempts.")
                raise

def save_all_data_to_google_sheets():
    """Fetch data from NSE API, process, and upload to Google Sheets."""
    sheet_id = "1IUChF0UFKMqVLxTI69lXBi-g48f-oTYqI1K9miipKgY"

    try:
        # Fetch data from NSE APIs (directly as pandas DataFrames)
        preopen_payload = fetch_nse_data_with_retry(nse_preopen, "FO")  # Fetch without 'pandas'
        preopen_movers = fetch_nse_data_with_retry(nse_preopen_movers, "FO")  # Fetch without 'pandas'
        gainers = fetch_nse_data_with_retry(nse_preopen_movers, "NIFTY")  # Fetch without 'pandas'
        movers = fetch_nse_data_with_retry(nse_preopen_movers, "NIFTY")  # Fetch without 'pandas'

        # Fetch 'FO' segment preopen data to extract "advances", "declines", "unchanged"
        payload = nse_preopen("FO", "raw")  # Use "raw" format for the payload
        advances = payload.get("advances", [])
        declines = payload.get("declines", [])
        unchanged = payload.get("unchanged", [])

        # Ensure each key has a corresponding list (even if empty)
        payload_data = {
            "Advances": advances if isinstance(advances, list) else [advances],
            "Declines": declines if isinstance(declines, list) else [declines],
            "Unchanged": unchanged if isinstance(unchanged, list) else [unchanged]
        }

        # Convert to DataFrame
        payload_df = pd.DataFrame(payload_data)

        # Upload data to Google Sheets and save to CSV
        upload_data_to_sheets(preopen_payload, "Preopen", sheet_id)
        upload_data_to_sheets(preopen_movers, "Preopen Movers", sheet_id)
        upload_data_to_sheets(gainers, "Pre_Nifty Gainers", sheet_id)
        upload_data_to_sheets(movers, "pre_Nifty Movers", sheet_id)
        upload_data_to_sheets(payload_df, "FO Preopen Data", sheet_id)

    except Exception as e:
        logging.error(f"Error while saving data to Google Sheets: {e}")

def upload_data_to_sheets(data, tab_name, sheet_id):
    """Validate and upload data to Google Sheets and save to CSV."""
    try:
        dataframe = validate_and_convert_to_dataframe(data, tab_name)
        if dataframe is not None:
            upload_to_google_sheets(sheet_id, tab_name, dataframe)
            save_dataframe_to_csv(dataframe, f"{tab_name}.csv")
        else:
            logging.warning(f"{tab_name} data was not valid and was skipped.")
    except Exception as e:
        logging.error(f"Error uploading {tab_name} data to Google Sheets: {e}")

if __name__ == "__main__":
    save_all_data_to_google_sheets()
