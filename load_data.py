import os
from dotenv import load_dotenv
import pandas as pd
import gspread
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from typing import List
import logging
import argparse

# --- Config ---
load_dotenv()
FOLDER_ID = os.environ["FOLDER_ID"]
CREDENTIALS_FILE = "credentials.json"
COMBINED_CSV = "combined_expenses.csv"
SUMMARY_CSV = "sheet_summary.csv"
SHEET_URLS_TXT_FILE = "cached_sheet_urls.txt"

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


def getSheetUrls(from_cache: bool = False) -> List[str]:
    def writeSheetUrlsToCache(sheet_urls: List[str]) -> None:
        with open(SHEET_URLS_TXT_FILE, mode="w") as file:
            file.writelines([url + "\n" for url in sheet_urls])
        logging.info(f"Wrote {len(sheet_urls)} to cache file: {SHEET_URLS_TXT_FILE}")

    def getSheetUrlsFromCache() -> List[str]:
        logging.info(f"Using cached sheet urls")

        sheet_urls = []
        with open(SHEET_URLS_TXT_FILE) as file:
            sheet_urls = [line.rstrip() for line in file]

        logging.info(f"Total sheets found: {len(sheet_urls)}")
        return sheet_urls

    if from_cache:
        return getSheetUrlsFromCache()

    scopes = ["https://www.googleapis.com/auth/drive.readonly"]
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    service = build("drive", "v3", credentials=creds)

    query = f"'{FOLDER_ID}' in parents and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])

    sheet_urls = []
    for f in files:
        url = f"https://docs.google.com/spreadsheets/d/{f['id']}"
        logging.debug(f"Found sheet: {f['name']} → {url}")
        sheet_urls.append(url)

    logging.info(f"Total sheets found: {len(sheet_urls)}")
    writeSheetUrlsToCache(sheet_urls)
    return sheet_urls


def parseSheets(sheet_urls: List[str]) -> None:
    gc = gspread.service_account(
        filename=CREDENTIALS_FILE, http_client=gspread.BackOffHTTPClient
    )
    gc.http_client._MAX_BACKOFF = 10

    all_expenses = []
    summary_data = []

    for url in sheet_urls:
        try:
            logging.info(f"Processing sheet: {url}")
            sh = gc.open_by_url(url)

            # Ensure 'Expenses' tab exists
            worksheets = [ws.title for ws in sh.worksheets()]
            assert "Expenses" in worksheets, f"'Expenses' tab missing"

            ws = sh.worksheet("Expenses")
            rows = ws.get_all_values()

            # Ensure non-empty data
            assert rows, "No data found"

            header, data = rows[0], rows[1:]
            df = pd.DataFrame(data, columns=header)

            # Assert required columns
            required_cols = {"Date", "Description", "Amount"}
            missing_cols = required_cols - set(df.columns)
            assert not missing_cols, f"Missing columns: {missing_cols}"

            # Keep only required columns
            df = df[[col for col in df.columns if col in required_cols]]

            # Drop "Accounted" if present
            if "Accounted" in df.columns:
                logging.info("Dropping 'Accounted' column")
                df = df.drop(columns=["Accounted"])

            # Convert Amount → amountCents
            df["amountCents"] = (
                df["Amount"]
                .str.replace(r"[,\$]", "", regex=True)
                .str.replace(r"\((.*?)\)", r"-\1", regex=True)
                .astype(float)
                .mul(100)
                .round()
                .astype(int)
            )
            df = df.drop(columns=["Amount"])

            # Check integer type
            assert pd.api.types.is_integer_dtype(
                df["amountCents"]
            ), "amountCents not integer type"

            logging.info(f"Parsed {len(df)} rows from {url}")
            all_expenses.append(df)
            summary_data.append({"sheet_url": url, "rows": len(df), "status": "OK"})

        except AssertionError as e:
            logging.error(f"Assertion failed for {url}: {e}")
            summary_data.append(
                {"sheet_url": url, "rows": 0, "status": f"Assertion failed: {e}"}
            )
        except Exception as e:
            logging.exception(f"Unexpected error processing {url}")
            summary_data.append({"sheet_url": url, "rows": 0, "status": f"Error: {e}"})

    # Combine all sheets
    if all_expenses:
        combined_df = pd.concat(all_expenses, ignore_index=True)
        combined_df.to_csv(COMBINED_CSV, index=False)
        logging.info(
            f"Saved combined expenses to {COMBINED_CSV} with {len(combined_df)} total rows"
        )
    else:
        logging.warning("No expenses compiled.")

    print(summary_data)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--from-cache", help="Use cached sheets list", action="store_true"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)",
    )
    args = parser.parse_args()
    logging.getLogger().setLevel(getattr(logging, args.log_level.upper()))
    logging.debug(f"Log level set to {args.log_level.upper()}")

    sheet_urls = getSheetUrls(args.from_cache)
    parseSheets(sheet_urls)


if __name__ == "__main__":
    main()
