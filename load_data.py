import pprint
import re
import os
from dotenv import load_dotenv
import pandas as pd
import gspread
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from typing import List, Dict, Any
import logging
import argparse

# --- Config ---
load_dotenv()
FOLDER_ID = os.environ["FOLDER_ID"]
CREDENTIALS_FILE = "credentials.json"
OUTPUT_DIR = "./output"
OUTPUT_FILE_NAME = "combined_expenses.csv"
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


class SheetsParser:
    def __init__(self, credentials_file: str, exclude_regex: str = None):
        self.gc = gspread.service_account(
            filename=credentials_file, http_client=gspread.BackOffHTTPClient
        )
        self.gc.http_client._MAX_BACKOFF = 10
        self.all_expenses: List[pd.DataFrame] = []
        self.summary_data: List[Dict[str, Any]] = []
        self.exclude_regex = re.compile(exclude_regex) if exclude_regex else None

    def parseSheets(self, sheet_urls: List[str]) -> None:
        for url in sheet_urls:
            result = self._parseSingleSheet(url)
            if result["rows"] > 0 and "df" in result:
                self.all_expenses.append(result["df"])
            self.summary_data.append({k: v for k, v in result.items() if k != "df"})

        # Combine all sheets
        if self.all_expenses:
            combined_df = pd.concat(self.all_expenses, ignore_index=True)

            if not os.path.exists(OUTPUT_DIR):
                os.mkdir(OUTPUT_DIR)

            file_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE_NAME)

            combined_df.to_csv(file_path, index=False)
            logging.info(
                f"Saved combined expenses to {file_path} with {len(combined_df)} total rows"
            )
        else:
            logging.warning("No expenses compiled.")

        pprint.pp(self.summary_data)

    def _parseSingleSheet(self, url: str) -> Dict[str, Any]:
        try:
            logging.info(f"Processing sheet: {url}")
            sh = self.gc.open_by_url(url)

            # Ensure 'Expenses' tab exists
            worksheets = [ws.title for ws in sh.worksheets()]
            assert "Expenses" in worksheets, f"'Expenses' tab missing"

            ws = sh.worksheet("Expenses")
            rows = ws.get_all_values()
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

            # Apply exclude regex if given
            if self.exclude_regex is not None:
                mask = df["Description"].str.contains(
                    self.exclude_regex, regex=True, na=False
                )
                for dropped_row in df[mask].to_dict(orient="records"):
                    logging.info(
                        f"Dropping row with description matching regex: {dropped_row}"
                    )
                df = df[~mask]

            # Convert Amount → amountCents
            df["amountCents"] = (
                df["Amount"]
                .str.replace(r"[,\$]", "", regex=True)  # remove $ and ,
                .replace("", "0")
                .fillna("0")
                .astype(float)
                .mul(100)
                .round()
                .astype(int)
            )
            df = df.drop(columns=["Amount"])

            assert pd.api.types.is_integer_dtype(
                df["amountCents"]
            ), "amountCents not integer type"

            logging.info(f"Parsed {len(df)} rows from {url}")
            return {"sheet_url": url, "rows": len(df), "status": "OK", "df": df}

        except AssertionError as e:
            logging.error(f"Assertion failed for {url}: {e}")
            return {"sheet_url": url, "rows": 0, "status": f"Assertion failed: {e}"}
        except Exception as e:
            logging.exception(f"Unexpected error processing {url}")
            return {"sheet_url": url, "rows": 0, "status": f"Error: {e}"}


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
    parser.add_argument(
        "--sheet-url",
        help="Process only a single sheet by URL (overrides --from-cache and Drive lookup)",
    )
    parser.add_argument(
        "--exclude-regex",
        help="Regex to exclude rows based on Description column",
    )
    args = parser.parse_args()
    logging.getLogger().setLevel(getattr(logging, args.log_level.upper()))
    logging.debug(f"Log level set to {args.log_level.upper()}")

    if args.sheet_url:
        sheet_urls = [args.sheet_url]
        logging.info(f"Processing only single sheet: {args.sheet_url}")
    else:
        sheet_urls = getSheetUrls(args.from_cache)

    parser = SheetsParser(CREDENTIALS_FILE, exclude_regex=args.exclude_regex)
    parser.parseSheets(sheet_urls)


if __name__ == "__main__":
    main()
