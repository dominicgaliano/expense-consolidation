# Expense Consolidation

TODO:

- dynamically parse only files that didn't have failures
- create a csv reading class to use data?
- better reporting

COMPLETE:

- cache list of urls
- add rate limiting so that reads fit within quota

## Rate Limits

### Google Drive API:

- used to get URLs to expense sheets in expenses folder
- done using a single query, so limits are not of concern

### Sheets API:

- I have a limit of 60 requests per minute per user (on the GCP free tier)
- Client rate limiting is necessary to stay within this limit

The first thing I tried was the use the `BackOffHTTPClient` provided by `gspread`.
This was effective, but the overall parsing process took 4.5 minutes, most of which was spent waiting.
A problem with `BackOffHTTPClient` is that it uses exponential backoff with `delay = 2 ** retry_count`.
This meant the request waited for 2, 4, 8, 16, 32, etc... seconds when the rate limit was reached.
Depending on where in the 1 minute bucket we hit our rate limit, we could end up waiting up more time that necessary.

Hacky solution: Update `_MAX_BACKOFF` to 10 seconds.
This reduced the procesing time to 3 minutes.
