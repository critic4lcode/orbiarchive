import requests
import json
import time
import math
from urllib.parse import quote

BASE_URL = "https://kormany.hu/application/document-groups"
ITEMS_PER_PAGE = 18
TOTAL_ITEMS = 5146
TOTAL_PAGES = math.ceil(TOTAL_ITEMS / ITEMS_PER_PAGE)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "hu-HU,hu;q=0.9,en-US;q=0.8,en;q=0.7",
}


def build_url(page: int, items_per_page: int = ITEMS_PER_PAGE) -> str:
    filter_param = quote('{"categories":[],"ministries":[]}', safe='')
    pagination_param = quote(f'{{"itemsPerPage":{items_per_page},"page":{page}}}', safe='')
    return f"{BASE_URL}?filter={filter_param}&pagination={pagination_param}"


def fetch_page(session: requests.Session, page: int, retries: int = 3, delay: float = 2.0) -> dict | None:
    url = build_url(page)
    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            print(f"  [HTTP Error] Page {page}, attempt {attempt}/{retries}: {e}")
        except requests.exceptions.ConnectionError as e:
            print(f"  [Connection Error] Page {page}, attempt {attempt}/{retries}: {e}")
        except requests.exceptions.Timeout:
            print(f"  [Timeout] Page {page}, attempt {attempt}/{retries}")
        except json.JSONDecodeError as e:
            print(f"  [JSON Error] Page {page}, attempt {attempt}/{retries}: {e}")

        if attempt < retries:
            time.sleep(delay * attempt)  # exponential-ish backoff

    print(f"  [FAILED] Could not fetch page {page} after {retries} attempts.")
    return None


def paginate_all(output_file: str = "all_document_groups.json", delay_between_requests: float = 0.5):
    all_items = []
    failed_pages = []
    total_pages = TOTAL_PAGES

    print(f"Starting pagination: {TOTAL_ITEMS} items, {total_pages} pages ({ITEMS_PER_PAGE} items/page)")
    print(f"Output file: {output_file}")
    print("-" * 60)

    with requests.Session() as session:
        for page in range(1, total_pages + 1):
            print(f"Fetching page {page}/{total_pages}...", end=" ", flush=True)

            data = fetch_page(session, page)

            if data is None:
                failed_pages.append(page)
                print(f"FAILED")
                continue

            items = data.get("data", [])
            all_items.extend(items)

            # Read actual total from first page meta
            if page == 1:
                meta = data.get("meta", {}).get("pagination", {})
                actual_total = meta.get("itemsTotal", TOTAL_ITEMS)
                if actual_total != TOTAL_ITEMS:
                    print(f"\n  [INFO] API reports {actual_total} total items (expected {TOTAL_ITEMS})")
                    total_pages = math.ceil(actual_total / ITEMS_PER_PAGE)
                    print(f"  [INFO] Adjusted to {total_pages} total pages")

            print(f"OK ({len(items)} items, total so far: {len(all_items)})")

            # Be polite to the server
            if page < total_pages:
                time.sleep(delay_between_requests)

    print("-" * 60)
    print(f"Pagination complete.")
    print(f"  Total items collected : {len(all_items)}")
    print(f"  Failed pages          : {failed_pages if failed_pages else 'None'}")

    # Save results
    result = {
        "meta": {
            "total_items": len(all_items),
            "total_pages": total_pages,
            "items_per_page": ITEMS_PER_PAGE,
            "failed_pages": failed_pages,
        },
        "data": all_items
    }

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"  Saved to              : {output_file}")

    # Retry failed pages
    if failed_pages:
        print(f"\nRetrying {len(failed_pages)} failed pages...")
        retry_results = []
        with requests.Session() as session:
            for page in failed_pages:
                print(f"  Retrying page {page}...", end=" ", flush=True)
                data = fetch_page(session, page, retries=5, delay=5.0)
                if data:
                    items = data.get("data", [])
                    retry_results.extend(items)
                    print(f"OK ({len(items)} items)")
                else:
                    print("STILL FAILED")
                time.sleep(1.0)

        if retry_results:
            all_items.extend(retry_results)
            result["data"] = all_items
            result["meta"]["total_items"] = len(all_items)
            result["meta"]["failed_pages"] = []
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            print(f"Updated file with {len(retry_results)} recovered items.")

    return all_items


if __name__ == "__main__":
    all_data = paginate_all(
        output_file="all_document_groups.json",
        delay_between_requests=0.5  # seconds between requests — increase if you get rate limited
    )
    print(f"\nDone. {len(all_data)} document groups collected.")
