import hashlib
import json
from pathlib import Path

import duckdb
import requests
from tenacity import retry, wait_exponential
from tqdm import tqdm

con = duckdb.connect(":memory:")
q = """
WITH unique_heds_per_test AS (
SELECT
    clickability_test_id,
    COUNT(DISTINCT headline) AS num_headlines
FROM
    'data/upworthy_exploratory.csv'
GROUP BY 
    1
),
multi AS (
SELECT clickability_test_id FROM unique_heds_per_test WHERE num_headlines > 1
)
SELECT
    clickability_test_id,
    headline,
    slug,
    first_place
FROM
    'data/upworthy_exploratory.csv'
WHERE
    clickability_test_id IN (SELECT * FROM multi)
"""

df = con.execute(q).fetchdf()

df["slug_truncated"] = df["slug"].apply(lambda x: "-".join(x.split("-")[:-2]))
df["slug_pairs"] = df.apply(lambda x: (x["slug_truncated"], x["first_place"]), axis=1)


def get_pair_id(slug_truncated, first_place):
    """Create a unique identifier for a slug pair."""
    pair_str = f"{slug_truncated}_{first_place}"
    return hashlib.md5(pair_str.encode()).hexdigest()


def load_processed_pairs(output_file):
    """Load previously processed pair IDs from file."""
    if not Path(output_file).exists():
        return set()

    processed = set()
    with open(output_file, "r") as f:
        for line in f:
            try:
                data = json.loads(line)
                processed.add(data["id"])
            except:
                continue
    return processed


def save_result(output_file, pair_id, url):
    """Save result to output file."""
    with open(output_file, "a") as f:
        json.dump({"id": pair_id, "url": url}, f)
        f.write("\n")


@retry(wait=wait_exponential(multiplier=1, min=4, max=10))
def get_archive_url(slug):
    r = requests.get(
        "http://web.archive.org/cdx/search/cdx",
        params={"url": f"https://www.upworthy.com/{slug}", "output": "json"},
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        },
    )

    if r.status_code != 200:
        return None

    r_json = r.json()
    if len(r_json) < 2:
        return None

    return f"http://web.archive.org/web/{r_json[1][1]}/{r_json[1][2]}"


OUTPUT_FILE = "data/wayback_urls.jsonl"
processed_pairs = load_processed_pairs(OUTPUT_FILE)

archive_urls = []
for slug_trunc, slug_full in tqdm(df.slug_pairs):
    pair_id = get_pair_id(slug_trunc, slug_full)

    if pair_id in processed_pairs:
        continue

    url = get_archive_url(slug_full)
    if url is None:
        url = get_archive_url(slug_trunc)

    save_result(OUTPUT_FILE, pair_id, url)
