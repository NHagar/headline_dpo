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


archive_urls = []
for slug_trunc, slug_full in tqdm(df.slug_pairs):
    url = get_archive_url(slug_full)
    if url is None:
        url = get_archive_url(slug_trunc)

    archive_urls.append(url)
