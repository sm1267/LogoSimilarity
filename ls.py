import pandas as pd
import requests
from PIL import Image
from io import BytesIO
from bs4 import BeautifulSoup
import imagehash

PARQUET_FILE = "logos.snappy.parquet"
CSV_OUTPUT = "logo_hash_results.csv"
DOMAIN_COLUMN = "domain"
HASH_DISTANCE_THRESHOLD = 10

headers = {"User-Agent": "Mozilla/5.0"}


def is_image_response(r):
    return r.ok and r.headers.get("Content-Type", "").startswith("image")

def fetch_image(url):
    try:
        r = requests.get(url, headers=headers, timeout=5)
        if is_image_response(r):
            return Image.open(BytesIO(r.content))
    except:
        pass
    return None


def fetch_clearbit(domain):
    return fetch_image(f"https://logo.clearbit.com/{domain}")

def fetch_favicon(domain):
    for scheme in ["https", "http"]:
        img = fetch_image(f"{scheme}://{domain}/favicon.ico")
        if img:
            return img
    return None

def fetch_google_favicon(domain):
    return fetch_image(f"https://www.google.com/s2/favicons?sz=64&domain={domain}")

def fetch_from_html(domain):
    for scheme in ["https", "http"]:
        try:
            base = f"{scheme}://{domain}"
            r = requests.get(base, headers=headers, timeout=5)
            if r.ok:
                soup = BeautifulSoup(r.text, "html.parser")
                img_tag = soup.find("img", {"alt": lambda x: x and 'logo' in x.lower()})
                if img_tag and img_tag.get("src"):
                    src = img_tag["src"]
                    if src.startswith("//"): src = scheme + ":" + src
                    elif src.startswith("/"): src = base + src
                    elif not src.startswith("http"): src = f"{base}/{src}"
                    return fetch_image(src)
                link_tag = soup.find("link", rel=lambda x: x and 'icon' in x.lower())
                if link_tag and link_tag.get("href"):
                    href = link_tag["href"]
                    if href.startswith("//"): href = scheme + ":" + href
                    elif href.startswith("/"): href = base + href
                    elif not href.startswith("http"): href = f"{base}/{href}"
                    return fetch_image(href)
        except:
            continue
    return None


def compute_hash(img):
    try:
        return str(imagehash.phash(img))
    except:
        return None

def hamming_distance(h1, h2):
    return bin(int(h1, 16) ^ int(h2, 16)).count("1")

def group_hashes(hash_dict, threshold=HASH_DISTANCE_THRESHOLD):
    used = set()
    groups = []

    items = list(hash_dict.items())
    for i, (d1, h1) in enumerate(items):
        if d1 in used:
            continue
        group = [d1]
        used.add(d1)
        for j in range(i+1, len(items)):
            d2, h2 = items[j]
            if d2 not in used and hamming_distance(h1, h2) <= threshold:
                group.append(d2)
                used.add(d2)
        groups.append(group)
    return groups


df = pd.read_parquet(PARQUET_FILE)
domains = df[DOMAIN_COLUMN].dropna().unique()

results = []
hash_dict = {}

strategies = [
    ("clearbit", fetch_clearbit),
    ("favicon", fetch_favicon),
    ("google_s2", fetch_google_favicon),
    ("html_parse", fetch_from_html),
]

success = 0

for i, domain in enumerate(domains):
    print(f"[{i+1}/{len(domains)}] {domain}...")
    for name, method in strategies:
        img = method(domain)
        if img:
            hash_val = compute_hash(img)
            if hash_val:
                results.append((domain, "success", name, hash_val))
                hash_dict[domain] = hash_val
                success += 1
                break
    else:
        results.append((domain, "fail", None, None))

results_df = pd.DataFrame(results, columns=["domain", "status", "method", "hash"])
results_df.to_csv(CSV_OUTPUT, index=False)

groups = group_hashes(hash_dict, threshold=10)

print(f"\n Logo-uri extrase: {success}/{len(domains)} ({success / len(domains):.2%})")
print(f"Grupuri: {len(groups)}")


for i, g in enumerate(groups[:50]):
    print(f"\nGrup {i+1}:")
    for domain in g:
        print("  -", domain)


