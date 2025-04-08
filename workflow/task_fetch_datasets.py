from multiprocessing.pool import ThreadPool
import requests
#from tqdm import tqdm
#import hashlib
#import urllib.request
#import urllib.parse

MAX_THREADS = 10

urls = [
    "https://google.com/favicon.ico",
    "https://datasets.imdbws.com/title.basics.tsv.gz",
    "https://datasets.imdbws.com/title.principals.tsv.gz",
    "https://datasets.imdbws.com/name.basics.tsv.gz",
    "https://datasets.imdbws.com/title.ratings.tsv.gz",
    ...
]

def download(url):
    r = requests.get(url)
    with open(url, "wb") as f:
        f.write(r.content)

if __name__ == "__main__":
    with ThreadPool(MAX_THREADS) as p:
        p.map(download, urls)
