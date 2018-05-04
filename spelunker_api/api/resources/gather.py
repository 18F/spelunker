from pathlib import Path
from typing import List
from urllib.parse import urlparse
from celery import Celery, chord
from flask import Flask, send_file
from flask_restful import reqparse, Resource
from publicsuffix import PublicSuffixList
from spelunker_api.extensions import db
from spelunker_api.models import Url
from spelunker_api.tasks import load_url, handle_downloaded_urls, add, addcb
import spelunker_api.tasks
import csv
import datetime
import re
import requests
import uuid
import werkzeug

CSV_CACHE_REFRESH = 3600  # 1 hour
PUBLIC_SUFFIX_LIST_URL = 'https://publicsuffix.org/list/public_suffix_list.dat'
PSL_CACHE_REFRESH = 86400  # 1 day



'''
@celery.task()
def sample_task(a, b):

    return a + b
'''


class GatherHome(Resource):
    """Single object resource
    """

    def get(self) -> dict:
        return {"gatherers": "list of available gatherers goes here?"}

    def post(self) -> str:
        current_app = Flask("spelunker_api")
        self.STORAGE = Path(current_app.root_path, "storage").resolve()
        parser = reqparse.RequestParser()
        parser.add_argument("gatherers", type=str,
                            help="List of gatherers to use",
                            location="form", action="append")
        parser.add_argument("url", type=str,
                            help="List of URLs to gather files from",
                            location="form", action="append")
        parser.add_argument("suffix", type=str,
                            help="Required list of domain suffixes to gather",
                            location="form", action="append", required=True)
        parser.add_argument("sort", type=bool,
                            help="Required list of domain suffixes to gather",
                            location="form", action="store_true")
        parser.add_argument("file", type=werkzeug.datastructures.FileStorage,
                            location="files", action="append")
        args = {k: v for (k, v) in parser.parse_args(strict=True).items() if v
                is not None}

        domains = set()

        # fnames = [load_url(self.STORAGE, url, CSV_CACHE_REFRESH) for url in args["url"]]
        urls_to_fetch = []
        urls_fetched = []
        for url in args["url"]:
            if current_app.testing:
                if url == "https://analytics.usa.gov/data/live/sites.csv":
                    print("returning test_sites.csv")
                    urls_fetched.append("test_sites.csv")
                    continue
                elif url == PUBLIC_SUFFIX_LIST_URL:
                    print("returning test_public_suffix.csv")
                    urls_fetched.append("test_public_suffix.csv")
                    continue
                else:
                    raise

            dburl = Url.query.filter_by(url=url).first()
            if not dburl:
                urls_to_fetch.append(url)
            else:
                delta = datetime.datetime.utcnow() - dburl.timestamp
                if current_app.testing or (delta.seconds < interval):
                    print(current_app.testing)
                    print("Cache hit")
                    urls_fetched.append(dburl.filestorage)
                else:
                    print("Cache entry too old")
                    urls_to_fetch.append(url)
                    """
                    response = requests.get(url, stream=True)
                    response.raise_for_status()
                    dest = Path(storage, dburl.filestorage)
                    with dest.open("wb") as f:
                        for block in response.iter_content(1024):
                            f.write(block)
                    dburl.timestamp = datetime.datetime.utcnow()
                    db.session.commit()
                    return dburl.filestorage
                    """


        """
        res = chord((load_url.s(str(self.STORAGE), url, CSV_CACHE_REFRESH,
                                PUBLIC_SUFFIX_LIST_URL) for url in
                     urls_to_fetch), handle_downloaded_urls.s())()
        res.get()
        """
        res = chord((add.s(_) for _ in range(10)), addcb.s())()
        res.get()


        """
        suffixes = args.get("suffix", [])

        if "file" in args:
            import pdb
            pdb.set_trace()

        # accumulate list of sources
        # for now, assume series of URLs.

        for fname in fnames:
            fpath = Path(self.STORAGE, fname).resolve()
            domains.update(load_domains(fpath))

        domains = list(domains)
        # TODO: add option to sort the domains.
        if args.get("sort", False):
            domains.sort()
        headers = ["Domain", "Base Domain"]
        stamp = int(datetime.datetime.now().timestamp() * 1_000_000)
        outpath = Path(self.STORAGE, "%s.csv" % stamp)
        psl_fname = load_url(self.STORAGE, PUBLIC_SUFFIX_LIST_URL,
                             PSL_CACHE_REFRESH)
        pslf = Path(self.STORAGE, psl_fname).resolve().open(encoding="utf-8")
        psl = PublicSuffixList(pslf)

        with outpath.open("w") as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(headers)
            for domain in domains:
                if len(suffixes):
                    if not any(domain.endswith(_) for _ in suffixes):
                        continue
                row = [domain, psl.get_public_suffix(domain)]
                csv_writer.writerow(row)
        return send_file(str(outpath.resolve()), mimetype="text/x-csv")
        """






def load_domains(domain_csv: Path) -> List[str]:
    domains = []  # type: List[str]
    with domain_csv.open(newline='') as csvfile:
        for row in csv.reader(csvfile):
            # Skip empty rows.
            if (not row) or (not row[0].strip()):
                continue

            row[0] = row[0].lower().strip()

            # Skip any header row.
            if (not domains) and (row[0].lower().startswith("domain")):
                continue

            domains.append(row[0])
    return domains

"""
"""
