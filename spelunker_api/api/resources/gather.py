from pathlib import Path
from typing import List
from urllib.parse import urlparse
from flask import current_app, send_file
from flask_restful import reqparse, Resource
from publicsuffix import PublicSuffixList
from spelunker_api.extensions import db
from spelunker_api.models import Url
import csv
import datetime
import re
import requests
import uuid

CSV_CACHE_REFRESH = 3600  # 1 hour
PUBLIC_SUFFIX_LIST_URL = 'https://publicsuffix.org/list/public_suffix_list.dat'
PSL_CACHE_REFRESH = 86400  # 1 day


class GatherHome(Resource):
    """Single object resource
    """

    def get(self) -> dict:
        return {"gatherers": "list of available gatherers goes here?"}

    def post(self) -> str:
        self.STORAGE = Path(current_app.root_path, "storage")
        parser = reqparse.RequestParser()
        parser.add_argument("gatherers", type=str,
                            help="List of gatherers to use",
                            location="form", action="append")
        parser.add_argument("url", type=str,
                            help="List of URLs to gather files from",
                            location="form", action="append")
        args = {k: v for (k, v) in parser.parse_args(strict=True).items() if v
                is not None}
        fnames = [self.load_url(url, CSV_CACHE_REFRESH) for url in args["url"]]

        # accumulate list of sources
        # for now, assume series of URLs.

        domains = []
        for fname in fnames:
            fpath = Path(self.STORAGE, fname).resolve()
            domains.extend(load_domains(fpath))

        # TODO: add option to sort the domains.
        headers = ["Domain", "Base Domain"]
        stamp = int(datetime.datetime.now().timestamp() * 1_000_000)
        outpath = Path(self.STORAGE, "%s.csv" % stamp)
        psl_fname = self.load_url(PUBLIC_SUFFIX_LIST_URL, PSL_CACHE_REFRESH)
        pslf = Path(self.STORAGE, psl_fname).resolve().open(encoding="utf-8")
        psl = PublicSuffixList(pslf)

        with outpath.open("w") as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(headers)
            for domain in domains:
                row = [domain, psl.get_public_suffix(domain)]
                csv_writer.writerow(row)
        return send_file(str(outpath.resolve()), mimetype="text/x-csv")

    def load_url(self, url: str, interval: int) -> str:
        print("Fetching: %s" % url)
        response = requests.get(url, stream=True)
        response.raise_for_status()
        fname = get_filename_from_cd(response, url)
        if not fname:
            parsed = urlparse(url)
            fname = Path(parsed.path).name
        dburl = Url.query.filter_by(url=url).first()
        if not dburl:
            print("No cache hit")
            print("Fetching: %s" % url)
            response = requests.get(url, stream=True)
            response.raise_for_status()
            fname = get_filename_from_cd(response, url)
            filestorage = "%s.csv" % uuid.uuid4().hex
            dest = Path(current_app.root_path, "storage", filestorage)
            with dest.open("wb") as f:
                for block in response.iter_content(1024):
                    f.write(block)
            new_dburl = Url(url=url, filename=fname, filestorage=filestorage)
            db.session.add(new_dburl)
            db.session.commit()
            return filestorage
        else:
            print("Possible cache hit")
            delta = datetime.datetime.utcnow() - dburl.timestamp
            if delta.seconds < interval:
                print("Cache hit")
                return dburl.filestorage
            else:
                print("Cache entry too old")
                response = requests.get(url, stream=True)
                response.raise_for_status()
                dest = Path(self.STORAGE, dburl.filestorage)
                with dest.open("wb") as f:
                    for block in response.iter_content(1024):
                        f.write(block)
                dburl.timestamp = datetime.datetime.utcnow()
                db.session.commit()
                return dburl.filestorage


def get_filename_from_cd(response: requests.Response, url: str) -> str:
    """
    Get filename from content-disposition with fallback to last part of URL.
    """
    cd = response.headers.get("content-disposition")
    if cd:
        fname = re.findall('filename=(.+)', cd)
        if len(fname):
            return fname[0]
    parsed = urlparse(url)
    return Path(parsed.path).name


def load_domains(domain_csv: Path) -> List[str]:
    domains = []  # type: List[str]
    with domain_csv.open(newline='') as csvfile:
        for row in csv.reader(csvfile):
            # Skip empty rows.
            if (not row) or (not row[0].strip()):
                continue

            row[0] = row[0].lower().strip()

            # Skip any header row.
            if (not domains) and (row[0].startswith("domain")):
                continue

            domains.append(row[0])
    return domains
