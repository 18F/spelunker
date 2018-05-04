from pathlib import Path
from urllib.parse import urlparse
from celery import Celery
from flask import Flask
from spelunker_api.extensions import db
from spelunker_api.models import Url
import datetime
import re
import uuid
import requests


def make_celery(app):
    celery = Celery(app.import_name,
                    backend=app.config['CELERY_RESULT_BACKEND'],
                    broker=app.config['CELERY_BROKER_URL'])
    celery.conf.update(app.config)
    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery

current_app = Flask("spelunker_api")
current_app.config.update(
    CELERY_BROKER_URL='redis://persistent_redis:6379',
    CELERY_RESULT_BACKEND='redis://persistent_redis:6379',
    SQLALCHEMY_DATABASE_URI="sqlite:///spelunker_api.db",
    SQLALCHEMY_TRACK_MODIFICATIONS=False,
)
celery = make_celery(current_app)


@celery.task
def add(a):
    return a * a


@celery.task
def addcb(l):
    for item in l:
        print("item: ", l)


@celery.task
def load_url(storage: str, url: str, interval: int, psl_url) -> str:
    """
    from flask import current_app
    # print(current_app)
    # current_app = Flask("spelunker_api")
    # print(current_app.config["SQLALCHEMY_TRACK_MODIFICATIONS"])
    if current_app.testing:
        if url == "https://analytics.usa.gov/data/live/sites.csv":
            print("returning test_sites.csv")
            return "test_sites.csv"
        elif url == psl_url:
            print("returning test_public_suffix.csv")
            return "test_public_suffix.csv"
        else:
            raise
    """
    print("Fetching: %s" % url)
    response = requests.get(url, stream=True)
    response.raise_for_status()
    fname = get_filename_from_cd(response, url)
    if not fname:
        parsed = urlparse(url)
        fname = Path(parsed.path).name
    """
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
        if current_app.testing or (delta.seconds < interval):
            print(current_app.testing)
            print("Cache hit")
            return dburl.filestorage
        else:
            print("Cache entry too old")
    """
    response = requests.get(url, stream=True)
    response.raise_for_status()
    return response.content.decode("utf-8")
    """
    dest = Path(storage, dburl.filestorage)
    with dest.open("wb") as f:
        for block in response.iter_content(1024):
            f.write(block)
    dburl.timestamp = datetime.datetime.utcnow()
    db.session.commit()
    return dburl.filestorage
    """


def handle_downloaded_urls(results):
    for result in results:
        print(result)
    pass


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
