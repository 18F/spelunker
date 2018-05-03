import csv
import pytest


def test_gather_url_404(client, db):
    resp = client.get("/api/v1/gather/whatever")
    assert resp.status_code == 404


@pytest.mark.parametrize("suffix,base", [
    (".myhealth.va.gov", "va.gov"),
    (".amedd.army.mil", "army.mil"),
], ids=repr)
def test_gather_post_url(app, client, db, suffix, base):
    app.testing = True
    resp = client.post("/api/v1/gather", data={
        "suffix": suffix,
        "url": "https://analytics.usa.gov/data/live/sites.csv",
    })
    assert resp.status_code == 200
    lines = resp.data.decode(encoding="utf-8").split("\r\n")
    assert lines[0] == "Domain,Base Domain"
    reader = csv.reader(lines[1:])
    for row in reader:
        for cell in row:
            assert cell.endswith(base)
            assert row[0].endswith(suffix)


def test_gather_post_url_no_suffix(app, client, db):
    app.testing = True
    resp = client.post("/api/v1/gather", data={
        "url": "https://analytics.usa.gov/data/live/sites.csv",
    })
    assert resp.status_code == 400
