from spelunker_api.extensions import db
import datetime


class Url(db.Model):
    """URL in cache."""
    id = db.Column(db.Integer, primary_key=True)
    url = db.Column(db.String(2048), unique=True, nullable=False)
    filename = db.Column(db.String(2048), unique=False, nullable=False)
    filestorage = db.Column(db.String(2048), unique=True, nullable=False)
    timestamp = db.Column(db.DateTime, default=datetime.datetime.utcnow)

    def __repr__(self):
        return "<URL %s, %s, %s>" % (self.url, self.filename,
                                     self.filestorage)
