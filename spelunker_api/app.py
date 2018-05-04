from celery import Celery
from flask import Flask

from spelunker_api import api
from spelunker_api.extensions import db, jwt, migrate


def create_app(config=None, testing=False, cli=False):
    """Application factory, used to create application
    """
    app = Flask('spelunker_api')

    configure_app(app, testing)
    configure_extensions(app, cli)
    register_blueprints(app)

    return app


def configure_app(app, testing=False):
    """set configuration for application
    """
    # default configuration
    app.config.from_object('spelunker_api.config')

    if testing is True:
        # override with testing config
        app.config.from_object('spelunker_api.configtest')
    else:
        # override with env variable, fail silently if not set
        app.config.from_envvar("SPELUNKER_API_CONFIG", silent=True)


def configure_extensions(app, cli):
    """configure flask extensions
    """
    db.init_app(app)
    jwt.init_app(app)

    if cli is True:
        migrate.init_app(app, db)


def register_blueprints(app):
    """register all blueprints for application
    """
    app.register_blueprint(api.views.blueprint)


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
    CELERY_RESULT_BACKEND='redis://persistent_redis:6379'
)
celery = make_celery(current_app)

