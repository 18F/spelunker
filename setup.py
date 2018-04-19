from setuptools import setup, find_packages

__version__ = '0.1'


setup(
    name='spelunker_api',
    version=__version__,
    packages=find_packages(exclude=['tests']),
    install_requires=[
        'flask',
        'flask-sqlalchemy',
        'flask-restful',
        'flask-migrate',
        'flask-jwt-extended',
        'flask-marshmallow',
        'marshmallow-sqlalchemy',
        'passlib'
    ],
    entry_points={
        'console_scripts': [
            'spelunker_api = spelunker_api.manage:cli'
        ]
    }
)
