FROM python:3.6.4
MAINTAINER anonymouse

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
