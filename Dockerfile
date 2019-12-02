FROM python:3.7
COPY . .
RUN apt-get update && apt-get install -y python-pip
RUN pip install -r requirements.txt