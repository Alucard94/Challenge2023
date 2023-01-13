FROM python:3.7.4-slim-buster

# Install required libraries
RUN apt-get update && apt-get -y install libpq-dev \
    python-dev 

COPY . /
RUN ls -la /

RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /

ENTRYPOINT ["python", "src/main.py"]
