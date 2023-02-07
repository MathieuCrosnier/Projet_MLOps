FROM debian:latest
WORKDIR /home
ADD requirements.txt .
RUN apt-get update && apt-get install python3-pip -y && apt-get install curl -y && pip install -r requirements.txt
RUN mkdir API
WORKDIR /home/API
ADD API .
EXPOSE 8000
CMD uvicorn main:api --host 0.0.0.0
