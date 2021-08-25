from python:3.8-buster

COPY . .

RUN pip3 install -r solution/src/task1/requirements.txt

ENV DATA_PATH ./data
ENV LOG_LEVEL INFO
CMD [ "python3", "solution/src/task1/task1.py"]
