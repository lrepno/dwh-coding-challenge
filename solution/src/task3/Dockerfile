from python:3.8-buster

COPY . .

RUN pip3 install -r solution/src/task2/requirements.txt

WORKDIR solution/src

ENV DATA_PATH ../../data
ENV LOG_LEVEL INFO
ENV PYTHONPATH "${PYTHONPATH}:."

CMD [ "python3", "task3/task3.py"]
