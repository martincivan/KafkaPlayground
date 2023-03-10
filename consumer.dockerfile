FROM python

COPY "requirements.txt" ./
RUN pip install -r requirements.txt
COPY consumer.py ./

ENTRYPOINT python consumer.py