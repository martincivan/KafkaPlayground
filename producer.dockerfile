FROM python

COPY "requirements.txt" ./
RUN pip install -r requirements.txt
RUN pip install "uvicorn[standard]"

ENTRYPOINT uvicorn producer:app --host 0.0.0.0 --port 80