FROM python:3.9

COPY . .

RUN python -m pip install --upgrade pip

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python3", "crawler_run.py"]
