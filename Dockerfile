FROM python:3.10
ADD app.py .

RUN pip install pandas selenium peewee

CMD ["python", "./app.py"]