FROM python:3.8

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir materialized_views custom
COPY materialized_views ./materialized_views
COPY *.py ./
RUN chown 1000:1000 *.py

ENTRYPOINT ["python"]
CMD ["/usr/src/app/etl_main.py"]