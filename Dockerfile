FROM python:3.12-slim-bookworm as build

RUN apt-get update && apt-get install -y gcc git curl

ENV VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH" \
    LANG=C.UTF-8 \
    PYTHONDONTWRITEBYTECODE=1

RUN python3 -m venv /opt/venv

RUN pip install --no-cache-dir cython
RUN pip install --no-cache-dir cryptofeed
RUN pip install --no-cache-dir redis
RUN pip install --no-cache-dir pymongo[srv]
RUN pip install --no-cache-dir motor
RUN pip install --no-cache-dir asyncpg
RUN pip install --no-cache-dir aio-pika

FROM python:3.12-slim-bookworm
COPY --from=build /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY cryptostore.py /cryptostore.py

CMD ["/cryptostore.py"]
ENTRYPOINT ["python"]