FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    WQ_DATA_DIR=/app/data \
    WQ_DOWNSTREAM_DIR=/app/downstream

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY wq_publish ./wq_publish
COPY wsgi.py ./

# 数据目录与下游目录由 compose 挂载；以非 root 用户运行更安全
RUN useradd --create-home --uid 10001 appuser \
    && mkdir -p /app/data /app/downstream \
    && chown -R appuser:appuser /app
USER appuser

EXPOSE 5000

# 单 worker 多线程：并发写正确性依赖同一 SQLite 文件与同目录原子改名
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "1", "--threads", "8", \
     "--access-logfile", "-", "wsgi:create_app()"]
