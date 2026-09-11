FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir . && useradd --create-home --uid 10001 app && mkdir -p /home/app/state && chown app:app /home/app/state
USER app
ENV PYTHONUNBUFFERED=1 PORT=8000
EXPOSE 8000
CMD ["sh", "-c", "if [ \"${DORC_SELFTEST:-0}\" = 1 ]; then dorc selftest || exit 1; fi; exec dorc serve --host 0.0.0.0"]
