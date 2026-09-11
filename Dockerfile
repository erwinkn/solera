FROM python:3.12-slim AS builder
WORKDIR /app
RUN pip install --no-cache-dir uv==0.12.13
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable

FROM python:3.12-slim
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
RUN useradd --create-home --uid 10001 app && mkdir -p /home/app/state && chown app:app /home/app/state
USER app
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PORT=8000
EXPOSE 8000
CMD ["sh", "-c", "if [ \"${DORC_SELFTEST:-0}\" = 1 ]; then dorc selftest || exit 1; fi; exec dorc serve --host 0.0.0.0"]
