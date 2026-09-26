FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /home/app

COPY pyproject.toml uv.lock README.md ./
COPY packages packages
COPY apps/server apps/server
COPY apps/worker apps/worker

RUN uv sync --locked --no-dev --no-editable --extra postgres

ENV PATH="/home/app/.venv/bin:$PATH" \
    SOLERA_STATE_URL=file:///home/app/state \
    SOLERA_DATA=/home/app/data

EXPOSE 8000

CMD ["solera", "serve", "--host", "0.0.0.0"]
