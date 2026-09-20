FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /home/app

COPY pyproject.toml uv.lock ./
COPY packages packages
COPY apps/server apps/server
COPY apps/worker apps/worker

RUN uv sync --locked --no-dev --no-editable

ENV PATH="/home/app/.venv/bin:$PATH" \
    DORC_STATE_URL=file:///home/app/state

EXPOSE 8000

CMD ["dorc", "serve", "--host", "0.0.0.0"]
