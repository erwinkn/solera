FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS build

# The Rust toolchain builds the `solera._native` extension; the image only keeps the venv.
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates gcc libc6-dev \
    && curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
ENV PATH="/root/.cargo/bin:$PATH"

WORKDIR /home/app
COPY pyproject.toml uv.lock README.md ./
COPY native native
COPY python python
RUN uv sync --locked --no-dev --no-editable --extra postgres

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /home/app
COPY --from=build /home/app/.venv .venv

ENV PATH="/home/app/.venv/bin:$PATH" \
    SOLERA_STATE_URL=file:///home/app/state \
    SOLERA_DATA=/home/app/data

EXPOSE 8000

CMD ["solera", "serve", "--host", "0.0.0.0"]
