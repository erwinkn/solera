# The bucket probe and benchmark image (bench/railway): the repo with its dev
# dependencies and a release build of the extension.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates gcc libc6-dev git \
    && curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
ENV PATH="/root/.cargo/bin:/home/app/.venv/bin:$PATH" SOLERA_BUILD=probe
WORKDIR /home/app
COPY . .
RUN uv sync --locked --extra server --extra postgres
CMD ["sh", "bench/railway/run.sh"]
