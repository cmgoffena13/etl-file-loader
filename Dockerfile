FROM python:3.12-slim-bookworm as build

ENV PYTHONBUFFERED=1 UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && apt-get upgrade -y openssl

ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh && rm /uv-installer.sh

ENV PATH="/root/.local/bin:$PATH"
WORKDIR /fileloader

COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-install-project --no-dev

COPY . .
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev

RUN groupadd -r fileloader && useradd --no-log-init -r -g fileloader fileloader && \
    chown -R fileloader:fileloader /fileloader /root/.cache/uv

FROM python:3.12-slim-bookworm as runtime
WORKDIR /fileloader
COPY --from=build /fileloader /fileloader
ENV PATH="/root/.local/bin:$PATH"

RUN chown -R fileloader:fileloader /fileloader
USER fileloader

CMD ["uv", "run", "--", "python", "main.py"]
