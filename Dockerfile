## Builder: use uv image to provision a managed Python and install dependencies
FROM ghcr.io/astral-sh/uv:bookworm-slim AS builder
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
ENV UV_PYTHON_INSTALL_DIR=/python
ENV UV_PYTHON_PREFERENCE=only-managed

# Install a managed Python version for the builder
RUN uv python install 3.13

WORKDIR /app

# Use build cache mounts to speed up dependency installs. Bind pyproject.toml so
# buildkit can cache dependency resolution separately from source changes.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --no-install-project --no-dev || true

COPY . /app

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-dev

## Final image: lightweight Debian bookworm-slim
FROM linuxserver/ffmpeg:8.0.1

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create nonroot user with specific UID/GID to avoid conflicts
RUN groupadd --system --gid 1000 nonroot \
 && useradd --system --gid 1000 --uid 1000 --create-home nonroot

# Copy the Python installation from the builder
COPY --from=builder --chown=python:python /python /python

# Copy the application from the builder (already installed deps in .venv)
COPY --from=builder --chown=nonroot:nonroot /app /app

# Place the virtualenv executables at the front of PATH
ENV PATH="/app/.venv/bin:$PATH"

USER nonroot
WORKDIR /app

ENV PORT=8000
# Default: run the FastAPI app via python main.py
EXPOSE 8000
ENTRYPOINT ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT} --workers 4"]
