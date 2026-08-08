# Syrup Controller (Python)

Python service that connects a syrup machine controller to MQTT.

## Requirements

- Python 3.11+
- uv

## Quick Start

1. Install dependencies:

```bash
uv sync --all-groups
```

2. Run the CLI:

```bash
uv run syrup-controller --help
```

3. Run with simulator mode:

```bash
uv run syrup-controller --simulator
```

## Development

Run lint, type checks, and tests:

```bash
uvx ruff check .
uv run pyright
uv run pytest -q
```

Run pre-commit on all files:

```bash
uvx pre-commit run --all-files
```

## Docker

Build image:

```bash
docker build -t syrup-controller .
```

Run image:

```bash
docker run --rm syrup-controller --help
```

## Notes

- Dependencies are locked with uv.lock for reproducible builds.
- CI runs checks, tests, package build, and Docker build.
