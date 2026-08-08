# Syrup Controller (Python)

[![CI](https://github.com/heia-fr/syrup-controller/actions/workflows/ci.yml/badge.svg)](https://github.com/heia-fr/syrup-controller/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![GHCR](https://img.shields.io/badge/GHCR-ghcr.io%2Fheia--fr%2Fsyrup--controller-blue)](https://ghcr.io/heia-fr/syrup-controller)

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
