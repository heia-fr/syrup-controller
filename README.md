# Syrup Controller (Python)

[![CI](https://github.com/heia-fr/syrup-controller/actions/workflows/ci.yml/badge.svg)](https://github.com/heia-fr/syrup-controller/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.12%2B-blue.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![GHCR](https://img.shields.io/badge/GHCR-ghcr.io%2Fheia--fr%2Fsyrup--controller-blue)](https://ghcr.io/heia-fr/syrup-controller)

Python service that connects a syrup machine controller to MQTT.

## Requirements

- Python 3.12+
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

## MQTT Interface

Topics are relative to the base topic (`--mqtt-base-topic`). Syrups are numbered 1 to 3.

| Topic      | Payload                                                  | Effect                                           |
| ---------- | -------------------------------------------------------- | ------------------------------------------------ |
| `pour`     | `{"syrup": 1, "check_cup": true, "water_only": false}`   | Pour for the configured duration                 |
| `stop`     | `{"syrup": 1}`                                           | Stop one pour                                    |
| `rinse`    | `{"duration": 30}`                                       | Rinse for up to 3600 s; zero or less: until stopped |
| `stop-all` | Ignored                                                  | Stop all pumps                                   |
| `reset`    | Ignored                                                  | Reset the pump board                             |
| `button`   | `1` to `3`: pour, `8`: rinse, `9`: reset (plain integer) | Same as the matching command                     |

`check_cup` and `water_only` are optional. `check_cup` defaults to the service setting
(`--ignore-cups-check`) and may override it either way, e.g. for a defective sensor or
a special type of cup. If the cup state cannot be read, pours that check their cup stop.

The service publishes the syrup number as a plain integer on `done` when a pour ends,
except after `stop`. Invalid payloads are logged and ignored.

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
