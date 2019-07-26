#!/usr/bin/env bash
set -euo pipefail

black --check tests bedrock_plugin.py && \
flake8 bedrock_plugin.py tests && \
pytest -vv tests
