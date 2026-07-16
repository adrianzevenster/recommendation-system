#!/usr/bin/env bash
set -euo pipefail

curl -s http://localhost:8000/recommendations/u001 | python -m json.tool
