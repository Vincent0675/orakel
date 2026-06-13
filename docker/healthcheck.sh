#!/usr/bin/env bash
# Health check script for Orakel services.
# Each service can override the check via the HEALTHCHECK CMD in compose.

set -euo pipefail

# Default: verify Python can import the project
if python -c "import orakel; import pyspark; print('ok')" > /dev/null 2>&1; then
    exit 0
fi

exit 1
