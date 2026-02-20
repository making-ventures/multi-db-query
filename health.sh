#!/bin/bash
set -euo pipefail

echo "=== Gitleaks (git history) ==="
gitleaks git . --verbose

echo ""
echo "=== Gitleaks (directory) ==="
gitleaks dir . --verbose

echo ""
echo "=== Dependency checks ==="
bash renovate-check.sh

echo ""
echo "=== Vulnerability audit ==="
pnpm audit

echo ""
echo "Health checks passed."
