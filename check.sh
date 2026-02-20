#!/bin/bash
set -euo pipefail

echo "=== Format ==="
pnpm exec biome format --write .

echo ""
echo "=== Lint ==="
pnpm exec biome check .

echo ""
echo "=== Typecheck ==="
pnpm typecheck

echo ""
echo "=== Typecheck (tests) ==="
pnpm typecheck:tests

echo ""
echo "=== Tests ==="
pnpm test

echo ""
echo "All checks passed."
