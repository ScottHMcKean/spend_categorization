#!/usr/bin/env bash
# One-shot deploy: build the React app, deploy bundle (jobs + database + app),
# then deploy app source code.
#
# Usage:
#   ./scripts/deploy.sh                # deploy + run pipeline (dev)
#   ./scripts/deploy.sh --skip-run     # deploy only, skip pipeline run
#   ./scripts/deploy.sh -t prod        # deploy to prod target
#   ./scripts/deploy.sh --app-only     # only build + deploy the app
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

SKIP_RUN=0
APP_ONLY=0
BUNDLE_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-run) SKIP_RUN=1; shift ;;
    --app-only) APP_ONLY=1; shift ;;
    *) BUNDLE_ARGS+=("$1"); shift ;;
  esac
done

echo "==> Building React frontend (apx build)"
APX_BUILD_FLAGS=()
if ! curl -s -m 3 -o /dev/null https://registry.npmjs.org/; then
  echo "    npm registry unreachable -- skipping UI build (will use last built __dist__)"
  APX_BUILD_FLAGS+=("--skip-ui-build")
fi
(cd packages/app && apx build "${APX_BUILD_FLAGS[@]}")

echo "==> Building root spend-categorization wheel"
uv build --wheel --out-dir packages/app/.build >/dev/null

echo "==> Stitching requirements.txt for app deploy"
ROOT_WHEEL="$(ls packages/app/.build/spend_categorization-*.whl | xargs -n1 basename | head -1)"
APP_WHEEL="$(ls packages/app/.build/spend_app-*.whl | xargs -n1 basename | head -1)"
{
  echo "$ROOT_WHEEL"
  echo "$APP_WHEEL"
} > packages/app/.build/requirements.txt

echo "==> Deploying bundle"
databricks bundle deploy "${BUNDLE_ARGS[@]}"

echo "==> Deploying app source"
databricks apps deploy "$(databricks bundle summary -o json | jq -r '.resources.apps.spend_app.name')" \
  --source-code-path "$(databricks bundle summary -o json | jq -r '.resources.apps.spend_app.source_code_path')"

if [[ $APP_ONLY -eq 1 ]]; then
  echo "==> --app-only set, skipping pipeline run"
  exit 0
fi

if [[ $SKIP_RUN -eq 0 ]]; then
  echo "==> Running pipeline"
  databricks bundle run spend_categorization_pipeline "${BUNDLE_ARGS[@]}"
fi

echo "==> Done."
