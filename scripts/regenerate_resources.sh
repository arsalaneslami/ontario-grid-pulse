#!/usr/bin/env bash
# regenerate_resources.sh
# -----------------------
# Pulls the current state of the workspace pipelines and job into local YAML.
# Run this whenever the workspace resources have been modified outside the
# bundle and you want to sync them back to the local repo.

set -euo pipefail  # exit on error, undefined var, or failed pipe

# Resource IDs — update these if you create new pipelines/jobs
SILVER_PIPELINE_ID="1fc72349-8bf4-4f33-942c-542f5e918936"
GOLD_PIPELINE_ID="1820f592-87c1-430a-b906-29ca575a343a"
JOB_ID="1087741484119716"

echo "Generating Silver pipeline YAML..."
databricks bundle generate pipeline \
  --existing-pipeline-id "$SILVER_PIPELINE_ID" \
  --config-dir resources \
  --source-dir src/silver

echo "Generating Gold pipeline YAML..."
databricks bundle generate pipeline \
  --existing-pipeline-id "$GOLD_PIPELINE_ID" \
  --config-dir resources \
  --source-dir src/gold

echo "Generating Job YAML..."
databricks bundle generate job \
  --existing-job-id "$JOB_ID" \
  --config-dir resources \
  --source-dir src

echo ""
echo "Done. Generated files:"
ls -la resources/
echo ""
echo "Source files:"
find src -type f
