#!/bin/bash
set -e

bash tools/format_code.sh

git diff --stat
diff_lines=$(git diff --stat|wc -l)
if [[ $diff_lines -gt 0 ]]; then
  echo "Found diff: $diff_lines. run tools/format_code.sh to format your code first."
  exit 1
else
  echo "No Diff"
  exit 0
fi
