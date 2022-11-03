#!/bin/bash

# This script auto generates and rewrite mkdocs.yaml
# Run from git root

# ENV
OUTPUT_BASE="docs/reference/"
PYTHON_FILES=$(find featurebyte -type f -name "*.py")

# Remove old file
rm -rf ${OUTPUT_BASE}

for source in $PYTHON_FILES ; do
    source_rm_ext=${source%???}
    source_replace_slash=$(sed 's|/|.|g' <<< "${source_rm_ext}")
    source_file_value="::: "${source_replace_slash}
    source_file=$(sed "s|^featurebyte/|${OUTPUT_BASE}|g" <<< "${source_rm_ext}")".md"
    source_file_dir=$(sed 's|/[^/]*$||g' <<< ${source_file})
    # echo "$source => $source_rm_ext => $source_replace_slash"
    # echo "${source_file} => ${source_file_dir}"
    if [ ! -d "${source_file_dir}" ]; then
      mkdir -p "${source_file_dir}"
    fi
    echo "${source_file_value}" > "${source_file}"
done
