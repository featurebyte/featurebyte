#!/bin/sh

curl -X POST \
    {{header_params}} \
    -d '{"entity_serving_names": {{entity_serving_names}}}' \
    {{serving_url}}
