#!/bin/bash
set -e

if [[ "$1" = "serve" ]]; then
    shift 1
    torchserve --start --ts-config /home/model-server/config.properties --model-store /home/model-server/model-store/ --log-config /home/model-server/log4j2.xml
else
    eval "$@"
fi

# prevent docker exit
tail -f /dev/null
