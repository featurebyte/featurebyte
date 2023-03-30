#!/bin/bash

set -Eeo pipefail

NON_PRIVUSER=runner
NON_PRIVGROUP=runnergroup

HOST_UID=${LOCAL_UID:-1000}
HOST_GID=${LOCAL_GID:-1000}

_is_sourced() {
	# https://unix.stackexchange.com/a/215279
	[ "${#FUNCNAME[@]}" -ge 2 ] \
		&& [ "${FUNCNAME[0]}" = '_is_sourced' ] \
		&& [ "${FUNCNAME[1]}" = 'source' ]
}

setup_permissions() {
  echo "Running script as HOST_UID:HOST_GID $(id -u):$(id -g)"

  if [ "$(id -u)" = '0' ]; then
    echo "Modifying user id/group id for runner/runnergroup (uid:gid ${HOST_UID}:${HOST_GID})"
    if [ "$(HOST_GID)" = '1000' ]; then
      echo "Using default group id 1000, skipping group modification"
    else
      groupmod -g "${HOST_GID}" "${NON_PRIVGROUP}"
    fi
    if [ "$(HOST_UID)" = '1000' ]; thenen
      echo "Using default user id 1000, skipping user modification."
    else
      usermod -u "${HOST_UID}" -o -m "${NON_PRIVUSER}"
    fi
    chown -R "${HOST_UID}:${HOST_GID}" /data/staging
  fi
}

_main() {
  setup_permissions

  if [ "$(id -u)" = '0' ]; then
    # Running as root, downgrading to normal user
    exec gosu "${NON_PRIVUSER}:${NON_PRIVGROUP}" "$BASH_SOURCE" "$@"
  else
    # Running as normal user
    python /scripts/migration.py
    uvicorn featurebyte.app:app --host=$API_HOST --port=$API_PORT --workers=$WORKERS --timeout-keep-alive=300 --log-level=$LOG_LEVEL
  fi
}

if ! _is_sourced; then
	_main "$@"
fi
