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
    echo "Creating user runner:runnergroup (uid:gid ${HOST_UID}:${HOST_GID})"
    if [ "$(getent group runnergroup)" ]; then
      echo "group exists."
    else
      groupadd -g "${HOST_GID}" "${NON_PRIVGROUP}" -o
    fi
    if [ "$(getent passwd runner)" ]; then
      echo "user exists."
    else
      useradd -g "${HOST_GID}" -u "${HOST_UID}" -o -M -d /app -r "${NON_PRIVUSER}"
    fi
    chown -R "${HOST_UID}:${HOST_GID}" /app
  fi
}

_main() {
  setup_permissions

  if [ "$(id -u)" = '0' ]; then
    # Running as root, downgrading to normal user
    exec gosu "${NON_PRIVUSER}:${NON_PRIVGROUP}" "$BASH_SOURCE" "$@"
  else
    # Running as normal user
    celery --app featurebyte.worker.start.celery worker --loglevel=INFO
  fi
}

if ! _is_sourced; then
	_main "$@"
fi
