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
    groupadd -g "${HOST_GID}" "${NON_PRIVGROUP}" -o
    useradd -g "${HOST_GID}" -u "${HOST_UID}" -o -M -d /app -r "${NON_PRIVUSER}"

    # Own the whole /app directory
    echo "Taking ownership of /app"
    chown -R "${NON_PRIVUSER}:${NON_PRIVGROUP}" /app

    # Soft link featurebyte config file
    if [[ ! -f /app/.featurebyte/config.yaml ]]; then echo "# featurebyte configuration file here" > /app/.featurebyte/config.yaml; fi
    mkdir -p /app/.featurebyte/notebook
    if [[ ! -f /app/.featurebyte/notebook/config.yaml ]]; then ln -s /app/.featurebyte/config.yaml /app/.featurebyte/notebook/config.yaml; fi
  fi
}

_main() {
  setup_permissions

  if [ "$(id -u)" = '0' ]; then
    # Running as root, downgrading to normal user
    gosu "${NON_PRIVUSER}:${NON_PRIVGROUP}" "$BASH_SOURCE" "$@"
  else
    # Running as normal user
    echo "Configuration Location: ~/.featurebyte"
    echo "Jupyterlab Running:    http://localhost:8090"
    jupyter-lab --app-dir=/app/.venv/share/jupyter/lab --notebook-dir=/app/.featurebyte/notebook  --port=8090 --NotebookApp.token='' --NotebookApp.password='' --NotebookApp.allow_origin='*' --NotebookApp.ip='0.0.0.0' --no-browser
  fi
}

if ! _is_sourced; then
	_main "$@"
fi
