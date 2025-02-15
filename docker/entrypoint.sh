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
    if [ "$HOST_GID" = '1000' ]; then
      echo "Using default group id 1000, skipping group modification"
    else
      groupmod -g "${HOST_GID}" "${NON_PRIVGROUP}" -o
    fi
    if [ "$HOST_UID" = '1000' ]; then
      echo "Using default user id 1000, skipping user modification."
    else usermod -u "${HOST_UID}" -o "${NON_PRIVUSER}"
    fi
    chown -R "${HOST_UID}:${HOST_GID}" /app
  fi
}

setup_kdc() {
  echo "Setting up KDC"
  export KRB5_CONFIG=/app/krb5.conf

  if [ -f "${KRB5_CONFIG}" ]; then
    echo "KRB5_CONFIG already exists, skipping KDC setup"
    return
  fi

  # skip if KRB5_REALM or KRB5_KDC is not set
  if [ -z "${KRB5_REALM}" ]; then
    echo "KRB5_REALM not set, skipping KDC setup"

  elif [ -z "${KRB5_KDC}" ]; then
    echo "KRB5_KDC not set, skipping KDC setup"
  else
    echo "Setting up KDC"
    cat <<EOF > ${KRB5_CONFIG}
[libdefaults]
    default_realm = ${KRB5_REALM}

[realms]
    ${KRB5_REALM} = {
        kdc = ${KRB5_KDC}
    }
EOF
  fi
}

_main() {
  setup_permissions
  setup_kdc
  if [ "$(id -u)" = '0' ]; then
    # Running as root, downgrading to normal user
    exec gosu "${NON_PRIVUSER}:${NON_PRIVGROUP}" "$BASH_SOURCE" "$@"
  else
    # Running as normal user
    echo "Starting process: $1"
    if [ "$1" = 'scheduler' ]; then
      celery --app featurebyte.worker.start.celery beat --loglevel=INFO --scheduler featurebyte.worker.schedulers.MongoScheduler --max-interval=1
    elif [ "$1" = 'worker:cpu' ]; then
      celery --app featurebyte.worker.start.celery worker -Q cpu_task,cpu_task:1,cpu_task:2,cpu_task:3 --loglevel=INFO --pool=prefork
    elif [ "$1" = 'worker:io' ]; then
      celery --app featurebyte.worker.start.celery worker -Q io_task,io_task:1,io_task:2,io_task:3 --loglevel=INFO --pool=gevent -c 500
    elif [ "$1" = 'worker' ]; then
      celery --app featurebyte.worker.start.celery beat --loglevel=INFO --scheduler featurebyte.worker.schedulers.MongoScheduler --max-interval=1 &
      celery --app featurebyte.worker.start.celery worker -Q cpu_task,cpu_task:1,cpu_task:2,cpu_task:3 --loglevel=INFO --pool=prefork &
      HOSTNAME="featurebyte-docker-worker-io" celery --app featurebyte.worker.start.celery worker -Q io_task,io_task:1,io_task:2,io_task:3 --loglevel=INFO --pool=gevent -c 500
    elif [ "$1" = 'server' ]; then
      python /scripts/migration.py
      uvicorn featurebyte.app:app --host=$API_HOST --port=$API_PORT --timeout-keep-alive=300 --log-level=info
    elif [ "$1" = '' ]; then
      echo "No command specified, choose either 'worker' or 'server'"
      exit 1
    else
      echo "Unknown command: $1"
      exit 1
    fi
  fi
}

if ! _is_sourced; then
	_main "$@"
fi
