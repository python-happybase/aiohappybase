#!/usr/bin/env bash

set -xe

PYTHON=/usr/bin/python${PYTHON_VERSION}

${PYTHON} -m pip install -U -r test-requirements.txt

/opt/hbase-server & &> /dev/null  # It's already got its own logs
# Wait for thrift port to bind
while ! netstat -tna | grep 'LISTEN\>' | grep -q ':9090\>'; do sleep 1; done
sleep 1  # Give it a little extra time


if [ "${CODECOV_TOKEN}" != "" ]; then
  ${PYTHON} -m pip install coverage codecov

  run_tests() {
    ${PYTHON} -m coverage run -m pytest
    ${PYTHON} -m coverage xml
    ${PYTHON} -m codecov --token=${CODECOV_TOKEN}
  }
else
  run_tests() { ${PYTHON} -m pytest }
fi

run_tests  # Run tests for latest HBase compat
export AIOHAPPYBASE_COMPAT=0.90
run_tests  # Run tests for min HBase compat
