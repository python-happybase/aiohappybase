#!/usr/bin/env bash

set -x

PYTHON=/usr/bin/python${PYTHON_VERSION}

${PYTHON} -m pip install -U -r test-requirements.txt

wait_hbase() {
  # Wait for thrift port to bind
  while ! netstat -tna | grep 'LISTEN\>' | grep -q ':9090\>'; do sleep 1; done
  sleep 1  # Give it a little extra time
}

if [ "${CODECOV_TOKEN}" != "" ]; then
  ${PYTHON} -m pip install coverage codecov

  run_tests() {
    ${PYTHON} -m coverage run -m pytest
    ${PYTHON} -m coverage xml
    ${PYTHON} -m codecov --token=${CODECOV_TOKEN}
  }
else
  run_tests() { ${PYTHON} -m pytest; }
fi

if [ "$AIOHAPPYBASE_CLIENT" == "http" ]; then
  ENABLE_HTTP=$(echo "<property><name>hbase.regionserver.thrift.http</name><value>true</value></property>" | sed 's/\//\\\//g')
  sed -i "/<\/configuration>/ s/.*/${ENABLE_HTTP}\n&/" /opt/hbase/conf/hbase-site.xml
fi

/opt/hbase-server &>/dev/null &  # It's already got its own logs

wait_hbase

HBASE_PID=$!

# Run tests for latest HBase compat
export AIOHAPPYBASE_COMPAT=0.98
run_tests

# Run tests for min HBase compat
export AIOHAPPYBASE_COMPAT=0.90
run_tests
