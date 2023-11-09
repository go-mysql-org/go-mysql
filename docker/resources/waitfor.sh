#!/bin/bash
host=$1
port=$2

echo "Waiting for mysql at $host:$port"
while true; do
  docker run --rm -it --network=host mysql:8.0 mysql -h$host -P$port -e "SELECT RAND()" >/dev/null
  if [[ $? -eq 0 ]]; then
    echo 'Connected'
    break
  fi

  echo 'Still waiting...'
  sleep 1
done
