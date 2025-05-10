#!/bin/bash

if [ "$(id -u)" -ne 0 ]; then
  echo "This script must be run as root." >&2
  exit 1
fi

sync

echo 3 > /proc/sys/vm/drop_caches

exit 0