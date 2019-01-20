#!/bin/bash

newip=`ifconfig | grep inet | grep netmask | grep broadcast | grep -Eo '([0-9]{1,3}\.){3}[0-9]{1,3}' | grep -v 255`

echo "The new ip is: $newip"

sed -r -i -e 's/(\b[0-9]{1,3}\.){3}[0-9]{1,3}\b'/$newip/  src/main/scala/dataagg.conf

echo "## Here is the dataagg.conf"

cat src/main/scala/dataagg.conf
