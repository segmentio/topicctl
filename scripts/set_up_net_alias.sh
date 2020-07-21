#!/bin/sh

# Copied from https://github.com/segmentio/kingmaker/blob/master/setup_net_alias.sh.

ADDR=169.254.123.123
echo "aliasing $ADDR to localhost..."

UNAME=$(uname -a)
case "$UNAME" in
    Linux*) sudo ifconfig lo:0 $ADDR netmask 255.255.255.0 up;;
    Darwin*) sudo ifconfig lo0 alias $ADDR;;
    *) exit
esac

if [[ $? != 0 ]]
then
    >&2 echo "unable to create alias"
    exit 1
fi
