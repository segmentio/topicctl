#!/bin/bash

set -euo pipefail

ADDR=169.254.123.123
NETMASK=255.255.255.0
CIDR=24
echo "Aliasing $ADDR to localhost..."

alias_exists_with_ip() {
    ip addr show dev lo | grep -q "$ADDR"
}

OS=$(uname -s)
case "$OS" in
    Linux*)
        if command -v ifconfig >/dev/null 2>&1; then
            sudo ifconfig lo:0 "$ADDR" netmask "$NETMASK" up
        elif command -v ip >/dev/null 2>&1; then
            if alias_exists_with_ip; then
                echo "Alias already present on loopback interface."
            else
                sudo ip addr add "$ADDR/$CIDR" dev lo
            fi
        else
            >&2 echo "Neither ifconfig nor ip is available; cannot create alias."
            exit 1
        fi
        ;;
    Darwin*)
        sudo ifconfig lo0 alias "$ADDR"
        ;;
    *)
        >&2 echo "Unsupported platform: $OS"
        exit 1
        ;;
esac

echo "Alias configured."
