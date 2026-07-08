#!/bin/sh

# SPDX-FileCopyrightText: 2026 Jacques Supcik <jacques.supcik@hefr.ch>
#
# SPDX-License-Identifier: MIT

service_name="raspi-off"

remove() {
    printf "\033[32m Post Remove of a normal remove\033[0m\n"
    echo "Remove" > /tmp/postremove-proof
}

purge() {
    printf "\033[32m Post Remove purge, deb only\033[0m\n"
    echo "Purge" > /tmp/postremove-proof
}

upgrade() {
    printf "\033[32m Post Remove of an upgrade\033[0m\n"
    echo "Upgrade" > /tmp/postremove-proof
}

echo "$@"

action="$1"

case "$action" in
  "0" | "remove")
    remove
    ;;
  "1" | "upgrade")
    upgrade
    ;;
  "purge")
    purge
    ;;
  *)
    printf "\033[32m Alpine\033[0m"
    remove
    ;;
esac
