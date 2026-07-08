# SPDX-FileCopyrightText: 2026 Jacques Supcik <jacques.supcik@hefr.ch>
#
# SPDX-License-Identifier: MIT

build:
    go build -o syrup-controller .

release:
    goreleaser release --snapshot --clean
