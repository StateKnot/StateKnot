#!/bin/sh
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0

set -eu

/usr/sbin/nginx -t
/bin/systemctl reload nginx
