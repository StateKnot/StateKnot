#!/usr/bin/env bash
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
[[ $(uname -s) == Linux ]]
[[ ${GITHUB_JOB:?} == postgres-native-build ]]
[[ ${STATEKNOT_REQUIRE_POSTGRES_TESTS:-} == 1 ]]
mkdir postgres-native-parallel
if [[ -e target ]]; then
  [[ -d target && ! -L target && -z $(find target -mindepth 1 -print -quit) ]]
  printf 'empty\n' > postgres-native-parallel/cold-target.txt
else
  printf 'absent\n' > postgres-native-parallel/cold-target.txt
fi
df -Pk . > postgres-native-parallel/build-before.df
python3 .github/scripts/postgres-native-binaries.py build store
python3 .github/scripts/postgres-native-binaries.py build runtime
python3 .github/scripts/postgres-native-artifacts.py export
