#!/usr/bin/env sh

if [ $# -ne 2 ]; then
    echo 'Usage: drop.sh USERNAME DATABASE' 2>&1
    exit 1
fi

cd -- "`dirname "$0"`/.."
psql -U "$1" "$2" < sql/drop.sql

