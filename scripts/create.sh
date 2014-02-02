#!/usr/bin/env sh

if [ $# != 2 ]; then
    echo 'Usage: create.sh USERNAME DATABASE' 1>&2
    exit 1
fi

cd -- "`dirname "$0"`/.."
psql -U "$1" "$2" < sql/create.sql

