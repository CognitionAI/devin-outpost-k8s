#!/bin/sh
set -eu
root=$1
output=$2
umask 077
chmod 700 "${output%/*}"
temporary=$output.tmp
trap 'rm -f "$temporary"' 0
rm -f "$temporary"
cd "$root"
{
    printf 'devin-base-paths-v1\000'
    find . -xdev ! -path . -print0
} > "$temporary"
mv "$temporary" "$output"
