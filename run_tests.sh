#!/bin/bash -e

find src -name "*Test.py" -o -name "*_test.py" | xargs -n1 /bin/bash -c 'echo ""; echo "Running $@:"; echo ""; echo ""; python "$@";' ''
