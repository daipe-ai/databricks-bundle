#!/bin/bash -e

pylint \
--ignore-patterns=".+Test\.py" \
--rcfile=.pylintrc \
--class-naming-style=PascalCase \
--module-rgx=".+" \
--function-rgx="^[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?$" \
--method-rgx="^[_]{0,2}[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?(__)?$" \
--attr-rgx="^[_]{0,2}[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?$" \
--argument-rgx="^[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?$" \
--variable-rgx="^[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?$" \
--class-attribute-rgx="^[_]{0,2}[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?$" \
--const-rgx="^[a-z]+((\d)|([A-Z0-9][a-z0-9]+))*([A-Z])?$" \
src
