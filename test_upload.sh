#!/bin/bash
set -e
twine upload --repository testpypi $*
