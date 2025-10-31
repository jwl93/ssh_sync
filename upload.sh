#!/bin/bash
set -e
export TWINE_CERT=/etc/ssl/certs/ca-certificates.crt
twine upload -r trade $*
