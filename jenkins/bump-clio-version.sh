#!/bin/sh

# Turn on verbose
set -ex

CALLING_DIR=$(pwd)
CLIO_DIR=$(cd $(dirname $(dirname $0)) && pwd)

# Run this script on Jenkins as part of the stage-RC process to
# create and commit a bump to Clio's base version.

# Expects the env var RELEASED_VERSION to be set to the full value of the just-released version.

# If run on develop, bumps the minor version.
# Otherwise (for a hotfix branch) bumps the patch version.

if [ -z "$RELEASED_VERSION" ]
then
  >&2 echo "Error: RELEASED_VERSION not set!!"
  exit 1
fi

VERSION_FILE=${CLIO_DIR}/.clio-version
if [ ! -f "$VERSION_FILE" ]
then
  >&2 echo "Error: No version file at '${VERSION_FILE}'!!"
  exit 1
fi

cd "$CLIO_DIR"
COMMIT=$(git rev-parse HEAD)
BASE_VERSION=$(cat ${CLIO_DIR}/.clio-version | tr -d '\n')

HEAD_TAG=$(git describe --exact-match || echo)

if [ "$HEAD_TAG" = dev ]
then
  NEXT_MINOR=$(perl -ne 'print $1.".".($2+1).".".0 if /(\d+)\.(\d+)\.(\d+)/' ${VERSION_FILE})
  git checkout develop
  echo "$NEXT_MINOR" > ${VERSION_FILE}
  git commit -a -m "Bump next base version to ${NEXT_MINOR}"
  git checkout "$COMMIT"
fi

NEXT_PATCH=$(perl -ne 'print $1.".".$2.".".($3+1) if /(\d+)\.(\d+)\.(\d+)/' ${VERSION_FILE})
git checkout -b "${RELEASED_VERSION}-hotfix"
echo "$NEXT_PATCH" > ${VERSION_FILE}
git commit -a -m "Bump next base version to ${NEXT_PATCH}"
git checkout "$COMMIT"

exit 0
