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

# By default, `git describe` searches backwards for the closest tag
# Passing --exact-match makes it only look at HEAD, but then it raises an error if the HEAD is un-tagged
HEAD_TAG=$(git describe --exact-match || echo)

# We check for the tag dev instead of the branch develop because that's what the stage-RC job does
# The HEAD of develop might be broken at any given point, but the tag dev is only moved after a successful
# test & deploy to the dev environment
if [ "$HEAD_TAG" = dev ]
then
  # When we cut from dev, we bump the minor component of the base Clio version, and set the patch to 0
  # Bumping the minor version on develop makes it be always ahead of any version bumps that might happen
  # on the hotfix branch cut during this release
  NEXT_MINOR=$(perl -ne 'print $1.".".($2+1).".0" if /(\d+)\.(\d+)\.\d+/' ${VERSION_FILE})
  git checkout develop
  echo "$NEXT_MINOR" > ${VERSION_FILE}
  git commit -a -m "Bump next base version to ${NEXT_MINOR}"
  git checkout "$COMMIT"
fi

# When we cut from anything other than dev, we assume it's a hotfix branch and bump the patch version
# We don't touch develop in this case because when the hotfix branch was cut, the minor version would
# have been bumped in develop, so we can bump the patch version forever and still be "behind" dev
NEXT_PATCH=$(perl -ne 'print $1.".".$2.".".($3+1) if /(\d+)\.(\d+)\.(\d+)/' ${VERSION_FILE})
git checkout -b "${RELEASED_VERSION}-hotfix"
echo "$NEXT_PATCH" > ${VERSION_FILE}
git commit -a -m "Bump next base version to ${NEXT_PATCH}"
git checkout "$COMMIT"
