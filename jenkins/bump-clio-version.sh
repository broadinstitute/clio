#!/bin/bash

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

VERSION_FILE="${CLIO_DIR}/.clio-version"
if [ ! -f "$VERSION_FILE" ]
then
  >&2 echo "Error: No version file at '${VERSION_FILE}'!!"
  exit 1
fi

cd "$CLIO_DIR"
COMMIT=$(git rev-parse HEAD)
BASE_VERSION=$(cat "$VERSION_FILE" | tr -d '\n')

# First, tag the current commit with the just-released version.
git tag "$RELEASED_VERSION"
git push origin "$RELEASED_VERSION"

# By default, `git describe` searches backwards for the closest annotated tag.
#   --tags makes it look for un-annotated tags (which we use) as well
#   --match restricts the command to only returning "dev" (or an error)
#   --exact-match makes it only look at HEAD (no backwards search)
HEAD_TAG=$(git describe --tags --match dev --exact-match || echo)

# We check for the tag dev instead of the branch develop because that's what the stage-RC job does
# The HEAD of develop might be broken at any given point, but the tag dev is only moved after a successful
# test & deploy to the dev environment.
if [ "$HEAD_TAG" = dev ]
then
  # When we cut from dev, we bump the minor component of the base Clio version, and set the patch to 0
  # Bumping the minor version on develop makes it be always ahead of any version bumps that might happen
  # on the hotfix branch cut during this release.
  NEXT_MINOR=$(perl -ne 'print $1.".".($2+1).".0" if /(\d+)\.(\d+)\.\d+/' ${VERSION_FILE})
  git checkout develop
  echo "$NEXT_MINOR" > ${VERSION_FILE}
  git commit -a -m "Bump next base version to ${NEXT_MINOR}"
  git push origin develop
  git checkout "$COMMIT"
fi

# Regardless of whether or not we're cutting from dev, we create a hotfix branch at the just-released
# commit, named after the just-released version, in which the patch number is bumped.
#
# If we're releasing from a hotfix branch, this will result in a new hotfix branch being cut from
# the head of the old hotfix branch. This isn't a problem because at the point the original hotfix
# branch was cut, a commit was also added to develop bumping the minor version number, so we can cut
# hotfixes and bump the patch number forever and still be behind develop SemVer-wise.
NEXT_PATCH=$(perl -ne 'print $1.".".$2.".".($3+1) if /(\d+)\.(\d+)\.(\d+)/' ${VERSION_FILE})
HOTFIX_BRANCH="${RELEASED_VERSION}-hotfix"
git checkout -b "$HOTFIX_BRANCH"
echo "$NEXT_PATCH" > ${VERSION_FILE}
git commit -a -m "Bump next base version to ${NEXT_PATCH}"
git push origin "$HOTFIX_BRANCH"
git checkout "$COMMIT"
