#!/usr/bin/env bash

set -e -o pipefail

function usage() {
    local -r av0=${0##*/} environment=$1 jar=$2
    local -r -a the_environments=(dev prod)
    local -r -a the_tools=(curl gsutil jq)
    local -r -a help=(
        "$av0:    Query a local Clio server for early and late records."
        ''
        "Usage:   $av0 <env> <jar> [...]"
        ''
        'Where:   <env> is the deployment environment,'
        "               one of: ${the_environments[*]}"
        '         <jar> is a clio-server jarfile.'
        ''
        "Example: $av0 dev ./clio-server.jar"
        ''
        "Note:    $av0 needs the following tools on PATH."
        "         ${the_tools[*]}"
        ''
        "BTW, you ran this command: $*")
    local env line missing found tool svc=no
    for tool in "${the_tools[@]}"
    do
        &>/dev/null type $tool || missing="$missing $tool"
    done
    test "$missing" && 1>&2 echo $av0: Missing tools: $missing
    for env in "${the_environments[@]}"
    do
        test X$env = X$environment && found=env
    done
    test "$found" || 1>&2 echo $av0: Not an environment: "'$environment'"
}

function versionMatches() {
    local -r av0=${0##*/} bucket=$1 jar=$2
    gsutil cat $bucket/current-clio-version.txt
    local -r hash=$(gsutil cat $bucket/current-clio-version.txt)
}

function main() {
    local -r av0=${0##*/} environment=$1 jar=$2
    usage "$av0" "$environment" "$jar"
    local -r bucket=gs://broad-gotc-$environment-clio
    local -r -a indexes=(arrays bam gvcf-v2 gvcf wgs-cram-v2 wgs-ubam)
}

main "$@"
