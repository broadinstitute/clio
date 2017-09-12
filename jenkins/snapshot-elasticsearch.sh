#!/bin/bash

set -e

# Name the Elasticsearch application to snapshot.
# Use this to name the GCS bucket and Elasticsearch repository.
#
declare -r APPLICATION=clio-elasticsearch

# The number of Elasticsearch nodes to try before giving up.
#
declare -i -r ELASTICSEARCH_NODE_COUNT=3

# Echo the GCS bucket named for $environment.
#
function bucketForEnvionment () {
    local -r environment=$1
    echo broad-gotc-$environment-$APPLICATION
}

# Show usage and fail if there is a problem with the command line.
#
function help () {
    local -r av0="$1" environment="$2" verb="$3"
    local -r -a environments=(dev prod staging)
    local -r -a tools=(curl date jq)
    local -r -a verbs=(list take configure restore delete)
    local -r bucket=$(bucketForEnvionment '<environment>')
    local -a usage=(
        ''
        "$av0: Snapshot an Elasticsearch cluster into a GCS bucket."
        ''
        "Usage: $av0 <environment> <verb> [<snapshot> ...]"
        ''
        "Where: <environment> is one of: ${environments[*]}"
        "       <verb> is one of: ${verbs[*]}, and"
        "       <snapshot> is a snapshot name from 'list'"
        "       and where:"
        "       list      lists the snapshots already taken,"
        "       take      takes a snapshot now,"
        "       configure configures snapshots in <environment>,"
        "       restore   restores the named <snapshot>, and"
        "       delete    deletes the named <snapshot>s."
        ''
        "Note: Name the GCS bucket: $bucket"
        ''
        "Example: $av0 dev list"
        ''
        "BTW, you ran: $*"
        '')
    local ok=yes x
    for x in "${tools[@]}"; do which $x >/dev/null || ok=no; done
    test $ok = no && usage+=("${av0}: You need these tools on PATH: ${tools[*]}")
    ok=no
    for x in ${environments[*]}; do test $x = "$environment" && ok=yes; done
    test $ok = no && usage+=("${av0}: No environment: '$environment'")
    ok=no
    for x in ${verbs[*]}; do test $x = "$verb" && ok=yes; done
    test $ok = no && usage+=("${av0}: No verb: '$verb'")
    if test $ok = no
    then
        for line in "${usage[@]}"; do echo 1>&2 "$line"; done
        exit 1
    fi
}

# Echo ELASTICSEARCH_NODE_COUNT names of Elasticsearch nodes.
#
function elasticsearchNodes () {
    local -i n last first=1
    local -r prefix=elasticsearch
    let 'last = first + ELASTICSEARCH_NODE_COUNT' 'n = first'
    local result
    while let 'n < last'
    do
        result="$result $prefix$n"
        let 'n = n + 1'
    done
    echo $result
}

# Report that $av0 is running the rest of arguments as a command line.
# Then run that command line and pipe it through 'jq .'.
#
function echoRunJq () {
    local -r av0="$1" ; shift
    local -a command
    local arg result
    for arg in "$@"; do command+=("'$arg'"); done
    echo $av0: Running "${command[@]}"
    result=$(echo "$@")         # That echo disables this script!
    local -r status=$?
    echo "$result" | jq .
    return $status
}

# Configure snapshots to $bucket in for the $es cluster.
#
function snapshot_configure () {
    local -r av0="$1" es=$2 bucket=$3
    local -r data='
    {
        "type": "gcs",
        "settings": {
            "bucket": "'$bucket'",
            "service_account": "_default_",
            "compress": true,
            "application_name": "'$APPLICATION'"
        }
    }'
    echoRunJq "$av0" curl -s -X PUT --data "$data" $es/_snapshot/$bucket
}

# List the snapshots in $environment for $es cluster.
#
function snapshot_list () {
    local -r av0="$1" es=$2 bucket=$3
    echoRunJq "$av0" curl -s $es/_snapshot/$bucket/_all
}

# Take a snapshot in $environment for $es cluster.
#
function snapshot_take () {
    local -r av0="$1" es=$2 bucket=$3
    local -r jq='keys|.[]|select(startswith(".")|not)'
    local -r snapshot=snap-$(date -u +%Y-%m-%d-%H-%M-%S)
    local -r url=$es/_snapshot/$bucket/$snapshot?wait_for_completion=true
    local -a -r curl=(curl -s $es/_aliases)
    local -r indexes=$("${curl[@]}" | jq --raw-output $jq)
    if test -z "$indexes"
    then
        echo 1>&2 $av0: No document indexes in $es
        echoRunJq "$av0" "${curl[@]}"
        exit 2
    fi
    local index indices comma
    for index in $indexes; do indices=$indices$comma$index; comma=,; done
    local -r data='
    {
        "indices": "'$indices'",
        "include_global_state": false
    }'
    echoRunJq "$av0" curl -s -X PUT --data "$data" $url
}

# Restore the snapshot named in $4.
#
function snapshot_restore () {
    local -r av0="$1" es=$2 bucket=$3 snap=$4
    echoRunJq "$av0" curl -s -X POST $es/_snapshot/$bucket/$snap/_restore
}

# Delete the snapshots named in $4 ... and so on.
#
function snapshot_delete () {
    local -r av0="$1" es=$2 bucket=$3 ; shift 3
    for snap in "$@"
    do
        echoRunJq "$av0" curl -s -X DELETE $es/_snapshot/$bucket/$snap
    done
}

# Fail or run $snapshot on $nodes until one succeeds.
#
function main () {
    local -r av0="${0##*/}" environment="$1" verb="$2"
    help "$av0" "$@" ; shift 2
    local node es snapshot=snapshot_$verb nodes=$(elasticsearchNodes)
    local -r bucket=$(bucketForEnvionment $environment)
    local -r domain=gotc-$environment.broadinstitute.org
    for node in $nodes
    do
        $snapshot "$av0" http://$node.$domain:9200 $bucket "$@" && return 0
    done
    echo 1>&2 "$av0:" $verb failed on all: $nodes
    return 1
}

main "$@"
