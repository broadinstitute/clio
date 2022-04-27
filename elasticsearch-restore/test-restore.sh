#!/usr/bin/env bash

set -e -o pipefail

# HACK: Work around local readonly variable in trap!
# I probably don't understand some scoping subtlety. =tbl
#
declare AV0                     # the name of this program
declare CONTEXT                 # Restore this k8s context.
declare NAMESPACE               # Manage this k8s namespace.
declare SERVER                  # Run this Clio server jar.

# Fail when Java's needed but do not have version 1.8.
#
function java_ok() {
    local -r environment=$1 server=$2 client=$3
    local jars version
    test "$SERVER" != "${server%.jar}" &&
        test "$client" != "${client%.jar}" &&
        jars=yes
    test ! "$SERVER" && test ! "$client" && jars=no
    if test yes = "$jars"
    then
        local -a -r java=($(2>&1 java -version))
        local -a parse
        test version = "${java[1]}" &&
            { test java = "${java[0]}" || test openjdk = "${java[0]}"; } &&
            parse=($(echo ${java[2]} | xargs)) && version="${parse%.*}"
        if test 1.8 != "$version"
        then
            1>&2 echo $AV0: Need java -version 1.8 to run Clio.
            1>&2 echo $AV0: You are running: $version
        fi
    fi
    test "$jars" || 1>&2 echo $AV0: Need 2 .jar files: $SERVER $client
    test no = "$jars" || test 1.8 = "$version"
}

# Validate the command line arguments.
#
function usage() {
    local -r environment=$1
    local -r -a the_environments=(dev prod)
    local -r -a the_tools=(base64 cat curl date gcloud grep gsutil helm
                           jar java jq kubectl lsof mktemp sleep vault)
    local -r -a help=(
        ''
        "$AV0:    Deploy a temporary Elasticsearch cluster,"
        '         restore a snapshot from some deployment'
        '         environment to it, then run representative'
        '         Clio queries against that cluster.'
        ''
        "Usage:   $AV0 <env> [<server> <client>]"
        ''
        'Where:   <env>    is the deployment environment,'
        "                  one of: ${the_environments[*]}"
        '         <server> is a clio-server.jar file.'
        '         <client> is a clio-client.jar file.'
        ''
        "Example: $AV0 dev"
        "         $AV0 dev ./server.jar ./client.jar"
        ''
        'Note:    Debug this in dev before trying in prod.'
        ''
        "Note:    $AV0 needs the following tools on PATH."
        "         ${the_tools[*]}"
        ''
        'Note:    Clio requires a 1.8 JDK to run.'
        ''
        'Note:    Run this on the Broad VPN (non-split)!'
        ''
        "BTW, you ran this command: $*")
    local env line missing found tool
    for tool in "${the_tools[@]}"
    do
        &>/dev/null type $tool || missing="$missing $tool"
    done
    test "$missing" && 1>&2 echo $AV0: Missing tools: $missing
    for env in "${the_environments[@]}"
    do
        test X$env = X$environment && found=env
    done
    test "$found" || 1>&2 echo $AV0: Not an environment: "'$environment'"
    if ! java_ok "$@" || test "$missing" || test ! "$found"
    then
        for line in "${help[@]}"; do 1>&2 echo "$line"; done
        exit 1
    fi
}

# Echo the arguments as a 'safe' command line.
#
function show() {
    local -a command
    local arg
    for arg in "$@"; do command+=("'$arg'"); done
    1>&2 echo $AV0: "${command[@]}"
}

# Report the arguments as a command line, then run it.
#
function run() {
    local result status
    show "$@"
    set +e
    result=$("$@")
    status=$?
    set -e
    test 0 -eq $status || 1>&2 echo $AV0: Returned status: $status
    echo "$result"
    return $status
}

# Clean up the Kubernetes $NAMESPACE made by setup_k8s() and restore
# $CONTEXT if necessary.
#
function cleanup_k8s() {
    run kubectl delete secret -n $NAMESPACE elasticsearch-gcs-sa ||
        true
    run kubectl delete namespace $NAMESPACE || true
    if test "$CONTEXT"
    then
        run kubectl config set current-context $CONTEXT
    else
        run kubectl config unset current-context
    fi || true
}

# Make a Kubernetes $NAMESPACE for $environment using $creds.
#
function setup_k8s() {
    local -r environment=$1 creds=${2##*/}
    local -r zone=us-central1-a project=broad-gotc-$environment
    local -r cluster=gotc-$environment-shared-$zone
    local -r context=gke_${project}_${zone}_${cluster}
    run kubectl config set current-context $context
    run kubectl get svc --request-timeout=3s ||
        1>&2 echo $AV0: Use the non-split Broad VPN.
    run gcloud container clusters get-credentials $cluster \
        --zone $zone --project $project
    run kubectl create namespace $NAMESPACE
    local -r -a vault=(vault read -field=sa.json.b64
                       -address=https://clotho.broadinstitute.org:8200
                       secret/dsde/gotc/$environment/clio/elasticsearch-restore)
    >/dev/null run "${vault[@]}"
    run kubectl create secret generic -n $NAMESPACE \
        elasticsearch-gcs-sa --from-file=$creds=<("${vault[@]}" | base64 -d)
}

# Clean up the temporary Elasticsearch cluster made by setup_es().
#
function cleanup_es() {
    local -r pid=$(lsof -t -i:9200 || true)
    if test "$pid" && test 0 != "$pid"
    then
        run lsof -i tcp:9200
        run kill $pid || true
    fi
    run helm uninstall -n $NAMESPACE test-elasticsearch || true
}

# Wait for a green status on the scratch Elasticsearch cluster.
# Return 0 when result is green and not timed out.
#
function wait_for_green() {
    local -r port=$1
    local -r -a health=(curl -s http://localhost:$port/_cluster/health)
    run curl -s ${health[@]} | jq .status
    local -i n=9
    until let 0==n || test green = $(${health[@]} | jq -r .status)
    do
        sleep 1
    done
    run ${health[@]} | jq .
    let n
}

# Make a temporary Elasticsearch cluster for $environment in
# Kubernetes $NAMESPACE using the Helm $values file.  Open a local
# port to the service and wait for the Elasticsearch cluster to get
# healthy.
#
function setup_es() {
    local -r environment=$1 snapshots=$2 values=$3 port=9200
    if run helm install --debug --wait --timeout 5m -n $NAMESPACE \
           -f "$values" test-elasticsearch terra-helm/elasticsearch
    then
        local -i pid=$(lsof -t -i:$port || true)
        if test "$pid" && test 0 -ne $pid
        then
            1>&2 echo $AV0: Killing proceess found on port $port.
            run lsof -i tcp:$port
            run kill $pid
        fi
        run kubectl port-forward -n $NAMESPACE svc/clio-master $port:$port &
        until >/dev/null lsof -t -i:$port; do sleep 1; done
        run lsof -i tcp:$port
        wait_for_green $port
    else
        run kubectl --namespace $NAMESPACE get pods
        exit 4
    fi
}

# Maybe kill the server and clean up the state established by setup().
#
function cleanup() {
    if test "$SERVER"
    then
        local -r ps=$(ps -x | grep -e "java .* -jar $SERVER" | grep -v grep)
        local -r -a pid=($ps)
        test "$pid" && test 0 -ne "$pid" && run kill "$pid"
    fi
    cleanup_es
    cleanup_k8s
}

# Set up a scratch Elasticsearch cluster for $environment in a new
# Kubernetes $NAMESPACE using Helm $values.  Make $snapshots the scratch
# cluster's snapshot repo and print the most recent snapshot there.
#
function setup() {
    local -r environment=$1 creds=$2 snapshots=$3 values=$4
    local -r terra=https://terra-helm.storage.googleapis.com/
    run helm version
    run helm repo add terra-helm $terra
    run helm repo update
    setup_k8s $environment $creds
    setup_es  $environment $snapshots "$values"
}

# Use $creds to make $snapshots the snapshot repository of the scratch cluster.
#
 function make_snapshot_repository() {
    local -r snapshots=$1 creds=$2
    local -r data='{"type": "gcs",
                    "settings": {"application_name": "clio-elasticsearch",
                                 "bucket":           "'$snapshots'",
                                 "compress":         true,
                                 "service_account":  "'$creds'"}}'
    local -r url=http://localhost:9200/_snapshot/$snapshots
    run curl -X PUT -s --data "$data" "$url?verify=false"
}

# Print the name of the most recent snapshot in $snapshots.
#
function most_recent_snapshot() {
    local -r snapshots=$1 prefix="clio-$(date "+%Y-%m-%d")"
    local -r url=http://localhost:9200/_snapshot/$snapshots
    run curl -s "$url/$prefix*" | 1>&2 jq .
    curl -s "$url/$prefix*" | jq -r .snapshots[-1].snapshot
}

# Restore snapshot $snap from $snapshots to the scratch cluster.
#
function restore_snapshot() {
    local -r snapshots=$1 snap=$2 es=http://localhost:9200
    1>&2 echo $AV0: Restoring snapshot $snap to $es.
    let -i n=9
    until curl -s $es/_cat/recovery || let 0==n--
    do
        run curl -s $es/_cat/recovery || sleep 5
    done
    local -r url=$es/_snapshot/$snapshots/$snap/_restore
    run curl -X POST -s "$url?wait_for_completion=true"
    wait_for_green ${es##*:}
    run curl -s $es/_cat/indices
}

# Map an Elasticsearch index to a Clio client query command.
# Emulate an "associative array" for old bash versions.
#
function index_to_query() {
    local -r index=$1
    local query='arrays bam gvcf-v2 wgs-cram-v2 wgs-ubam'
    case "$index" in
        arrays)      query=query-arrays;;
        bam)         query=query-bam;;
        gvcf-v2)     query=query-gvcf;;
#       gvcf)        query=query-gvcf;; # This index fails.
        wgs-cram-v2) query=query-cram;;
        wgs-ubam)    query=query-ubam;;
    esac
    echo $query
}

# Map a Clio client query command to JSON record fields.
# Emulate an "associative array" for old bash versions.
#
function query_to_fields() {
    local -r query=$1
    local -r default='location project data_type sample_alias version'
    local fields='query-arrays query-bam query-cram query-gvcf query-ubam'
    case "$query" in
        query-arrays) fields='location chipwell_barcode version';;
        query-bam)    fields="$default";;
        query-cram)   fields="$default";;
        query-gvcf)   fields="$default";;
        query-ubam)   fields='location flowcell_barcode lane library_name';;
    esac
    echo $fields
}

# Pass rest arguments to Clio client after setting up $environment.
#
function run_client() {
    local -r environment=$1 client=$2; shift 2
    local -r secret=secret/dsde/gotc/$environment/clio/clio-account.json
    local -r -a config=(-Dclio.server.hostname=localhost
                        -Dclio.server.use-https=false)
    show vault read -format json -field data $secret
    show java "${config[@]}" -jar "$client" "$@"
    java "${config[@]}" -Dclio.client.service-account-json=<(
        vault read -format json -field data $secret
    ) -jar "$client" "$@"
}

# Run Clio $client command to query for $record in Elasticsearch
# $index.
#
# Use --include-all for when test case is a "Deleted" or "External" record.
# clio-util/src/main/scala/org/broadinstitute/clio/util/model/DocumentStatus.scala
#
function make_query() {
    local -r environment=$1 client=$2 index=$3 record=$4
    local -r query=$(index_to_query $index)
    local -r json=$(run gsutil cat "$record")
    local -a args=($query)
    local key value
    for key in $(query_to_fields $query)
    do
        value="$(echo "$json" | jq -r .$key)"
        test null = "$value" || args+=(--$(echo $key | tr _ -) "$value")
    done
    args+=(--include-all)
    run_client $environment "$client" "${args[@]}"
    local -r result=$(run_client $environment "$client" "${args[@]}")
    if test '[]' == "$result"
    then
        1>&2 echo $AV0: '[]' from: "${args[@]}"
        1>&2 echo $AV0: FAILED
        false
    fi
    if echo "$result" | jq .
    then
        1>&2 echo $AV0: SUCCEEDED
    else
        1>&2 echo $AV0: FAILED bad JSON from: "${args[@]}"
        false
    fi
}

# Run Clio commands on $client to query for both the earliest and most
# recent record in $bucket for each Elasticsearch index in
# $environment.
#
function run_bookend_queries() {
    local -r environment=$1 bucket=$2 client=$3
    local -r -a ls=(run gsutil ls)
    local -a years month0 monthN days records
    local index day0 dayN n
    for index in $(index_to_query)
    do
        years=(  $("${ls[@]}" gs://$bucket/$index))
        month0=( $("${ls[@]}" "${years[0]}"))
        let n=${#years[@]}   || true; let --n || true
        monthN=( $("${ls[@]}" "${years[$n]}"))
        days=(   $("${ls[@]}" "${month0[0]}"))
        day0="${days[0]}"
        let n=${#monthN[@]}  || true; let --n || or true
        days=(   $("${ls[@]}" "${monthN[$n]}"))
        let n=${#days[@]}    || true; let --n || true
        dayN="${days[$n]}"
        records=($("${ls[@]}" "$day0"))
        1>&2 echo $AV0: Looking at "${records[0]}"
        gsutil cat "${records[0]}" | jq .
        make_query $environment "$client" $index "${records[0]}"
        records=($("${ls[@]}" "$dayN"))
        let n=${#records[@]} || true; let --n || true
        1>&2 echo $AV0: Looking at "${records[$n]}"
        gsutil cat "${records[$n]}" | jq .
        make_query $environment "$client" $index "${records[$n]}"
    done
}

# Return status 0 when the version resources in $jar agree with what
# the currently deployed Clio server wrote in $bucket.
#
function assert_clio_version_ok() {
    local -r bucket=$1 jar=$2
    local -r current=gs://$bucket/current-clio-version.txt
    local -r pwd=$(pwd) version=$(run gsutil cat $current)
    local -a bad files=($(jar -tf "$jar" | grep '^clio-.*-version\.conf$'))
    if test "${#files[@]}" -lt 3
    then
        1>&2 echo $AV0: Not enough version resources in "$jar"
        1>&2 echo $AV0: Found only these: "${files[@]}"
        exit 2
    fi
    local -r dir=$(mktemp -d)
    run cd "$dir"
    run jar -xf "$jar" "${files[@]}"
    local file
    for file in "${files[@]}"
    do
        run grep -q -e ": $version" "$file" || bad+=("$file")
    done
    run cd "$pwd"
    run rm -rf "$dir"
    if test "${#bad[@]}" -gt 0
    then
        1>&2 echo $AV0: Wrong version here: "${bad[@]}"
        exit 3
    fi
}

# Return when the Clio $client in $environment reports the server
# 'Started'.
#
function wait_for_clio_server_started() {
    local -r environment=$1 client=$2
    1>&2 echo $AV0: Waiting for Clio server to finish Recovering.
    until run_client $environment $client get-server-health
    do
        sleep 1
    done
    run_client $environment $client get-server-health
    local json=$(run_client $environment $client get-server-health)
    until test Started = $(echo "$json" | jq -r .clio)
    do
        sleep 1
        json=$(run_client $environment $client get-server-health)
    done
    run_client $environment $client get-server-health
}

# Test Clio in $environment using $SERVER and $client .jar files.
#
function test_clio() {
    local -r environment=$1 client=$2
    local -r bucket=broad-gotc-$environment-clio
    assert_clio_version_ok $bucket "$SERVER"
    assert_clio_version_ok $bucket "$client"
    local -r secret=secret/dsde/gotc/$environment/clio/clio-account.json
    local -r version=gs://$bucket/current-clio-version.txt
    local -r -a java=(java -Dclio.server.persistence.type=GCP
                      -Dclio.server.persistence.project-id=$AV0
                      -Dclio.server.persistence.bucket=$bucket)
    1>&2 echo $AV0 Starting: "${java[@]}"
    "${java[@]}" -Dclio.server.persistence.service-account-json=<(
        vault read -format json -field data $secret
    ) -jar "$SERVER" &
    local -r pid=$!
    1>&2 echo Clio server PID = $pid
    wait_for_clio_server_started $environment "$client"
    run_bookend_queries $environment $bucket "$client"
    run kill $pid
}

function main() {
    AV0=${0##*/}
    usage "$@"
    local -r environment=$1 server=$2 client=$3
    SERVER="$server"
    run kubectl config current-context &&
        CONTEXT=$(run kubectl config current-context)
    NAMESPACE=elasticsearch-restore-test-$environment
    local -r creds=/usr/share/elasticsearch/config/snapshot_credentials.json
    local -r snapshots=broad-gotc-$environment-clio-es-snapshots
    local -r wd=$(&>/dev/null cd $(dirname "${BASH_SOURCE[0]}") &&
                      pwd && >/dev/null cd -)
    local -r values="$wd/helm-values.yaml"
    trap 'cleanup; exit' ERR EXIT HUP INT TERM
    setup $environment $creds $snapshots "$values"
    make_snapshot_repository $snapshots $creds
    local -r snap=$(most_recent_snapshot $snapshots)
    1>&2 echo $AV0: $snap is the most recent snapshot.
    restore_snapshot $snapshots $snap
    if test "$SERVER"
    then
        test_clio $environment "$SERVER" "$client"
    else
        echo; echo $AV0: No server and client .jar files to run.; echo
    fi
}

main "$@"
