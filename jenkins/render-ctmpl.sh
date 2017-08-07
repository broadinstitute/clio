#!/bin/bash

# This script was copied from the gotc project.

progname=`basename $0`

# set defaults

# flag to determine if we base64 decode
base64=0
# flag to determine if we keep ctmpls around after render
keep=0

# default working directory
output="/working"
# environment
ENVIRONMENT=${ENVIRONMENT-""}
# Vault
VAULT_TOKEN=${VAULT_TOKEN-`cat /root/.vault-token`}
# loglevel
LOG_LEVEL=${LOG_LEVEL-"err"}

export ENVIRONMENT
export VAULT_TOKEN

# config values
CONSUL_TEMPLATE_PATH=${CONSUL_TEMPLATE-"/usr/local/bin"}
CONSUL_CONFIG=${CONSUL_CONFIG-"/etc/consul-template/config/config.json"}

# add getops for
#  -e ENVIRONMENT
#  -k keep files
#  -v VAULT_TOKEN
#  -l LOG_LEVEL
#  -w working directory

# filenames to process are rest of args
# files="$*"


# check if required values are missing

# check if output dir exists

usage() {
    echo "${progname} [-k] [-e ENV] [-v VAULT_TOKEN] [-l LOG_LEVEL] [-w working_directory] file(s)"
}

errorout() {
    if [ $1 -ne 0 ]; then
        echo
        echo "${2}"
        exit $1
    fi
}

cd ${output}

for file in `find . -type f -name "*.ctmpl"`
do

    # filename without .ctmpl
    outputfile="${file%.*}"
    extension="${file##*.}"

    # check if outputfile ends in .b64
    # get second extension
    extension2="${outputfile##*.}"

    # set flag and get final file name
    if [ "${extension2}" == "b64" ]
    then
        base64=1
        finalfile="${outputfile%.*}"
    fi

    echo "Rendering ${file} to ${outputfile} .."

    ${CONSUL_TEMPLATE_PATH}/consul-template \
        -once \
        -config=${CONSUL_CONFIG} \
        -log-level=${LOG_LEVEL} \
        -template=$file:$outputfile
    errorout $? "FATAL ERROR: Unable to execute consule-template for ${file}!"

    if [ "${base64}" -eq "1" ]
    then
       echo "Base64 decoding ${outputfile} to ${finalfile}"
       # base64
       # since ctmpl will likely create blanklines and base64 does not like
       # them  - remove them
       tr -d '\n' < ${outputfile} | base64 -d  > ${finalfile}
       test "${keep}" -eq 0 && rm -f ${outputfile}
    fi

    # clean up
    test "${keep}" -eq 0 && rm -f ${file}
    # reset base64 flag
    base64=0

done

exit 0
