#!/bin/bash
# Stratio Deep External Deployment script

TMPDIR="$1"

cd ${TMPDIR}

mkdir external
cd ./external


compileExternal()
{
  DEEP_EXTERNAL_NAME="$1"
  DEEP_EXTERNAL_REPO="$2"
  DEEP_EXTERNAL_DIR="$3"

  read -p "What tag want to use for $DEEP_EXTERNAL_NAME repository? " DEEP_EXTERNAL_BRANCH

  git clone "$DEEP_EXTERNAL_REPO" ${DEEP_EXTERNAL_DIR} || { echo "Cannot clone Deep external project from repository: ${DEEP_EXTERNAL_REPO}"; exit 1; }

  cd ./${DEEP_EXTERNAL_DIR}/
  git checkout "$DEEP_EXTERNAL_BRANCH" || { echo "Cannot checkout branch: ${DEEP_EXTERNAL_BRANCH}"; exit 1; }

  mvn clean package -DskipTests || { echo "Cannot build Deep external project, aborting"; exit 1; }

  echo "Adding $DEEP_EXTERNAL_NAME libs ..."
  cp -u ./target/*.jar ${TMPDIR}/lib || { echo "Cannot copy target jars to output lib directory, aborting"; exit 1; }
  cp -u ./target/alternateLocation/*.jar ${TMPDIR}/lib || { echo "Cannot copy alternate jars to output lib directory, aborting"; exit 1; }
  echo "Added $DEEP_EXTERNAL_NAME libs"

  cd ..
  rm -rf ${DEEP_EXTERNAL_DIR}
}


#### Create Deep external extractors jars from github

echo "Aerospike native ... "
compileExternal "Aerospike native" "git@github.com:Stratio/deep-aerospike.git" "aerospike"
