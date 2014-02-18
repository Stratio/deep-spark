#!/bin/bash
# Stratio Deep Deployment script

TMPDIR=/tmp/stratio-deep-distribution

rm -rf ${TMPDIR}
mkdir -p ${TMPDIR}

SPARK_REPO="$1"

if [ -z "$1" ]; then
    SPARK_REPO="git@bitbucket.org:stratio/stratiospark.git"
fi

SPARK_BRANCH="$2"

if [ -z "$2" ]; then
    SPARK_BRANCH="branch-0.9"
fi

echo "SPARK_REPO: ${SPARK_REPO}"
echo "SPARK_BRANCH: ${SPARK_BRANCH}"
echo " >>> STRATIO DEEP MAKE DISTRIBUTION <<< "

LOCAL_DIR=`pwd`

echo "LOCAL_DIR=$LOCAL_DIR"

mvn -version >/dev/null || { echo "Cannot find Maven in path, aborting"; exit 1; }

RELEASE_VER=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version 2>/dev/null | grep -v '\[') || { echo "Cannot obtain project version, aborting"; exit 1; }
echo "RELEASE_VER: ${RELEASE_VER}"

if [ "$RELEASE_VER" = "" ]; then
   echo "Release version empty, aborting"; exit 1;
fi

#### Create Stratio Deep jars from bitbucket (master tag) through maven release plugin

echo "################################################"
echo "Compiling Stratio Deep"
echo "################################################"
echo "$(pwd)"
mvn clean package || { echo "Cannot build StratioDeep project, aborting"; exit 1; }

mkdir -p ${TMPDIR}/lib || { echo "Cannot create output lib directory"; exit 1; }

cp ../*/target/*.jar ${TMPDIR}/lib || { echo "Cannot copy target jars to output lib directory, aborting"; exit 1; }
cp ../*/target/alternateLocation/*.jar ${TMPDIR}/lib || { echo "Cannot copy alternate jars to output lib directory, aborting"; exit 1; }

#mvn dependency:get -DgroupId=org.apache.cassandra -DartifactId=cassandra-clientutil -Dversion=${CASS_VER} -Ddest=. -Dtransitive=false -DremoteRepositories=stratio-snapshots::default::http://nexus.strat.io:8081/nexus/content/repositories/snapshots/

echo "################################################"
echo "Creating Spark distribuition"
echo "################################################"
cd ${TMPDIR}

STRATIOSPARKDIR=stratiospark

git clone "$SPARK_REPO" ${STRATIOSPARKDIR} || { echo "Cannot clone Spark project from repository: ${SPARK_REPO}"; exit 1; }

cd ./${STRATIOSPARKDIR}/
git checkout "$SPARK_BRANCH" || { echo "Cannot checkout branch: ${SPARK_BRANCH}"; exit 1; }

#--hadoop 2.0.0-mr1-cdh4.4.0
./make-distribution.sh || { echo "Cannot make Spark distribution"; exit 1; }

cd ..

cp ${TMPDIR}/lib/*.jar ${STRATIOSPARKDIR}/dist/jars/
mv ${STRATIOSPARKDIR}/dist/ spark-deep-distribution

DISTFILENAME=spark-deep-distribution-${RELEASE_VER}.tgz

echo "DISTFILENAME: ${DISTFILENAME}"

tar czf ${DISTFILENAME} spark-deep-distribution || { echo "Cannot create tgz"; exit 1; }

mv ${DISTFILENAME} ${LOCAL_DIR}

echo "################################################"
echo "Finishing process"
echo "################################################"
cd ${LOCAL_DIR}
rm -rf ${TMPDIR}




