#!/bin/bash
# Stratio Deep Deployment script

STRATIO_DEEP_REPO="git@github.com:Stratio/stratio-deep.git"

echo " >>> STRATIO DEEP DEPLOYMENT <<< "

SPARK_REPO="$1"

if [ -z "$1" ]; then
    STRATIO_SPARK_REPO="git@bitbucket.org:stratio/stratiospark.git"
fi

SPARK_BRANCH="$2"

if [ -z "$2" ]; then
    SPARK_BRANCH="branch-0.9"
fi

#if [ -z "$2" ]; then
#echo "Usage: $0 -cv <cassandra version>"
#  exit 0
#fi
#
## Parse arguments
#while (( "$#" )); do
#case $1 in
#    -cv)
#      CASS_VER="$2"
#      shift
#      ;;
#  esac
#shift
#done

LOCAL_DIR=`pwd`

echo "LOCAL_DIR=$LOCAL_DIR"

TMPDIR=/tmp/stratiodeep-clone
TMPDIR_SPARK=/tmp/stratiospark-clone

rm -rf ${TMPDIR}
mkdir -p ${TMPDIR}

rm -rf ${TMPDIR_SPARK}
mkdir -p ${TMPDIR_SPARK}

mvn -version >/dev/null || { echo "Cannot find Maven in path, aborting"; exit 1; }

#### Create Stratio Deep jars from bitbucket (master tag) through maven release plugin

# Clone Stratio Deep (master tag) from bitbucket
git clone ${STRATIO_DEEP_REPO} ${TMPDIR} || { echo "Cannot clone stratio deep project from bitbucket"; exit 1; }
cd ${TMPDIR}

git checkout develop

cd StratioDeepParent
echo "Generating version number"
RELEASE_VER=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version 2>/dev/null | grep -v '\[' | sed s/\-SNAPSHOT//) || { echo "Cannot generate next version number"; exit 1; }

cd ${TMPDIR}/

git flow init -d || { echo "Cannot initialize git flow in stratiodeep-clone project"; exit 1; }
git flow release start $RELEASE_VER || { echo "Cannot create $RELEASE_VER branch"; exit 1; }

git status

echo "Updating pom version numbers"
cd ${TMPDIR}/StratioDeepParent/
mvn versions:set -DnewVersion=${RELEASE_VER} || { echo "Cannot modify pom file with next version number"; exit 1; }

cd ..

find . -name 'pom.xml.versionsBackup' | xargs rm

git commit -a -m "[StratioDeep release prepare] preparing for version ${RELEASE_VER}"  || { echo "Cannot commit changes in stratiodeep-clone project"; exit 1; }

echo " >>> Uploading new release branch to remote repository"
git flow release publish $RELEASE_VER || { echo "Cannot publish $RELEASE_VER branch"; exit 1; }

cd StratioDeepParent

mvn clean package deploy || { echo "Cannot deploy $RELEASE_VER of Stratio Deep"; exit 1; }

mkdir -p ${TMPDIR}/lib || { echo "Cannot create output lib directory"; exit 1; }

cp ../*/target/*.jar ${TMPDIR}/lib || { echo "Cannot copy target jars to output lib directory, aborting"; exit 1; }
cp ../*/target/alternateLocation/*.jar ${TMPDIR}/lib || { echo "Cannot copy alternate jars to output lib directory, aborting"; exit 1; }

echo "Finishing release ${RELEASE_VER}"
mvn clean

echo "Generating next SNAPSHOT version"
curr_version=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version 2>/dev/null | grep -v '\[')
major=$(echo $curr_version | cut -d "." -f 1)
minor=$(echo $curr_version | cut -d "." -f 2)
bugfix=$(echo $curr_version | cut -d "." -f 3)

next_bugfix=$(expr $bugfix + 1)

next_version="${major}.${minor}.${next_bugfix}-SNAPSHOT"

echo "Next SNAPSHOT version: ${next_version}"

cd .. 

echo "Finishing release"
git flow release finish -mFinishing_Release_$RELEASE_VER $RELEASE_VER || { echo "Cannot finnish Stratio Deep ${next_version}"; exit 1; }
git checkout develop

cd StratioDeepParent

echo "Setting new snapshot version"
mvn versions:set -DnewVersion=${next_version} || { echo "Cannot set new version: ${next_version}"; exit 1; }
cd ..

find . -name 'pom.xml.versionsBackup' | xargs rm

echo "Commiting next_version"
git commit -a -m "[StratioDeep release finish] next snapshot version ${next_version}" || { echo "Cannot commit new changes for ${next_version}"; exit 1; }

git push origin || { echo "Cannot push new version: ${next_version}"; exit 1; }

echo "RELEASE_VER=$RELEASE_VER"
#echo "CASS_VER=$CASS_VER"

# Clone develop branch from bitbucket stratiospark project
# (TODO) Find out last stable branch instead of using develop branch
git clone ${STRATIO_SPARK_REPO} ${TMPDIR_SPARK} || { echo "Cannot clone stratiospark project from bitbucket"; exit 1; }

cd ${TMPDIR_SPARK}
git checkout "$SPARK_BRANCH" || { echo "Cannot checkout branch: ${SPARK_BRANCH}"; exit 1; }

echo " >>> Executing make distribution script"
./make-distribution.sh || { echo "Cannot make Spark distribution"; exit 1; }

cp ${TMPDIR}/lib/*.jar ${TMPDIR_SPARK}/dist/jars/
mv ${TMPDIR_SPARK}/dist/ spark-deep-distribution

DISTFILENAME=spark-deep-distribution-${RELEASE_VER}.tgz

echo "DISTFILENAME: ${DISTFILENAME}"

tar czf ${DISTFILENAME} spark-deep-distribution || { echo "Cannot create tgz"; exit 1; }

#### (TODO) Upload the tgz file to a remote repository (Stratio Nexus)

echo " >>> Uploading the tgz distribution to a remote repository"

mv ${DISTFILENAME} ${LOCAL_DIR} || { echo "Cannot move tar.gz file"; exit 1; }

cd "$LOCAL_DIR"

echo " >>> Finishing"

cd ..

# Delete cloned stratiospark project
rm -rf ${TMPDIR}  || { echo "Cannot remove stratiodeep-clone project"; exit 1; }

# Delete cloned Stratio Deep project
rm -rf ${TMPDIR_SPARK}  || { echo "Cannot remove stratiospark-clone project"; exit 1; }

echo " >>> SCRIPT EXECUTION FINISHED <<< "
