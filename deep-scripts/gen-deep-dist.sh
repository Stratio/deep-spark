#!/bin/bash
# Stratio Deep Deployment script

STRATIO_DEEP_REPO="git@github.com:Stratio/stratio-deep.git"

echo " >>> STRATIO DEEP DEPLOYMENT <<< "

SPARK_REPO="$1"

if [ -z "$1" ]; then
    STRATIO_SPARK_REPO="git@github.com:Stratio/spark.git"
fi

SPARK_BRANCH="$2"

if [ -z "$2" ]; then
    SPARK_BRANCH="stratio-branch-1.0.0"
fi

LOCAL_EDITOR=$(which vim)

if [ -z "$LOCAL_EDITOR" ]; then
    $LOCAL_EDITOR=$(which vi)
fi

if [ -z "$LOCAL_EDITOR" ]; then
    echo "Cannot find any command line editor, ChangeLog.txt won't be edited interactively"
fi

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
git clone ${STRATIO_DEEP_REPO} ${TMPDIR} || { echo "Cannot clone stratio deep project from github"; exit 1; }
cd ${TMPDIR}

git checkout develop

cd deep-parent
echo "Generating version number"
RELEASE_VER=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version 2>/dev/null | grep -v '\[' | sed s/\-SNAPSHOT//) || { echo "Cannot generate next version number"; exit 1; }

cd ${TMPDIR}/

git flow init -d || { echo "Cannot initialize git flow in stratiodeep-clone project"; exit 1; }
git flow release start version-$RELEASE_VER || { echo "Cannot create $RELEASE_VER branch"; exit 1; }

git status

echo "Updating pom version numbers"
cd ${TMPDIR}/deep-parent/
mvn versions:set -DnewVersion=${RELEASE_VER} || { echo "Cannot modify pom file with next version number"; exit 1; }

cd ..

find . -name 'pom.xml.versionsBackup' | xargs rm

git commit -a -m "[stratio-deep release prepare] preparing for version ${RELEASE_VER}"  || { echo "Cannot commit changes in stratiodeep-clone project"; exit 1; }

echo " >>> Uploading new release branch to remote repository"
git flow release publish version-$RELEASE_VER || { echo "Cannot publish $RELEASE_VER branch"; exit 1; }

cd deep-parent

mvn clean package || { echo "Cannot deploy $RELEASE_VER of Stratio Deep"; exit 1; }

mkdir -p ${TMPDIR}/lib || { echo "Cannot create output lib directory"; exit 1; }

cp ../*/target/*.jar ${TMPDIR}/lib || { echo "Cannot copy target jars to output lib directory, aborting"; exit 1; }
cp ../*/target/alternateLocation/*.jar ${TMPDIR}/lib || { echo "Cannot copy alternate jars to output lib directory, aborting"; exit 1; }

# Generating ChangeLog
git fetch --tags
latest_tag=$(git describe --tags `git rev-list --tags --max-count=1`)

echo -e "[${RELEASE_VER}]\n\n$(git log ${latest_tag}..HEAD)\n\n$(cat ChangeLog.txt)" > ChangeLog.txt

if [ -n "$LOCAL_EDITOR" ]; then
    $LOCAL_EDITOR ChangeLog.txt
fi

echo "Finishing release ${RELEASE_VER}"
mvn clean

git commit -a -m "[Updated ChangeLog.txt for release ${RELEASE_VER}]"

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
git flow release finish -k -mFinishing_Release_$RELEASE_VER version-$RELEASE_VER || { echo "Cannot finish Stratio Deep ${next_version}"; exit 1; }
git push --tags

git checkout master
git push origin || { echo "Cannot push to master"; exit 1; }

git checkout develop

cd deep-parent

echo "Setting new snapshot version"
mvn versions:set -DnewVersion=${next_version} || { echo "Cannot set new version: ${next_version}"; exit 1; }
cd ..

find . -name 'pom.xml.versionsBackup' | xargs rm

echo "Commiting next_version"
git commit -a -m "[stratio-deep release finish] next snapshot version ${next_version}" || { echo "Cannot commit new changes for ${next_version}"; exit 1; }

git push origin || { echo "Cannot push new version: ${next_version}"; exit 1; }

echo "RELEASE_VER=$RELEASE_VER"
#echo "CASS_VER=$CASS_VER"

# Clone develop branch from bitbucket stratiospark project
# (TODO) Find out last stable branch instead of using develop branch
git clone ${STRATIO_SPARK_REPO} ${TMPDIR_SPARK} || { echo "Cannot clone stratiospark project from bitbucket"; exit 1; }

cd ${TMPDIR_SPARK}
git checkout "$SPARK_BRANCH" || { echo "Cannot checkout branch: ${SPARK_BRANCH}"; exit 1; }

chmod +x bin/stratio-deep-shell

echo " >>> Executing make distribution script"
##  --skip-java-test has been added to Spark 1.0.0, avoids prompting the user about not having JDK 6 installed
./make-distribution.sh --skip-java-test || { echo "Cannot make Spark distribution"; exit 1; }

DISTDIR=spark-deep-distribution-${RELEASE_VER}
DISTFILENAME=${DISTDIR}.tgz

cp ${TMPDIR}/lib/*.jar ${TMPDIR_SPARK}/dist/lib/
rm -f ${TMPDIR_SPARK}/dist/lib/*-sources.jar
rm -f ${TMPDIR_SPARK}/dist/lib/*-javadoc.jar
rm -f ${TMPDIR_SPARK}/dist/lib/*-tests.jar

mv ${TMPDIR_SPARK}/dist/ ${DISTDIR}
cp ${TMPDIR_SPARK}/LICENSE ${DISTDIR}
cp ${TMPDIR}/deep-parent/ChangeLog.txt ${DISTDIR}/

echo "DISTFILENAME: ${DISTFILENAME}"

tar czf ${DISTFILENAME} ${DISTDIR} || { echo "Cannot create tgz"; exit 1; }

mv ${DISTFILENAME} ${LOCAL_DIR} || { echo "Cannot move tar.gz file"; exit 1; }

cd "$LOCAL_DIR"

echo " >>> Finishing"

cd ..

# Delete cloned stratiospark project
rm -rf ${TMPDIR}  || { echo "Cannot remove stratiodeep-clone project"; exit 1; }

# Delete cloned Stratio Deep project
rm -rf ${TMPDIR_SPARK}  || { echo "Cannot remove stratiospark-clone project"; exit 1; }

cd "$LOCAL_DIR"/..
git checkout develop

echo " >>> SCRIPT EXECUTION FINISHED <<< "
