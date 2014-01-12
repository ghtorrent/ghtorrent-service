#!/usr/bin/env bash

if [ -z $1 ]; then
  echo "usage: $0 job.txt"
  echo "$0: create and package dumps for the GHTorrent project service" 
  echo "  job.txt: contains a list of repositories to dump. The name prefix"
  echo "           must be the same with the MySQL DB containing the"
  echo "           project data"
  exit 1
fi

topdir=`pwd`
jobname=`echo $1|cut -f1 -d'.'`
echo Jobname:  $jobname
mkdir -p $jobname
cd $jobname

#1. 
echo "Dumping MySQL db $jobname"
#echo mysqldump -u root -p'george' $jobname > mysql.dump

#2. 
cat ../$1|
grep -v "\#"|
while read project; 
do
  owner=`echo $project|cut -f1 -d' '` 
  repo=`echo $project|cut -f2 -d' '` 
  mkdir $owner-$repo
  cd $project
  echo git clone git@github.com:$owner/$repo.git git 
  for collection in repo_labels repo_collaborators watchers forks pull_requests pull_request_comments issues issue_events issue_comments; do
    echo mongodump -h dutihr --collection --query '{owner:"$owner", repo:"$repo"}}'
  done
  cd -
done

cd $topdir 

