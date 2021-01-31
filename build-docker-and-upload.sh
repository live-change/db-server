#!/bin/bash
VERSION=`echo "console.log(require('./package.json').version)" | node`
echo building docker image for version $VERSION
docker build -t livechange/db-server:$VERSION -t livechange/db-server:latest .
echo uploading docker image to docker hub
docker push livechange/db-server:$VERSION
docker push livechange/db-server:latest

