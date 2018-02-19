#!/bin/bash

set -e
git pull

mvn clean install

VERSION=`grep -o -a -m 1 -h -r "version>.*</version" ./pom.xml | head -1 | sed "s/version//g" | sed "s/>//" | sed "s/<\///g"`
SERVER='snurran.sics.se'

echo ""
echo "Deploying hops-examples-spark-${VERSION}.jar to ${SERVER}"
echo ""
scp spark/target/hops-examples-spark-${VERSION}.jar glassfish@snurran.sics.se:/var/www/hops


echo ""
echo "Deploying hops-examples-flink-${VERSION}.jar to ${SERVER}"
echo ""
scp flink/target/hops-examples-flink-${VERSION}.jar glassfish@snurran.sics.se:/var/www/hops

echo ""
echo "Deploying hops-examples-hive-${VERSION}.jar to ${SERVER}"
echo ""
scp hive/target/hops-examples-hive-${VERSION}.jar glassfish@snurran.sics.se:/var/www/hops
