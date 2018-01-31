#!/bin/bash
mvn clean package -DskipTests
cd ~/Downloads/streamsets-datacollector-3.0.0.0/user-libs/
tar xvfz /home/mpo/Documents/fraunhofer/aegis/streamSetsPlayground/hopsworks-destination/target/hopsworks-destination-1.0-SNAPSHOT.tar.gz
x hopsworks-destination/lib/hopsworks-destination-1.0-SNAPSHOT.jar

