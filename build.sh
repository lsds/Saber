#!/bin/sh

SABER="."
[ ! -d "lib" ] && mkdir lib/
mvn package
cp $SABER/target/saber-0.0.1-SNAPSHOT.jar lib/

exit 0
