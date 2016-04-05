#!/bin/bash
mvn clean package
scp target/WordCountExtended-jar-with-dependencies.jar zuphux.metacentrum.cz:/storage/brno2/home/stankmic/

