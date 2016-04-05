#!/bin/bash
mvn clean package
scp target/WordCountExtended.jar zuphux.metacentrum.cz:/storage/brno2/home/stankmic/

