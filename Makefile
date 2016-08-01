##
## Copyright (C) 2016 Zalando SE
##
## This software may be modified and distributed under the terms
## of the MIT license.  See the LICENSE file for details.
##


APP     = znap
SCALA   = 2.11

## container / stack identity
URL    ?= registry.opensource.zalan.do/ie
VSN    ?= $(shell test -z "`git status --porcelain`" && git describe --tags --long | sed -e 's/-g[0-9a-f]*//' | sed -e 's/-0//' || echo "`git describe --abbrev=0 --tags`-SNAPSHOT")

##
## docker image build flags
DFLAGS = \
   --build-arg SCALA=${SCALA} \
   --build-arg VSN=${VSN}


all: compile docker run

#####################################################################
##
## compile
##
#####################################################################
compile: scm-source.json
	@sbt -Dversion=${VSN} assembly 

scm-source.json: force
	@sh -c '\
		REV=$$(git rev-parse HEAD); \
		URL=$$(git config --get remote.upstream.url || git config --get remote.origin.url); \
		STATUS=$$(git status --porcelain |awk 1 ORS="\\\\\\\\n"); \
		if [ -n "$$STATUS" ]; then REV="$$REV (locally modified)"; fi; \
		echo "{\"url\": \"git:$$URL\", \"revision\": \"$$REV\", \"author\": \"$$USER\", \"status\": \"$$STATUS\"}" > scm-source.json'

#####################################################################
##
## package
##
#####################################################################
docker:
	docker build ${DFLAGS} -t ${URL}/${APP}:${VSN} .

force:


.PHONY: compile

