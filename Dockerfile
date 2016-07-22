##
## Copyright (C) 2016 Zalando SE
##
## This software may be modified and distributed under the terms
## of the MIT license.  See the LICENSE file for details.
##
FROM registry.opensource.zalan.do/stups/openjdk:8-29

ENV  APP   znap
ARG  SCALA=2.11
ARG  VSN=

COPY target/scala-${SCALA}/${APP}-assembly-${VSN}.jar /${APP}.jar
COPY scm-source.json /scm-source.json

ENV  JAVA_OPTS="\
   -server \
   -XX:+UseNUMA \
   -XX:+UseCondCardMark \
   -XX:-UseBiasedLocking \
   -Xms1024M \
   -Xmx1024M \
   -Xss1M \
   -XX:MaxPermSize=128m \
   -XX:+UseParallelGC"

ENTRYPOINT java ${JAVA_OPTS} -Dconfig.resource=/application.conf -jar $APP.jar
