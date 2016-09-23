##
## Copyright (C) 2016 Zalando SE
##
## This software may be modified and distributed under the terms
## of the MIT license.  See the LICENSE file for details.
##
FROM registry.opensource.zalan.do/stups/openjdk:8-37

ENV  APP   znap
ARG  SCALA=2.11
ARG  VSN=

COPY target/scala-${SCALA}/${APP}-assembly-${VSN}.jar /${APP}.jar
COPY scm-source.json /scm-source.json

RUN curl -o /jolokia.jar https://repo1.maven.org/maven2/org/jolokia/jolokia-jvm/1.3.4/jolokia-jvm-1.3.4-agent.jar

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

EXPOSE 8080

ENTRYPOINT java ${JAVA_OPTS} -Dconfig.resource=/application.conf \
           -javaagent:/jolokia.jar=port=8778,host=0.0.0.0 \
           -jar $APP.jar
