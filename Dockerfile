# For Java 11, try this
FROM adoptopenjdk/openjdk11:alpine-jre

# Refer to Maven build -> finalName
ARG JAR_FILE=target/linkedin-batch-0.0.1-SNAPSHOT.jar

# cd /opt/app
WORKDIR /opt/app

# cp target/spring-boot-web.jar /opt/app/batchapp.jar
COPY ${JAR_FILE} batchapp.jar

# java -jar /opt/app/app.jar
ENTRYPOINT ["java","-jar","batchapp.jar"]