FROM maven:3.9-eclipse-temurin-23-alpine as BUILDER
ARG VERSION=0.0.1-SNAPSHOT
WORKDIR /opt/build/api
COPY pom.xml /opt/build/api/
COPY src /opt/build/api/src/
RUN mvn -f /opt/build/api/pom.xml clean package -B -DskipTests


FROM eclipse-temurin:23-jdk-alpine
WORKDIR /opt/app
COPY --from=BUILDER /opt/build/api/target/*.jar /opt/app/api.jar

EXPOSE 8080 8091

ENTRYPOINT ["java","-jar", "/opt/app/api.jar"]