FROM maven:3.9-eclipse-temurin-23-alpine as BUILDER
ARG VERSION=0.0.1-SNAPSHOT
WORKDIR /opt/build/generator
COPY pom.xml /opt/build/generator/
COPY src /opt/build/generator/src/
RUN mvn -f /opt/build/generator/pom.xml clean package -B -DskipTests


FROM eclipse-temurin:23-jdk-alpine
WORKDIR /opt/app
COPY --from=BUILDER /opt/build/generator/target/*.jar /opt/app/generator.jar

EXPOSE 8080 8091

ENTRYPOINT ["java","-jar", "/opt/app/generator.jar"]