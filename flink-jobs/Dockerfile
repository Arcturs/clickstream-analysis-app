FROM maven:3.9-eclipse-temurin-23-alpine as BUILDER
ARG VERSION=0.0.1-SNAPSHOT
WORKDIR /opt/build/flinkjobs
COPY pom.xml /opt/build/flinkjobs/
COPY src /opt/build/flinkjobs/src/
RUN mvn -f /opt/build/flinkjobs/pom.xml clean package -B -DskipTests


FROM eclipse-temurin:23-jdk-alpine
WORKDIR /opt/app/flinkjobs
COPY --from=BUILDER /opt/build/flinkjobs/target/*.jar /opt/app/flinkjobs.jar

EXPOSE 8080 8091

ENTRYPOINT ["java", "--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED", "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED", "--add-opens=java.base/java.io=ALL-UNNAMED", "--add-opens=java.base/java.net=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED", "--add-opens=java.base/java.util=ALL-UNNAMED", "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED", "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED", "--add-opens=java.base/sun.security.action=ALL-UNNAMED", "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED", "--add-opens=java.base/java.time=ALL-UNNAMED", "-Djava.security.manager=allow", "-jar", "/opt/app/flinkjobs.jar"]