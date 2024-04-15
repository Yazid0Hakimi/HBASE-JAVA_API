FROM openjdk:8-jdk-alpine
COPY target/TP_HBASE_2-1.0-SNAPSHOT-shaded.jar /app.jar
CMD ["java", "-jar", "/app.jar"]
