FROM eclipse-temurin:21.0.7_6-jre
WORKDIR /app
COPY verification-tools/target/verification-tools-1.0-SNAPSHOT.jar /app/collector.jar
USER 65534:65534
CMD ["java", "-jar", "/app/collector.jar", "collector"]
