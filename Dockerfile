FROM openjdk

WORKDIR /app

COPY target/kafka-0.0.1-SNAPSHOT.jar /app/kafka.jar

ENTRYPOINT ["java", "-jar", "kafka.jar"]


