FROM eclipse-temurin:17-jdk
WORKDIR /Users/danielreuben/test/AvroArchiver.old/AvroArchiver
COPY OrderJob.yaml OrderJob.yaml 
COPY arc.png arc.png
COPY target/avro-archiver-0.0.1-SNAPSHOT.jar avro-archiver-0.0.1-SNAPSHOT.jar
ENTRYPOINT ["java", "-jar", "avro-archiver-0.0.1-SNAPSHOT.jar"]

# docker build -t avro-archiver:latest .
# docker run --rm avro-archiver:latest
