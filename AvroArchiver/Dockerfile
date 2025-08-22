FROM eclipse-temurin:21-jdk
#WORKDIR /Users/danielreuben/test/AvroArchiver/AvroArchiver
WORKDIR /app
COPY OrderJob.yaml OrderJob.yaml 
# Create a safe folder for the JAR
RUN mkdir -p /bin
COPY target/avro-archiver-0.0.1-SNAPSHOT.jar /bin/app.jar
ENTRYPOINT ["java", "-jar", "/bin/app.jar"]

# docker build -t avro-archiver:latest .
# docker run --rm avro-archiver:latest
# docker run --rm -v /Users/danielreuben/test/AvroArchiver/AvroArchiver:/app -w /app avro-archiver

