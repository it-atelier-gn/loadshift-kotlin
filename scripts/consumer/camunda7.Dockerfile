FROM eclipse-temurin:21-jre
ADD https://downloads.camunda.cloud/release/camunda-bpm/run/7.24/camunda-bpm-run-7.24.0.tar.gz /tmp/camunda-bpm-run.tar.gz
RUN mkdir -p /camunda && tar -xzf /tmp/camunda-bpm-run.tar.gz -C /camunda && rm /tmp/camunda-bpm-run.tar.gz
WORKDIR /camunda
EXPOSE 8080
CMD ["./internal/run.sh", "start", "--rest", "--webapps"]
