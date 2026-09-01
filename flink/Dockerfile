FROM flink:1.20.0-scala_2.12-java11

# Copiar el JAR construido
COPY target/flink-job-*.jar /opt/flink/usrlib/flink-job.jar

# Crear directorio para datos
RUN mkdir -p /opt/flink/data && \
    chown -R flink:flink /opt/flink/data

USER flink

# El JAR se ejecutará vía FlinkDeployment