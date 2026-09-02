FROM flink:1.20.0-scala_2.12-java11

# 🔴 CORREGIDO: Plugin de Azure Filesystem desde /opt/flink/opt/
RUN mkdir -p /opt/flink/plugins/azure-fs-hadoop && \
    cp /opt/flink/opt/flink-azure-fs-hadoop-1.20.0.jar \
       /opt/flink/plugins/azure-fs-hadoop/

# Copiar el JAR del Job
COPY target/flink-job-*.jar /opt/flink/usrlib/flink-job.jar

# Crear directorio para datos
RUN mkdir -p /opt/flink/data && \
    chown -R flink:flink /opt/flink/data

USER flink