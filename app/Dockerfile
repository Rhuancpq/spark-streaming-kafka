FROM bitnami/spark:2.4.6

RUN curl https://repo1.maven.org/maven2/org/apache/spark/\
spark-streaming-kafka-0-8-assembly_2.11/2.4.6/\
spark-streaming-kafka-0-8-assembly_2.11-2.4.6.jar \ 
--output /opt/bitnami/spark/jars/spark-streaming-kafka-0-8-assembly_2.11-2.4.6.jar

WORKDIR /app

USER root

COPY requirements.txt ./requirements.txt

RUN pip3 install -r requirements.txt --user 

COPY . ./

ENTRYPOINT [ "spark-submit", "--jars", "/opt/bitnami/spark/jars/spark-streaming-kafka-0-8-assembly_2.11-2.4.6.jar", "./app.py" ]

# ENTRYPOINT ["tail", "-F", "/dev/null"]
