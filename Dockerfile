FROM apache/spark:3.5.1

USER root

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# Add Hadoop AWS JARs
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar $SPARK_HOME/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar $SPARK_HOME/jars/

# Install JupyterLab and findspark
RUN pip install --no-cache-dir jupyterlab findspark

WORKDIR $SPARK_HOME/work-dir

EXPOSE 8888 4040

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''", "--NotebookApp.password=''"]