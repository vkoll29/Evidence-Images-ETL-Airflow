# Use the official airflow base image
FROM apache/airflow:2.6.3

# Extend the image by copying the requirements file in the project folder to the docker image
# Note that the requirements file wasn't copied to the projects root directory but the container's root
# requirements file spelled incorrectly here, don't waste 20 days figuring out what's wrong
COPY requirments.txt /requirments.txt

WORKDIR /opt/airflow/
COPY .env .env

# upgrade pip then install specified packages in requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirments.txt

# install nano and vim
# must set user to root first before running elevated commands, then set it back
USER root
RUN apt-get update && apt-get install -y nano
RUN apt-get update && apt-get install -y vim
USER airflow
