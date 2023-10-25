# Use the official airflow base image
FROM apache/airflow:2.6.3

# Extend the image by copying the requirements file in the project folder to the docker image
# Note that the requirements file wasn't copied to the projects root directory but the container's root
# requirements file spelled incorrectly here, don't waste 20 days figuring out what's wrong
#COPY requirements.txt /requirments.txt

WORKDIR /opt/airflow/
COPY requirements.txt /requirements.txt
COPY .env .env

# upgrade pip then install specified packages in requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt \
    #no-cache-dir used to not save the downloaded packages locally. manage image size

# install nano and vim
# must set user to root first before running elevated commands, then set it back
USER root
RUN apt-get update && apt-get install -y nano
RUN apt install -y net-tools
RUN apt-get update && apt-get install -y telnet
RUN apt-get install -y busybox
RUN apt-get update && apt-get install -y iputils-ping
RUN #apt-get update && apt-get install -y vim
USER airflow
