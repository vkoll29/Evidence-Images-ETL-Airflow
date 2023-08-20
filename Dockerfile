# Use the official airflow base image
FROM apache/airflow:2.6.3

# Extend the image by copying the requirements file in the project folder to the docker image
# requirements file spelled incorrectly here, don't waste 20 days figuring out what's wrong
COPY requirments.txt /requirments.txt

# upgrade pip then install specified packages in requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirments.txt

