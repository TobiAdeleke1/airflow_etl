# Pull official base image : from https://airflow.apache.org/docs/docker-stack/index.html
FROM apache/airflow:2.5.0

# install app dependencies
# RUN apt-get update

#set work directory
WORKDIR /app

#copy project 
COPY . . 
