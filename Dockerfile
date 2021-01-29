# Testing environment for AIOHappyBase
FROM aiudirog/hbase:latest

RUN apt-get update
RUN apt-get install -y software-properties-common
RUN apt-get update
RUN add-apt-repository ppa:deadsnakes/ppa -y
RUN apt-get update
RUN apt-get install -y \
    'python3.6' 'python3.7' 'python3.8' 'python3.9' \
    'python3.6-dev' 'python3.7-dev' 'python3.8-dev' 'python3.9-dev' \
    python3-pip net-tools git
