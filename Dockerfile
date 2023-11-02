FROM ubuntu:20.04

ENV DEBIAN_FRONTEND noninteractive

WORKDIR /usr/src/app

# create logs folder
RUN mkdir /var/logs && chmod -R 777 /var/logs

#Install dependencies and tools

RUN apt-get update  -y\
&& apt-get upgrade -y\
&& apt-get install libsasl2-dev libsasl2-2 libsasl2-modules-gssapi-mit  -y\
&& rm -rf /var/lib/apt/lists/*\
&& apt-get update  -y\
&& apt-get -qq autoclean

RUN apt-get update  -y \
&& apt-get install python3-tk -y

RUN apt-get update  -y\
&& apt-get install python3-pip -y 


RUN pip3 install --upgrade pip \
&& mkdir /.local /.cache \
&& chmod -R 777 /.local /.cache


RUN pip3 install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host=files.pythonhosted.org --no-cache-dir --upgrade pip setuptools

COPY . /usr/src/app

COPY certs/cargill-entrust-combined.pem /usr/local/share/ca-certificates/cargill-entrust-combined.crt

RUN update-ca-certificates



RUN pip3 install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host=files.pythonhosted.org --no-cache-dir --default-timeout=100  -r requirements.txt

# ARG DEBIAN_FRONTEND=noninteractive
# ENV TZ=US/Central
# RUN apt-get install -y python3-tk

# RUN apt-get update  -y\
# &&apt-get install python3-tk -y
# Needed for Celery
RUN export LC_ALL=C.UTF-8 && export LANG=C.UTF-8
RUN printf '#!/bin/sh\nexit 0' > /usr/sbin/policy-rc.d

# RUN apt-get update && apt-get upgrade -y
# RUN apt-get install -y --no-install-recommends redis-server

# RUN apt-get update && apt-get install -y supervisor
RUN pip3 install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host=files.pythonhosted.org --no-cache-dir supervisor
RUN mkdir -p /var/log/supervisor

# COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf     
# COPY redis.conf /etc/redis.conf

# Environment and Entry point to execute python based app.
EXPOSE 8000

# EXPOSE 5555

# Datadog
# ENV DD_SERVICE=supercommbackend
# ENV DD_ENV=dev 
# ENV DD_LOGS_INJECTION=true

# RUN redis-cli config set stop-writes-on-bgsave-error no

# RUN python3 cp_snowflake_api/manage.py makemigrations
# RUN python3 cp_snowflake_api/manage.py migrate

# ENTRYPOINT ["python3", "./cp_snowflake_api/manage.py", "runserver", "0.0.0.0:8000"]
ENTRYPOINT ["supervisord"]
# WORKDIR /usr/src/app/supercomm   
# RUN ["chmod", "+x", "./entrypoint.sh"]
# ENTRYPOINT ["./entrypoint.sh"]
