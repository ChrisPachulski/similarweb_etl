FROM debian:stretch-slim

RUN apt-get update && apt-get install -y --no-install-recommends apt-utils  && 
&& wget https://www.python.org/ftp/python/3.8.0/Python-3.8.0.tar.xz && tar xf Python-3.8.0.tar.xz \
&& cd ./Python-3.8.0  && ./configure && make && make install && update-alternatives --install /usr/bin/python3.5 python3.5 /usr/local/bin/python3.8 10 \
&& update-alternatives --install /usr/bin/python3 python3 /usr/local/bin/python3.8 10 \
&& cd /tmp && 
&& apt-get update -y && apt-get install -y lftp dos2unix \ 
&& pip3 install --upgrade pip \
&& pip3 install pandas==1.3.5 numpy==1.21.6 openpyxl==3.0.10 ConfigParser==5.2.0 Jinja2==2.11.3  clickhouse_driver==0.2.4 xlrd==2.0.1 iso3166==2.1.1 petl==1.7.10  pyjanitor==0.22.0 


RUN mkdir -p /root/.config/similarweb/
COPY resources/similarweb_secret.json /root/.config/similarweb/

WORKDIR /scripts
RUN mkdir -p /data/attachment