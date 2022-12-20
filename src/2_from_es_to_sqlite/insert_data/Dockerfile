FROM debian:latest
RUN apt-get update && apt-get install python3-pip -y && pip3 install elasticsearch && pip3 install elasticsearch_dsl

ADD insert_data.py /home/insert_data.py
ADD custom_vars_db.py /home/custom_vars_db.py

WORKDIR /home/
CMD python3 insert_data.py