FROM debian:latest
RUN apt-get update && apt-get install python3-pip -y

ADD create_tables.py /home/create_tables.py
ADD custom_vars_db.py /home/custom_vars_db.py

WORKDIR /home/
CMD python3 create_tables.py