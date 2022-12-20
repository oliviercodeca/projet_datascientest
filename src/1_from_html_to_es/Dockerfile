FROM debian:latest
RUN apt-get update && apt-get install python3-pip -y && pip3 install requests && pip3 install tqdm && pip3 install elasticsearch

ADD scraping_trustpilot.py /home/scraping_trustpilot.py
ADD custom_es_vars.py /home/custom_es_vars.py

WORKDIR /home/
CMD python3 scraping_trustpilot.py