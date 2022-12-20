#!/bin/bash

# create image
docker image build ./src/1_from_html_to_es -t scraping_trustpilot:latest
docker image build ./src/2_from_es_to_sqlite/create_tables -t create_tables:latest
docker image build ./src/2_from_es_to_sqlite/insert_data -t insert_data:latest


# run docker-compoe
docker-compose up