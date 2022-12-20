#!/usr/bin/env python3

import requests, re, os, json
from tqdm import tqdm
from elasticsearch import Elasticsearch
from custom_es_vars import es_vars
from elasticsearch import client

absolute_path = '/home/files_to_import_to_es'
es_address = 'elasticsearch'
es_port = 9200

# steps:
# 1- create all categories file -> categories.json
# 2- create  all companies file -> companies_default.json



# create the file categories.json in bulk Elasticsearch format
urlCat = 'https://fr.trustpilot.com/categories'
html_respCat = requests.get(url=urlCat)
html_substr_cat = re.findall('"subCategories"\:\{.+\]\}\]\}\,"languages"', html_respCat.text)
html_cat_data = '{' + html_substr_cat[0][:-12] + '}'
html_cat = '{"create":{"_index":"cat_1","_id":"1"}}\n' + str(html_cat_data) + '\n'
with open("{file_path}/categories.json".format(file_path=absolute_path), "w", encoding='utf-8') as file:
    file.write(html_cat)

# create a list with all the categories
list_cat = []
dict_cat = json.loads(html_cat_data)
for k_cat in dict_cat["subCategories"]:
    for l in dict_cat["subCategories"][k_cat]:
        list_cat.append(l["categoryId"])


# delete the company file if exsts
json_name = "companies_default"
if os.path.exists("{file_path}/{json_name}.json".format(json_name=json_name, file_path=absolute_path)):
  os.remove("{file_path}/{json_name}.json".format(json_name=json_name, file_path=absolute_path))

# create companies file
html_all_comp = ""
for cat_name in tqdm(["hotels"]):
    
    # get the last num html page to loop on page
    urlCompLast = 'https://fr.trustpilot.com/categories/{cat_name}'.format(cat_name=cat_name)
    html_respCompLast = requests.get(url=urlCompLast)
    m = re.search('"totalPages"\:(\d+)\,"perPage"', html_respCompLast.text)
    if m:
        html_last_num_page = int(m.group(1))

    # loop on page
    for num_page in range(1, html_last_num_page+1):
        doc_name = cat_name + "_" + str(num_page)
        urlComp = 'https://fr.trustpilot.com/categories/{cat_name}?page={num_page}'.format(cat_name=cat_name, num_page=num_page)
        html_respComp = requests.get(url=urlComp)
        if html_respComp.status_code == 200:
            html_substr_comp = re.findall('"businesses"\:\[\{"businessUnitId".+\}\]\}\]\,"totalPages"', html_respComp.text)
            html_comp = r'{"create":{"_index":"comp_1","_id":"' + '{doc_name}"}}\n'.format(doc_name=doc_name) + '{' + html_substr_comp[0][:-13] + '}\n'
            html_all_comp += html_comp
        else:
            break

with open("{file_path}/{json_name}.json".format(json_name=json_name, file_path=absolute_path), "a", encoding='utf-8') as file:
    file.write(html_all_comp)

####################################################################
# put mapping to es ################################################
####################################################################

conn = client.IndicesClient(Elasticsearch(hosts = "http://{es_address}:{es_port}".format(es_address=es_address, es_port=es_port)))

mapping_category = eval(es_vars("mapping_cat").replace("/n", "").replace(" ", ""))
mapping_company = eval(es_vars("mapping_comp").replace("/n", "").replace(" ", ""))
mapping_review = eval(es_vars("mapping_rev").replace("/n", "").replace(" ", ""))

resp = conn.create(index="cat_1", mappings=mapping_category)
print(resp)

resp = conn.create(index="comp_1", mappings=mapping_company)
print(resp)

resp = conn.create(index="rev_1", mappings=mapping_review)
print(resp)

####################################################################
# bulk to load data to es ##########################################
####################################################################

#bulk categories
client = Elasticsearch(hosts = "http://{es_address}:{es_port}".format(es_address=es_address, es_port=es_port))
with open("{file_path}/categories.json".format(file_path=absolute_path), "r") as f:
    resp = client.bulk(body=[f.read()]) 
    client.close()

#bulk default companies 
client = Elasticsearch(hosts = "http://{es_address}:{es_port}".format(es_address=es_address, es_port=es_port))
with open("{file_path}/companies_default.json".format(file_path=absolute_path), "r") as f:
    resp = client.bulk(body=[f.read()])
    client.close()