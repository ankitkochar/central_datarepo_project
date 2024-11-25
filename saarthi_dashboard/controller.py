# Library
import os
import logging
from typing import Dict, Any
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

# Modules
from constants import es_institute_index_name

from .utils import (
    add_prompt,
    delete_prompt,
    get_all_prompts,
    update_prompt,
    update_institute_fields,
    fetch_all_cld_ids,
    is_valid_institute_id,
    is_valid_institute_name,
    is_valid_institute_url,
    update_fields_in_institute_table,
    fetch_inst_url_and_name,
    find_latest_updates,
    add_latest_news,
    update_institute_field_for_reccomended_url_run,
    fetch_for_recommended_run,
    update_urls_for_cld_id_for_recommended_url_institute,
    find_refund_policy,
    add_refund_policy_links,
    populate_inst_master_course,
    populate_inst_specific_course
)
from crawling.utils import update_scrape_data_status
from embedding.utils import update_institute_embedding_status
from utils.url_recommended import url_recommended

# Initialization
load_dotenv()
es_user = os.getenv("ELASTICSEARCH_USER")
es_password = os.getenv("ELASTICSEARCH_PASSWORD")
es_host = os.getenv("ELASTIC_SEARCH_HOST")
chunk_index_by_sentence = os.environ.get("CHUNK_INDEX_SENTENCE")

es = Elasticsearch(es_host, basic_auth=(es_user, es_password))


def get_es_client(custom_settings: Dict[str, Any] = None) -> Elasticsearch:
    default_settings = {
        "hosts": [es_host],
        "basic_auth": (es_user, es_password),
    }

    if custom_settings:
        default_settings.update(custom_settings)

    return Elasticsearch(**default_settings)


# Controllers
def fetch_all_institutes(item):
    try:
        institutes = []
        # query = {"query": {"term": {"embedding_generated": True}}, "size": 10000}

        # Execute the search query
        with get_es_client() as esg:

            response = esg.search(index=es_institute_index_name, size=10000)

        # Process the response to get the hits
        hits = response["hits"]["hits"]

        # Print the results
        for hit in hits:
            institute = hit["_source"]
            institutes.append(institute)

        return institutes
    except Exception as e:
        errMsg = str(e)
        print("Error while fetching all institutes:", errMsg)


def prompt_crud_operations(item):
    operation = item.operation
    prompt = item.data
    id = item.prompt_id

    response = ""

    if operation == "VIEW":
        response = get_all_prompts()
    elif operation == "UPDATE":
        response = update_prompt(id, prompt)
    elif operation == "DELETE":
        response = delete_prompt(id)
    elif operation == "CREATE":
        response = add_prompt(prompt)
    else:
        response = "Invalid operation"

    return response


def populate_institutes_to_scrape(item):
    try:
        error_messages = {}
        cld_ids = fetch_all_cld_ids("institute")
        df = pd.read_csv(item.file)
        for index, row in df.iterrows():
            institute_id = row["id"]
            institute_name = row["name"]
            institute_url = row["url"]

            if (pd.isna(institute_id) or pd.isna(institute_name) or pd.isna(institute_url) or institute_id == '' or institute_name == '' or institute_url == ''):
                error_messages[str(institute_id)] = f"Failure: Please Check Entry at row {index + 1}. One or more fields are empty or 'nan'."
                logging.error(error_messages[str(institute_id)])
                continue

            if not is_valid_institute_id(institute_id):
                error_messages[institute_id] = f"Failure: Invalid institute ID {institute_id} at row {index + 1}."
                logging.error(error_messages[institute_id])
                continue

            if not is_valid_institute_name(institute_name):
                error_messages[institute_id] = f"Failure: Invalid institute name '{institute_name}' at row {index + 1}."
                logging.error(error_messages[institute_id])
                continue

            if not is_valid_institute_url(institute_url):
                error_messages[institute_id] = f"Failure: Invalid institute URL '{institute_url}' at row {index + 1}."
                logging.error(error_messages[institute_id])
                continue

            if institute_id not in cld_ids:
                doc = {
                    "cld_id": institute_id,
                    "url": institute_url,
                    'name' : institute_name,
                    "downloaded": False,
                    "embedding_generated": False,
                    "prompt_output_generated": False,
                    "status": True,
                    "created_at": datetime.now(),
                    "updated_at": datetime.now(),
                }
                es.index(index=es_institute_index_name, body=doc)
                logging.info(
                    f"New Entry created for cld_id {institute_id} in the institute index."
                )
            else:
                update_institute_fields(es_institute_index_name, institute_id, institute_name, institute_url)

        if not error_messages:
            return "Success"
        else:
            return error_messages

    except Exception as e:
        errMsg = str(e)
        logging.error("Error while fetching all institutes:", errMsg)
        return f"Failure: {errMsg}"


def add_institute_to_master_courses(item):
    csv_file = item.file
    try:
        result = populate_inst_master_course(csv_file)
        return result
    except Exception as e:
        return f"Error while populating institute master course table: {e}"


def add_institute_specific_courses(item):
    try:
        result = populate_inst_specific_course(item.file)
        return result
    except Exception as e:
        return f"Error while populating institute institute specific course table: {e}"

def populate_institutes_for_recommended_url(item):
    try:
        error_messages = {}
        cld_ids = fetch_all_cld_ids("recommended_url_institute")
        df = pd.read_csv(item.file)
        for index, row in df.iterrows():
            institute_id = row["id"]
            institute_name = row["name"]
            institute_url = row["url"]
            institute_city = row["city"]
            institute_state = row["state"]


            if (pd.isna(institute_id) or pd.isna(institute_name) or pd.isna(institute_city) or pd.isna(institute_state) or institute_id == '' or institute_name == '' or institute_state == '' or institute_city == ''):
                error_messages[str(institute_id)] = f"Failure: Please Check Entry at row {index + 1}. One or more fields are empty."
                logging.error(error_messages[str(institute_id)])
                continue

            if (pd.isna(institute_url)):
                institute_url = ""

            if not is_valid_institute_id(institute_id):
                error_messages[institute_id] = f"Failure: Invalid institute ID {institute_id} at row {index + 1}."
                logging.error(error_messages[institute_id])
                continue

            if not is_valid_institute_name(institute_name):
                error_messages[institute_id] = f"Failure: Invalid institute name '{institute_name}' at row {index + 1}."
                logging.error(error_messages[institute_id])
                continue

            if institute_id not in cld_ids:
                doc = {
                    "cld_id": institute_id,
                    "url": institute_url,
                    'name' : institute_name,
                    "city": institute_city,
                    "state": institute_state,
                    "status": True,
                    "recommended_run": False,
                    "created_at": datetime.now(),
                    "updated_at": datetime.now(),
                }
                es.index(index="recommended_url_institute", body=doc)
                logging.info(
                    f"New Entry created for cld_id {institute_id} in the institute index."
                )
            else:
                update_institute_field_for_reccomended_url_run("recommended_url_institute", institute_id, institute_name, institute_url, institute_city, institute_state)

        if not error_messages:
            return "Success"
        else:
            return error_messages

    except Exception as e:
        errMsg = str(e)
        logging.error("Error while fetching all institutes:", errMsg)
        return f"Failure: {errMsg}"

def institute_crud(item):
    institute_id = item.institute_id
    embedding_generated = item.embedding_generated
    downloaded = item.downloaded

    res = update_fields_in_institute_table(institute_id, downloaded, embedding_generated)
    return res

def institute_reset(item):
    response = {}
    df = pd.read_csv(item.file)
    for index, row in df.iterrows():
        institute_id = int(row["id"])
        try:
            update_fields_in_institute_table(institute_id, False, False, False)
            update_scrape_data_status(institute_id, "scraper_info")
            update_institute_embedding_status(institute_id, "chunk_by_sentence")
            response[institute_id] = 'Success'
        except Exception as e:
            logging.error(f"Error while reset institute: {institute_id}. Error: {e}")
            response[institute_id] = str(e)
    
    return response


def get_institute_latest_news(item):
    institute_ids = item.institute_ids
    response = {}

    for inst_id in institute_ids:
        try:
            inst_name, inst_url = fetch_inst_url_and_name(inst_id)

            if not inst_name or not inst_url:
                response[inst_id] = 'Institute Name or Url is Not Found'
            else:
                latest_news = find_latest_updates(inst_url,inst_name)
                add_latest_news(inst_id, latest_news)
                response[inst_id] = 'Success'

        except Exception as e:
            response[inst_id] = str(e)

    return response


def get_institute_refund_policies(item):
    institute_ids = item.institute_ids
    response = {}

    for inst_id in institute_ids:
        try:
            inst_name, inst_url = fetch_inst_url_and_name(inst_id)

            if not inst_name or not inst_url:
                response[inst_id] = 'Institute Name or Url is Not Found'
            else:
                refund_policy_links = find_refund_policy(inst_url,inst_name)
                add_refund_policy_links(inst_id, refund_policy_links, inst_name)
                response[inst_id] = 'Success'

        except Exception as e:
            response[inst_id] = str(e)

    return response

def run_institutes_for_recommended_url(data=None):
    institute_rows = fetch_for_recommended_run("recommended_url_institute")
    response = {}
    for row in institute_rows:
        try:
            all_urls, final_url = [], ""
            cld_id = row["cld_id"]
            name = row["name"]
            city = row["city"]
            state = row["state"]
            all_urls, final_url = url_recommended(name, city, state)
            update_urls_for_cld_id_for_recommended_url_institute(cld_id, all_urls, final_url, "recommended_url_institute")
            response[cld_id] = 'Success'
        except Exception as e:
            response[cld_id] = str(e)
    
    return response
