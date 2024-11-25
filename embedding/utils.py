# Library
import asyncio
import os
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
import logging

# Modules
from crawling.utils import update_institute_generation_status
from constants import es_institute_index_name
from .create_embeddings import process_all_documents

# Initialization
load_dotenv()
es_user = os.getenv("ELASTICSEARCH_USER")
es_password = os.getenv("ELASTICSEARCH_PASSWORD")

es_host = os.getenv("ELASTIC_SEARCH_HOST")
es = Elasticsearch(es_host, basic_auth=(es_user, es_password))

aws_access_key = os.environ.get("AWS_ACCESS_KEY")
aws_secret_key = os.environ.get("AWS_SECRET_KEY")
aws_region = os.environ.get("AWS_REGION")
chunk_index_by_word = os.environ.get("CHUNK_INDEX")
chunk_index_by_sentence = os.environ.get("CHUNK_INDEX_SENTENCE")
chunk_index_by_passage = os.environ.get("CHUNK_INDEX_PASSAGE")
temp_dw_folder = os.environ.get("TEMP_DW_FOLDER")


def get_es_client(custom_settings: Dict[str, Any] = None) -> Elasticsearch:
    default_settings = {
        "hosts": [es_host],
        "basic_auth": (es_user, es_password),
    }

    if custom_settings:
        default_settings.update(custom_settings)

    return Elasticsearch(**default_settings)


# Helper Functions
async def make_function_async(function_to_call, data):
    with ThreadPoolExecutor() as pool:
        result = await asyncio.get_running_loop().run_in_executor(
            pool, function_to_call, data
        )
    return result


def get_chunk_index(index):
    chunk_index = chunk_index_by_word
    if index == "passage":
        chunk_index = chunk_index_by_passage
    elif index == "sentence":
        chunk_index = chunk_index_by_sentence
    else:
        chunk_index = chunk_index_by_word

    return chunk_index


def check_id_already_exists(inst_id):

    query = {"query": {"match": {"cld_id": int(inst_id)}}}
    with get_es_client() as esg:

        result = esg.search(index=es_institute_index_name, body=query)
    does_entry_exists = result["hits"]["total"]["value"] > 0

    if not does_entry_exists:
        return False
    else:
        result_dict = result["hits"]["hits"][0]["_source"]
        if result_dict["embedding_generated"]:
            return True


def fetch_scrape_data(inst_id):

    query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"institute_id": inst_id}},
                    {"match": {"status": True}},
                ]
            }
        }
    }
    with get_es_client() as esg:
        result = esg.search(index="scraper_info", body=query, size=10000)

    total = []

    for hit in result["hits"]["hits"]:
        data = hit["_source"]
        total.append(
            {
                "actual_url": data["actual_url"],
                "s3_url": data["s3_url"],
            }
        )

    return total


def update_institute_embedding_status(inst_id, chunk_index):
    query = {
        "script": {
            "source": "ctx._source.status = params.new_status",
            "lang": "painless",
            "params": {"new_status": False},
        },
        "query": {
            "bool": {
                "must": [
                    {"match": {"institute_id": inst_id}},
                    {"match": {"status": True}},
                ]
            }
        },
    }
    es.indices.refresh(index=chunk_index)
    es.update_by_query(index=chunk_index, body=query)


def generate_embedding(inst_id, chunk_index, index_type):
    try:
        update_institute_embedding_status(inst_id, chunk_index)
        institute_scraped_data = fetch_scrape_data(inst_id)

        # Create a new event loop for this process
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # Run the coroutine in the new event loop
        loop.run_until_complete(
            process_all_documents(
                scrape_data=institute_scraped_data,
                inst_id=inst_id,
                index_type=index_type,
                index=chunk_index,
            )
        )

        update_institute_generation_status(inst_id, True, "embedding_generated")
        return "Success"

    except Exception as e:
        logging.error(f"Error while generating embeddings: {inst_id}. Error: {e}")
        return f"Failure : {e}"
