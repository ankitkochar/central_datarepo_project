# Library
import asyncio
from typing import Dict, Any
from elasticsearch import Elasticsearch
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import os

# Modules
from dotenv import load_dotenv
from constants import es_institute_index_name
from .crawl_website import WebScraper

# Initialization
load_dotenv()

es_host = os.getenv("ELASTIC_SEARCH_HOST")
auto_run_index = os.environ.get("AUTO_RUN_INDEX")
es_user = os.getenv("ELASTICSEARCH_USER")
es_password = os.getenv("ELASTICSEARCH_PASSWORD")

es = Elasticsearch(es_host, basic_auth=(es_user, es_password))


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


def check_already_downloaded(inst_id):

    query = {"query": {"match": {"cld_id": int(inst_id)}}}
    with get_es_client() as esg:

        result = esg.search(index=es_institute_index_name, body=query)
    does_entry_exists = result["hits"]["total"]["value"] > 0

    if not does_entry_exists:
        return False
    else:
        result_dict = result["hits"]["hits"][0]["_source"]
        if "downloaded" in result_dict and result_dict["downloaded"]:
            return True
        else:
            return False


def get_name_by_cld_id(cld_id):
    query = {"query": {"term": {"cld_id": cld_id}}}

    try:
        with get_es_client() as esg:
            response = esg.search(index=es_institute_index_name, body=query)

        if response["hits"]["total"]["value"] > 0:
            name = response["hits"]["hits"][0]["_source"]["name"]
            return name
        else:
            return None
    except Exception as e:
        print(f"Error querying Elasticsearch: {e}")
        return None


def fetch_institute_for_scrapping():
    institutes = []
    query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"downloaded": False}},
                ]
            }
        },
        "size": 80,
    }
    with get_es_client() as esg:
        response = esg.search(index=es_institute_index_name, body=query)

    hits = response["hits"]["hits"]

    for hit in hits:
        institute = hit["_source"]
        institutes.append(institute["cld_id"])

    return institutes


def update_scrape_data_status(inst_id, chunk_index):
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
    es.update_by_query(index=chunk_index, body=query)


def update_institute_generation_status(inst_id, status, field_name):

    # Define search query
    search_query = {"query": {"match": {"cld_id": int(inst_id)}}}

    # Search for documents matching the query
    with get_es_client() as esg:
        search_result = esg.search(index="institute", body=search_query)

    # Extract document IDs
    document_ids = [hit["_id"] for hit in search_result["hits"]["hits"]]

    # Update field for each document
    for doc_id in document_ids:
        # Update field
        update_body = {"doc": {field_name: status, "updated_at": datetime.now()}}
        # Perform update
        es.update(index="institute", id=doc_id, body=update_body)


def create_institute_entry(inst_id, institute_data):
    query = {"query": {"match": {"cld_id": int(inst_id)}}}
    with get_es_client() as esg:

        result = esg.search(index=es_institute_index_name, body=query)
    does_entry_exists = result["hits"]["total"]["value"] > 0

    if not does_entry_exists:
        doc = {
            "name": institute_data["institute_name"],
            "url": institute_data["institute_url"],
            "cld_id": int(inst_id),
            "embedding_generated": False,
            "prompt_output_generated": False,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        }
        es.index(index=es_institute_index_name, body=doc)
        print("New document created.")
        return False
    else:
        result_dict = result["hits"]["hits"][0]["_source"]
        if result_dict["embedding_generated"]:
            return True
        print(f"Document with cld_id: {inst_id} already exists.")


def filter_url(url):
    if "http://" in url or "https://" in url:
        return url
    else:
        return f"https://{url}"


def fetch_institute_url(institute_id):
    query = {"query": {"match": {"cld_id": int(institute_id)}}}
    with get_es_client() as esg:
        result = esg.search(index=es_institute_index_name, body=query)

    return result["hits"]["hits"][0]["_source"]["url"]


def scrape_institute_data(inst_id, input_url=None, enable_javascript=True):
    try:
        institute_url = input_url or fetch_institute_url(inst_id)
        if institute_url:
            scraper = WebScraper(
                start_url=institute_url,
                inst_id=inst_id,
                institute_name=get_name_by_cld_id(inst_id),
            )
            scraped_data, json_data = scraper.run(max_pages=200)

            update_institute_generation_status(inst_id, True, "downloaded")
            return "Success"
        else:
            return "Got Empty Website URL from DB."
    except Exception as e:
        print(f"Error while downloading and pushing documents to S3 for {inst_id}: {e}")
        return f"Failure : {e}"


def run_institute(inst_id):
    try:
        print(f"Processing: {inst_id}")
        update_scrape_data_status(inst_id, "scraper_info")
        result = scrape_institute_data(inst_id)
        print(f"Completed: {inst_id}, Result: {result}")
    except Exception as e:
        print(f"Error processing {inst_id}: {e}")
