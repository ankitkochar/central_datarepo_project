# Library
import asyncio
import os
from typing import Dict, Any
from elasticsearch import Elasticsearch
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# Modules
from .ouput_pipelines import run_query_pipeline
from crawling.utils import update_institute_generation_status
from .degree_output_pipeline import run_query_pipeline_course
from utils.validation_check import validation_model
from utils.transformation_run import transformation_run
from .save_results_to_es import add_in_prompt_result


# Initialization
load_dotenv()
es_user = os.getenv("ELASTICSEARCH_USER")
es_password = os.getenv("ELASTICSEARCH_PASSWORD")
es_host = os.getenv("ELASTIC_SEARCH_HOST")
prompt_result_index = os.getenv('PROMPTS_RESULT')
es = Elasticsearch(es_host, basic_auth=(es_user, es_password))

chunk_index = os.environ.get("CHUNK_INDEX_SENTENCE")


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


def find_url_crawed(inst_id):
    query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"status": True}},
                    {"match": {"institute_id": inst_id}},
                    {"match": {"filetype": "html"}},
                ]
            }
        },
        "size": 0,
        "aggs": {"unique_file_urls": {"cardinality": {"field": "file_url"}}},
    }
    with get_es_client() as esg:
        response = esg.search(index=chunk_index, body=query)
    unique_file_urls_count = response["aggregations"]["unique_file_urls"]["value"]

    return unique_file_urls_count


def institutes_for_output_generation():
    try:
        institutes = []
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"embedding_generated": True}},
                        {"match": {"prompt_output_generated": False}},
                    ]
                }
            },
            "size": 3000,
        }

        # Execute the search query
        with get_es_client() as esg:
            response = esg.search(index="institute", body=query)

        # Process the response to get the hits
        hits = response["hits"]["hits"]

        # Print the results
        for hit in hits:
            institute = hit["_source"]
            # unique_file_urls_count = find_url_crawed(institute["cld_id"])

            # if unique_file_urls_count > 4:
            institutes.append(institute["cld_id"])

        return institutes
    except Exception as e:
        errMsg = str(e)
        print("Error while fetching all institutes:", errMsg)


def process_institute(institute_id, chunk_index, model):
    result = {}
    try:
        ip_answer_objs = run_query_pipeline(
            institute_id, index=chunk_index, model=model
        )

        final_result = {}

        for ip_answer_obj in ip_answer_objs:
            status, ipa_obj, ip_obj,prompt_obj = validation_model(ip_answer_obj, institute_id)
            if status:
                output = transformation_run(ipa_obj,ip_obj,prompt_obj)
                final_result.update(output)

        add_in_prompt_result(es,institute_id,prompt_result_index,final_result,'overview')

        update_institute_generation_status(
            institute_id, True, "prompt_output_generated"
        )
        result[institute_id] = "Success"
    except Exception as e:
        result[institute_id] = str(e)
        print("Error", e)
    return result


def process_institute_course(institute_id, chunk_index, course, model):
    result = {}
    try:
        run_query_pipeline_course(
            institute_id, index=chunk_index, course=course, model=model
        )
        update_institute_generation_status(
            institute_id, True, "prompt_output_generated"
        )
        result[institute_id] = "Success"
    except Exception as e:
        result[institute_id] = "Failure"
        print("Error", e)
    return result