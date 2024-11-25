from elasticsearch import Elasticsearch
import os
from dotenv import load_dotenv
import logging
from typing import Dict, Any
from datetime import datetime

load_dotenv()

log_files_folder = os.environ.get("LOG_FILES_FOLDER")
es_host = os.getenv("ELASTIC_SEARCH_HOST")
es_user = os.getenv("ELASTICSEARCH_USER")
es_password = os.getenv("ELASTICSEARCH_PASSWORD")
es = Elasticsearch(es_host, basic_auth=(es_user, es_password))

chunk_index = os.environ.get("CHUNK_INDEX_SENTENCE")
prompts_institute_index = os.environ.get("PROMPTS_INSTITUTE_INDEX")
prompts_index = os.environ.get("PROMPTS")
prompts_run_log_index = os.environ.get("PROMPTS_RUN_LOGS")
prompts_result_index = os.environ.get("PROMPTS_RESULT")


def get_es_client(custom_settings: Dict[str, Any] = None) -> Elasticsearch:
    default_settings = {
        "hosts": [es_host],
        "basic_auth": (es_user, es_password),
    }

    if custom_settings:
        default_settings.update(custom_settings)

    return Elasticsearch(**default_settings)


def fetch_institute_for_embedding():
    institutes = []
    query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"embedding_generated": False}},
                    {"match": {"downloaded": True}},
                ]
            }
        },
        "size": 80,
    }
    with get_es_client() as esg:
        response = esg.search(index="institute", body=query)

    hits = response["hits"]["hits"]

    for hit in hits:
        institute = hit["_source"]
        institutes.append(institute["cld_id"])

    return institutes


def fetch_ip_answer():
    ip_answer_objs = []
    query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"status": True}},
                    {"match": {"validation_run_status": False}},
                ]
            }
        },
        "size": 500,
    }
    with get_es_client() as esg:
        response = esg.search(index="ip_answer", body=query)

    hits = response["hits"]["hits"]

    for hit in hits:
        ip_answer_obj = hit["_source"]
        ip_answer_obj["_id"] = hit["_id"]
        ip_answer_objs.append(ip_answer_obj)

    return ip_answer_objs


def fetch_ip_obj(ip_id, institute_id=""):
    es.indices.refresh(index=prompts_institute_index)
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"status": True}},
                    {"term": {"_id": ip_id}},
                ]
            }
        },
        "size": 1,
    }
    response = es.search(index=prompts_institute_index, body=query)
    ip_obj = response["hits"]["hits"][0]["_source"]
    return ip_obj


def fetch_prompt_obj(prompt_id):
    query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"status": True}},
                    {"match": {"_id": prompt_id}},
                ]
            }
        },
        "size": 1,
    }
    
    es.indices.refresh(index="prompts")
    response = es.search(index="prompts", body=query)
    prompt_obj = response["hits"]["hits"][0]["_source"]
    return prompt_obj


def add_ipa_validation_entry(ip_id, ipa_id, status, answer, comment):
    try:
        create_response = es.index(
            index="ipa_validation",
            body={
                "ip_id": ip_id,
                "ipa_id": ipa_id,
                "validation_status": status,
                "validation_answer": answer,
                "comment": comment,
                "status": True,
                "transformation_run_status": False,
            },
        )
        return {
            "_id": create_response["_id"],
            "ip_id": ip_id,
            "ipa_id": ipa_id,
            "validation_answer": answer,
        }

    except Exception as e:
        logging.error(f"Failed to ADD ipav_validation from Index, {e}")


def update_validation_run_status(ipa_id):
    query = {
        "script": {
            "source": "ctx._source.validation_run_status = params.new_status",
            "lang": "painless",
            "params": {"new_status": True},
        },
        "query": {"term": {"_id": ipa_id}},
    }
    response = es.update_by_query(index="ip_answer", body=query)
    return response


def fetch_ipa_validation():
    ipa_validation_objs = []
    query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"validation_status": True}},
                    {"match": {"transformation_run_status": False}},
                    {"match": {"status": True}},
                ]
            }
        },
        "size": 500,
    }
    with get_es_client() as esg:
        response = esg.search(index="ipa_validation", body=query)

    hits = response["hits"]["hits"]

    for hit in hits:
        ipa_validation_obj = hit["_source"]
        ipa_validation_obj["_id"] = hit["_id"]
        ipa_validation_objs.append(ipa_validation_obj)

    return ipa_validation_objs


def fetch_ipa_obj(ipa_id):
    query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"status": True}},
                    {"match": {"_id": ipa_id}},
                ]
            }
        },
        "size": 1,
    }
    with get_es_client() as esg:
        response = esg.search(index="ip_answer", body=query)
    ipa_obj = response["hits"]["hits"][0]["_source"]
    return ipa_obj


def add_ipav_transformation_entry(ip_id, ipa_id, ipav_id, answer):
    try:
        create_response = es.index(
            index="ipav_transformation",
            body={
                "ip_id": ip_id,
                "ipa_id": ipa_id,
                "ipav_id": ipav_id,
                "transformed_answer": answer,
                "status": True,
            },
        )
        return create_response["_id"]

    except Exception as e:
        logging.error(f"Failed to ADD ipav_transfromation from Index, {e}")


def update_transformation_run_status(ipav_id):
    query = {
        "script": {
            "source": "ctx._source.transformation_run_status = params.new_status",
            "lang": "painless",
            "params": {"new_status": True},
        },
        "query": {"term": {"_id": ipav_id}},
    }
    response = es.update_by_query(index="ipa_validation", body=query)
    return response


def update_prompts_institute(institute_id):
    try:
        query = {
            "query": {
                "term": {
                    "institute_id": institute_id
                }
            }
        }

        update_body = {
            "script": {
                "source": "ctx._source.status = params.status; ctx._source.updated_at = params.updated_at",
                "lang": "painless",
                "params": {
                    "status": False,
                    "updated_at": datetime.now()
                }
            },
            "query": query["query"]
        }

        response = es.update_by_query(index="prompts_institute", body=update_body)
        logging.info(f"Updated prompts_institute for institute Id {institute_id}. Total updated: {response['updated']}")

        return response

    except Exception as e:
        logging.error(f"Error in updating prompts_institute for institute Id {institute_id}: {e}")
        return None
    
def get_user_details(email):
    query = {
            "query": {
                "term": {
                    "email": email
                }
            },
            "size" : 1
        }
    
    res = es.search(index='users',body=query)
    user_details = res["hits"]["hits"][0]["_source"]
    
    return user_details
