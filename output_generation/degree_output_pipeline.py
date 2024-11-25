from dotenv import load_dotenv
from pathlib import Path
import os
import pandas as pd
from haystack_integrations.document_stores.elasticsearch import (
    ElasticsearchDocumentStore,
)
import warnings
import logging
from .ouput_pipelines import (
    query_pipeline_answer_builder,
    save_results_to_sheets,
)
from output_generation.save_results_to_es import (
    check_and_update_document_degree_initial_population,
)
from elasticsearch import Elasticsearch
from output_generation.ouput_pipelines import add_answer_in_ip_answer
import json

load_dotenv()  # Load the .env file
es_user = os.getenv("ELASTICSEARCH_USER")
es_password = os.getenv("ELASTICSEARCH_PASSWORD")

log_files_folder = os.environ.get("LOG_FILES_FOLDER")
open_ai_key = os.environ.get("OPENAI_API_KEY")
fireworks_api_key = os.environ.get("FIREWORKS_API_KEY")
es_host = os.environ.get("ELASTIC_SEARCH_HOST")

fireworks_api_url = "https://api.fireworks.ai/inference/v1"
drive_folder_id_external = os.environ.get("DRIVE_FOLDER_ID_EXTERNAL")
drive_folder_id_internal = os.environ.get("DRIVE_FOLDER_ID_INTERNAL")
prompts_institute_index = os.environ.get("PROMPTS_INSTITUTE_INDEX")
prompts_index = os.environ.get("PROMPTS")
prompts_run_log_index = os.environ.get("PROMPTS_RUN_LOGS")
prompts_result_index = os.environ.get("PROMPTS_RESULT")

try:
    log_file_path = os.path.join(log_files_folder, "query_multiple.log")
    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format="%(asctime)s - %(module)s - %(funcName)s - %(levelname)s - %(message)s",
    )
    logging.info("Logging started successfully.")
except Exception as e:
    print(f"Failed to set up logging: {e}")


def fetch_degree_prompts_from_es(es, index_name):

    try:
        query = {
            "bool": {
                "filter": [
                    {"term": {"status": True}},
                    {"term": {"degree_specific": True}},
                    {
                        "term": {
                            "specialization_specific": False,
                        }
                    },
                ]
            }
        }
        response = es.search(index=index_name, query=query, size=200)
        return response["hits"]["hits"]  # This returns a list of hits
    except Exception as e:
        logging.error(f"Error fetching prompts from Elasticsearch: {e}")
        return []


def fetch_degree_names_from_es(es, inst_id):
    # fetch array from field cld_degree_ids in index inst_degrees where field inst_id == inst_id
    # for every integer in the array, fetch the field "name" of the degree from index degree where field cld_degree_id = value in array
    try:
        query_inst_degrees = {
            "query": {"term": {"inst_id": inst_id}},
            "_source": ["cld_degree_ids"],
        }

        response_inst_degrees = es.search(index="inst_degrees", body=query_inst_degrees)
        degree_ids = []
        if response_inst_degrees["hits"]["hits"]:
            degree_ids = response_inst_degrees["hits"]["hits"][0]["_source"].get(
                "cld_degree_ids", []
            )
        else:
            print("No institution found matching the criteria.")

        degree_names = []
        for degree_id in degree_ids:
            query_degree = {
                "query": {"term": {"cld_degree_id": degree_id}},
                "_source": ["name"],  # Only fetch the 'name' field
            }

            response_degree = es.search(index="degree", body=query_degree)
            if response_degree["hits"]["hits"]:
                degree_name = response_degree["hits"]["hits"][0]["_source"].get(
                    "name", "No name available"
                )
                degree_names.append((degree_name, degree_id))
            else:
                degree_names.append(f"No degree found for ID {degree_id}")
        return degree_names
    except Exception as e:
        logging.error(f"Error fetching degrees from Elasticsearch: {e}")
        return []


def update_degree_generated_to_es(es, inst_id, degree_id):
    try:
        query = {
            "query": {"term": {"inst_id": inst_id}},
            "script": {
                "source": "if (ctx._source.cld_degree_ids_generated == null) { ctx._source.cld_degree_ids_generated = [params.degree_id] } else { ctx._source.cld_degree_ids_generated.add(params.degree_id) }",
                "params": {"degree_id": degree_id},
            },
        }
        response = es.update_by_query(index="inst_degrees", body=query, refresh=True)
    except Exception as e:
        print(f"Error updating document: {e}")
        return []


def fetch_degrees_run(es, inst_id):
    try:
        query = {
            "size": 1,
            "query": {"term": {"inst_id": inst_id}},
            "_source": ["cld_degree_ids_generated"],
        }

        response = es.search(index="inst_degrees", body=query)
        degree_ids = set()
        if response["hits"]["hits"]:
            degree_ids = set(
                response["hits"]["hits"][0]["_source"].get(
                    "cld_degree_ids_generated", []
                )
            )
        else:
            print(response)
            print("No institution found matching the criteria.")
        return degree_ids
    except Exception as e:
        logging.error(f"Error fetching degrees from Elasticsearch: {e}")
        return set()


def run_on_all_degrees(inst_id):
    es = Elasticsearch(es_host, basic_auth=(es_user, es_password))
    degree_names = fetch_degree_names_from_es(es, inst_id)
    # degrees_run = fetch_degrees_run(es, inst_id)
    degrees_run = set()
    for degree_name, degree_id in degree_names:
        if degree_id not in degrees_run:
            try:
                run_query_pipeline_course(
                    inst_id, "chunk_by_sentence", degree_name, degree_id
                )
                update_degree_generated_to_es(es, inst_id, degree_id)
            except Exception as e:
                logging.error(
                    f"Error running query pipeline for degree: inst_id : {inst_id} degree: {degree_name} - {e}"
                )


def add_prompts_institute_entry_course(es, inst_id, prompt_id):
    try:
        search_result = es.search(
            index=prompts_institute_index,
            body={
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"prompt_id": prompt_id}},
                            {"term": {"institute_id": inst_id}},
                        ]
                    }
                }
            },
        )

        if search_result["hits"]["total"]["value"] > 0:
            doc_id = search_result["hits"]["hits"][0]["_id"]
            update_response = es.update(
                index=prompts_institute_index,
                id=doc_id,
                body={"doc": {"run_status": True, "status": True}},
            )
            return doc_id
        else:
            create_response = es.index(
                index=prompts_institute_index,
                body={
                    "institute_id": inst_id,
                    "prompt_id": prompt_id,
                    "run_status": True,
                    "status": True,
                },
            )
            return create_response["_id"]

    except Exception as e:
        logging.error(f"Failed to ADD or UPDATE Prompts Institute Entry, {e}")
        return None


def run_query_pipeline_course(
    inst_id, index, course, degree_cld_id, model="4o-mini"
):
    """
    Initiates the process of running a query pipeline for multiple prompts, saving the results.

    Parameters:
    - inst_id (str): The identifier for the institution for which the pipeline is run.
    - model (str): The model to be used for answer generation. Values are 3.5t mixtral7b mixtral22b llama. number of chunks for llama is 7
    - index (str): The name of the Elasticsearch index to be used for document retrieval.
    This function reads prompts from an Excel file, processes each using the query pipeline, and saves the results.
    """
    document_store = ElasticsearchDocumentStore(
        hosts=es_host, index=index, basic_auth=(es_user, es_password)
    )
    warnings.filterwarnings("ignore")

    es = Elasticsearch(es_host, basic_auth=(es_user, es_password))
    prompts = fetch_degree_prompts_from_es(es, "prompts")
    new_prompt_status = False

    ip_answer_objs = []

    results = {}
    logging.info(
        f"Running course level query pipeline for institution ID: '{inst_id}' course '{course}' using model: '{model}'"
    )
    for hit in prompts:
        prompt_id = hit["_id"]
        source = hit["_source"]
        try:
            prompt = source["prompt"].replace("<course name>", course)
            new_prompt_status = True

            data_type = source["output_format"]
            tags = source["tags"]
            category = source["category"]
            num_chunks = source["num_chunks"]
            if "search_terms" in source:
                search_terms = source["search_terms"]
            else:
                search_terms = None
            if "response_type" in source:
                response_type = source["response_type"]
            else:
                response_type = "text"
            search_terms = search_terms + " " + course
            results[prompt] = query_pipeline_answer_builder(
                prompt,
                inst_id,
                data_type,
                document_store,
                search_terms=search_terms,
                num_chunks=num_chunks,
                model=model,
                response_type=response_type,
            )
            results[prompt].update(
                {"data_type": data_type, "tags": tags, "category": category}
            )
            answer_obj = (results[prompt]
                        .get("answer_builder", {})
                        .get("answers", [])[0]
                        .data)

            answer_obj = json.loads(answer_obj)
            actual_answer = answer_obj['answer']
            sources = answer_obj['sources']

            answer = json.dumps(
                {
                    degree_cld_id: {
                        tags: actual_answer
                    }
                }
            )
            documents = (
                results[prompt]
                .get("answer_builder", {})
                .get("answers", [])[0]
                .documents
            )
            original_links = []
            source_links = []

            for doc in documents:
                if str(doc.id) in sources:
                    source_links.append(doc.meta["file_url"])
                original_links.append(doc.meta["file_url"])

            ip_id = add_prompts_institute_entry_course(es, inst_id, prompt_id)
            ip_answer_obj = add_answer_in_ip_answer(es, ip_id, answer, data_type, original_links, tags,source_links)
            ip_answer_objs.append(ip_answer_obj)
        
        except Exception as e:
            logging.error(f"Error while running the prompt: {prompt} - {e}")
            results[prompt] = {
                "data_type": data_type,
                "tags": tags,
                "category": category,
            }

    logging.info(
        f"Completed processing. Starting to save results for institution ID: '{inst_id}'"
    )

    return ip_answer_objs
