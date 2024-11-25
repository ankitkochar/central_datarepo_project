from .word_replacer import word_transformation
from .elastic import (
    fetch_ipa_obj,
    add_ipav_transformation_entry,
    update_transformation_run_status,
    fetch_ip_obj,
    fetch_prompt_obj,
)
from output_generation.save_results_to_es import (
    check_and_update_document_overview_new,
    check_and_update_document_degree_level_post_transform,
)
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
import os
import json
from time import sleep

load_dotenv()
es_host = os.getenv("ELASTIC_SEARCH_HOST")
prompts_result_index = os.environ.get("PROMPTS_RESULT")
es_user = os.getenv("ELASTICSEARCH_USER")
es_password = os.getenv("ELASTICSEARCH_PASSWORD")
es = Elasticsearch(es_host, basic_auth=(es_user, es_password))


def transformation_run(ipa_validation_obj,ip_obj,prompt_obj):
    ipav_answer = ipa_validation_obj["validation_answer"]
    ipa_id = ipa_validation_obj["ipa_id"]
    ip_id = ipa_validation_obj["ip_id"]
    ipav_id = ipa_validation_obj["_id"]
    # ip_obj = fetch_ip_obj(ip_id)
    # ipa_obj = fetch_ipa_obj(ipa_id)

    p_id = ip_obj["prompt_id"]
    institute_id = ip_obj["institute_id"]

    # prompt_obj = fetch_prompt_obj(p_id)
    # ipa_obj = fetch_ipa_obj(ipa_id)
    # print(ip_obj)
    answer_format = prompt_obj["output_format"]
    category = prompt_obj["category"]
    tag = prompt_obj["tags"]
    degree_specific = prompt_obj["degree_specific"]
    specialization_specific = prompt_obj["specialization_specific"]
    if not degree_specific and not specialization_specific:
        if type(ipav_answer) == str:
            transformed_answer = word_transformation(ipav_answer, answer_format)
        else:
            transformed_answer = word_transformation(
                json.dumps(ipav_answer), answer_format
            )
        add_ipav_transformation_entry(ip_id, ipa_id, ipav_id, transformed_answer)
        update_transformation_run_status(ipav_id)
        final_output = {
            tag: {"answer": transformed_answer, "data_type": answer_format, "tags": tag, "category": category}
        }

        return final_output
    elif degree_specific and not specialization_specific:
        for k, v in json.loads(ipav_answer).items():
            cld_degree_answer = v
            final_output = {
                tag: {
                    "answer": cld_degree_answer[tag],
                    "data_type": answer_format,
                    "tags": tag,
                    "category" : category
                }
            }
        add_ipav_transformation_entry(ip_id, ipa_id, ipav_id, ipav_answer)
        update_transformation_run_status(ipav_id)

        return final_output
    elif not degree_specific and specialization_specific:
        for k, v in json.loads(ipav_answer).items():
            cld_degree_answer = v
            final_output = {
                tag: {
                    "answer": cld_degree_answer[tag],
                    "data_type": answer_format,
                    "tags": tag,
                    "category" : category
                }
            }
        add_ipav_transformation_entry(ip_id, ipa_id, ipav_id, ipav_answer)
        update_transformation_run_status(ipav_id)

        return final_output
