# Library
import json
import logging
from dotenv import load_dotenv
import os
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor, as_completed
from elasticsearch import Elasticsearch
from haystack_integrations.document_stores.elasticsearch import (
    ElasticsearchDocumentStore,
)

# Modules
from .ouput_pipelines import query_pipeline_answer_builder
from embedding.utils import get_chunk_index
from .utils import (
    process_institute,
    process_institute_course,
    institutes_for_output_generation,
)
from .degree_output_pipeline import (
    fetch_degree_names_from_es,
    run_query_pipeline_course,
    update_degree_generated_to_es,
)
from crawling.utils import update_institute_generation_status
from utils.validation_check import validation_model
from utils.transformation_run import transformation_run
from .save_results_to_es import (
    check_and_update_document_degree_level_post_transform,
    check_and_update_document_course_level_post_transform,
)
from .course_output_pipeline import (
    fetch_course_names_from_es,
    run_query_pipeline_course_s,
    update_course_generated_to_es,
)

# Initialization
load_dotenv()
es_user = os.getenv("ELASTICSEARCH_USER")
es_password = os.getenv("ELASTICSEARCH_PASSWORD")

es_host = os.environ.get("ELASTIC_SEARCH_HOST")


def query_single_prompt(item):

    prompt = item.prompt
    inst_id = item.institute_id
    sys_prompt = item.sys_prompt
    llm = item.llm
    index_type = item.index
    chunk_index = get_chunk_index(index_type)

    logging.info(f"Querying documents for prompt: {prompt} and inst_id: {inst_id}")
    document_store = ElasticsearchDocumentStore(
        hosts=es_host, index=chunk_index, basic_auth=(es_user, es_password)
    )
    try:
        answer = query_pipeline_answer_builder(
            prompt,
            inst_id,
            answer_data_type=item.answer_format,
            document_store=document_store,
            search_terms=item.search_terms,
            model=llm,
            sys_prompt=sys_prompt,
            response_type=item.response_type,
        )
        answer_data = answer["answer_builder"]["answers"][0].data
        documents = answer["answer_builder"]["answers"][0].documents

        answer_data = json.loads(answer_data)
        chunks_found = []

        for doc in documents:
            chunks_found.append(
                {
                    "chunk": doc.meta["file_url"],
                    "score": doc.score,
                    "content": doc.content,
                    "id": doc.id,
                }
            )

        logging.info(f"Query successful.")
    except Exception as e:
        logging.error(f"Error querying documents: {e}")
        answer_data = f"Error querying documents: {e}"
    return {"chunks_found": chunks_found, "answer_data": answer_data}


def generate_data_points(data):
    institute_ids = data.institute_ids
    result = {}
    index_type = data.index
    chunk_index = get_chunk_index(index_type)
    model = data.llm

    # Use a ThreadPoolExecutor with a fixed number of threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_institute_id = {
            executor.submit(
                process_institute, institute_id, chunk_index, model
            ): institute_id
            for institute_id in institute_ids
        }

        for future in concurrent.futures.as_completed(future_to_institute_id):
            institute_id = future_to_institute_id[future]
            try:
                institute_result = future.result()
                result.update(institute_result)
            except Exception as e:
                result[institute_id] = "Failure"
                print(f"Error processing institute ID {institute_id}: {e}")

    return result


def generate_prompt_output(item):

    institute_ids = institutes_for_output_generation()

    if len(institute_ids) > 0:
        chunk_index = "chunk_by_sentence"
        model = "mixtral22b"
        result = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_institute_id = {
                executor.submit(
                    process_institute, institute_id, chunk_index, model
                ): institute_id
                for institute_id in institute_ids
            }

            for future in concurrent.futures.as_completed(future_to_institute_id):
                institute_id = future_to_institute_id[future]
                try:
                    institute_result = future.result()
                    result.update(institute_result)
                except Exception as e:
                    result[institute_id] = "Failure"
                    print(f"Error processing institute ID {institute_id}: {e}")

        return result
    else:
        return "No Institute found!"


def generate_prompt_output_temporary(item):
    # fmt: off
    institute_ids = [39261, 36347, 3310, 3428, 39266, 816, 33544, 5620, 1061, 729]

    # fmt: on
    if len(institute_ids) > 0:
        chunk_index = "chunk_by_sentence"
        model = "4o-mini"
        result = {}

        # for institute_id in institute_ids:
        #     output = process_institute(institute_id,chunk_index,model)
        #     result.update(output)

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_institute_id = {
                executor.submit(
                    process_institute, institute_id, chunk_index, model
                ): institute_id
                for institute_id in institute_ids
            }

            for future in concurrent.futures.as_completed(future_to_institute_id):
                institute_id = future_to_institute_id[future]
                try:
                    institute_result = future.result()
                    result.update(institute_result)
                except Exception as e:
                    result[institute_id] = "Failure"
                    print(f"Error processing institute ID {institute_id}: {e}")

        return result
    else:
        return "No Institute found!"


def process_institute_course(institute_id, chunk_index, course, model):
    result = {}
    try:
        print(institute_id)
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


def generate_data_points_course(data):
    institute_ids = data.institute_ids
    result = {}
    index_type = data.index
    chunk_index = get_chunk_index(index_type)
    course = data.course
    model = data.llm

    # Use a ThreadPoolExecutor with a fixed number of threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_institute_id = {
            executor.submit(
                process_institute_course, institute_id, chunk_index, course, model
            ): institute_id
            for institute_id in institute_ids
        }

        for future in concurrent.futures.as_completed(future_to_institute_id):
            institute_id = future_to_institute_id[future]
            try:
                institute_result = future.result()
                result.update(institute_result)
            except Exception as e:
                result[institute_id] = "Failure"
                print(f"Error processing institute ID {institute_id}: {e}")

    return result


def process_inst_id(inst_id, es_host, es_user, es_password):
    try:
        es = Elasticsearch(es_host, basic_auth=(es_user, es_password))
        degree_names = fetch_degree_names_from_es(es, inst_id)
        final_result = {}
        for degree_name, degree_id in degree_names:
            try:
                ip_answer_objs = run_query_pipeline_course(
                    inst_id, "chunk_by_sentence", degree_name, degree_id
                )

                result = {}

                for ip_answer_obj in ip_answer_objs:
                    status, ipa_obj, ip_obj, prompt_obj = validation_model(
                        ip_answer_obj, inst_id
                    )
                    if status:
                        output = transformation_run(ipa_obj, ip_obj, prompt_obj)
                        result.update(output)

                final_result.update({degree_id: result})

                update_degree_generated_to_es(es, inst_id, degree_id)
            except Exception as e:
                logging.error(
                    f"Error running query pipeline for degree: inst_id : {inst_id} degree: {degree_name} - {e}"
                )

        check_and_update_document_degree_level_post_transform(
            es, inst_id, "degree_level", final_result, "prompts_result"
        )
    except Exception as e:
        logging.error(
            f"Error processing inst_id {inst_id} in the Degree Output Generation API: {e}"
        )
        return f"Failure: {e}"

    return "Success"


def run_on_all_degrees(data):
    try:
        inst_ids = data.institute_ids

        with ProcessPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(
                    process_inst_id, inst_id, es_host, es_user, es_password
                ): inst_id
                for inst_id in inst_ids
            }

            for future in as_completed(futures):
                inst_id = futures[future]
                try:
                    result = future.result()
                    if result != "Success":
                        logging.error(
                            f"Failed processing inst_id {inst_id} in the Degree Output Generation API: {result}"
                        )
                except Exception as e:
                    logging.error(f"Exception for inst_id {inst_id}: {e}")

        return "Success"
    except Exception as e:
        return f"Failure : {e}"


def generate_prompt_output(item):

    institute_ids = institutes_for_output_generation()

    if len(institute_ids) > 0:
        chunk_index = "chunk_by_sentence"
        model = "4o-mini"
        result = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_institute_id = {
                executor.submit(
                    process_institute, institute_id, chunk_index, model
                ): institute_id
                for institute_id in institute_ids
            }

            for future in concurrent.futures.as_completed(future_to_institute_id):
                institute_id = future_to_institute_id[future]
                try:
                    institute_result = future.result()
                    result.update(institute_result)
                except Exception as e:
                    result[institute_id] = "Failure"
                    print(f"Error processing institute ID {institute_id}: {e}")

        return result
    else:
        return "No Institute found!"


def process_institutes_specializations(inst_id):
    try:
        es = Elasticsearch(es_host, basic_auth=(es_user, es_password))
        course_data = fetch_course_names_from_es(es, inst_id)
        final_result = {}

        for course in course_data:
            course_name = course["course_name"]
            course_id = course["course_id"]
            inst_course_id = course["inst_course_id"]

            try:
                ip_answer_objs = run_query_pipeline_course_s(
                    inst_id, "chunk_by_sentence", course_name, inst_course_id
                )

                result = {}

                for ip_answer_obj in ip_answer_objs:
                    status, ipa_obj, ip_obj, prompt_obj = validation_model(
                        ip_answer_obj, inst_id
                    )
                    if status:
                        output = transformation_run(ipa_obj, ip_obj, prompt_obj)
                        result.update(output)

                final_result.update({inst_course_id: result})

                update_course_generated_to_es(es, inst_id, course_id)
            except Exception as e:
                logging.error(
                    f"Error running query pipeline for course: inst_id : {inst_id} course: {course_name} - {e}"
                )

        check_and_update_document_course_level_post_transform(
            es, inst_id, "specialization_level", final_result, "prompts_result"
        )
        return "Success"
    except Exception as e:
        return f"Failure : {e}"


def run_on_all_courses(data):

    try:
        inst_ids = data.institute_ids

        result = {}

        with concurrent.futures.ProcessPoolExecutor(max_workers=15) as executor:
            future_to_institute_id = {
                executor.submit(
                    process_institutes_specializations, institute_id
                ): institute_id
                for institute_id in inst_ids
            }

            for future in concurrent.futures.as_completed(future_to_institute_id):
                institute_id = future_to_institute_id[future]
                try:
                    institute_result = future.result()
                    result[institute_id] = institute_result
                except Exception as e:
                    result[institute_id] = "Failure"
                    print(f"Error processing institute ID {institute_id}: {e}")

        return result
    except Exception as e:
        return f"Failure : {e}"
