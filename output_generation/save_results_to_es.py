import logging
import json
import os
from dotenv import load_dotenv
from time import sleep
from elasticsearch.exceptions import ConnectionTimeout

load_dotenv()
prompts_result_index = os.environ.get("PROMPTS_RESULT")


def check_and_update_document_overview(
    es, cld_id, update_field, results, index_name=prompts_result_index
):
    try:
        query = {"query": {"term": {"cld_id": cld_id}}}
        response = es.search(index=index_name, body=query, size=1)

        all_records = {}

        for question, answer in results.items():
            if "answer_builder" in answer:
                answer_data = answer["answer_builder"]["answers"][0].data
                documents = answer["answer_builder"]["answers"][0].documents
                data_type = answer["data_type"]
                tags = answer["tags"]
                category = answer["category"]
                original_links = []
                for doc in documents:
                    original_links.append(doc.meta["file_url"])
                row = {
                    "query": question,
                    "answer": answer_data,
                    "data_type": data_type,
                    "category": category,
                    "tags": tags,
                    "original_links": original_links,
                }
                all_records[tags] = row

        if response["hits"]["total"]["value"] > 0:
            document_id = response["hits"]["hits"][0]["_id"]
            prev_response = response["hits"]["hits"][0]["_source"]
            if "overview" in prev_response:
                result_json = prev_response["overview"]
            else:
                result_json = {}

            if isinstance(result_json, str):
                result_json = json.loads(result_json)

            result_json.update(all_records)

            update_body = {"doc": {update_field: json.dumps(result_json)}}
            update_response = es.update(
                index=index_name, id=document_id, body=update_body
            )
            return f"Document updated in Prompt Result: {update_response}"
        else:

            document_body = {"cld_id": cld_id, update_field: json.dumps(all_records)}
            create_response = es.index(index=index_name, body=document_body)
            return f"Document created in Prompt Result:"
    except Exception as e:
        logging.error(f"Error in updating or creating document: {e}")
        return str(e)


def check_and_update_document_degree_level_post_transform(
    es, cld_id, update_field, results, index_name=prompts_result_index
):
    try:
        query = {"query": {"term": {"cld_id": cld_id}}}
        response = es.search(index=index_name, body=query, size=1)

        if response["hits"]["total"]["value"] > 0:
            document_id = response["hits"]["hits"][0]["_id"]
            prev_response = response["hits"]["hits"][0]["_source"]
            if "degree_level" in prev_response:
                result_json = prev_response["degree_level"]
            else:
                result_json = {}

            if isinstance(result_json, str):
                result_json = json.loads(result_json)

            # FORMAT OF RESULTS
            #             results = {
            #                 cld_dedgree_id: {"tags" :{
            #                     "answer": cld_degree_answer,
            #                     "data_type": answer_format,
            #                     "tags": tag,
            #                 }}
            #             }
            if isinstance(results, str):
                results = json.loads(results)

            cld_degree_id = list(results.keys())

            if result_json == None:
                result_json = {}

            for id in cld_degree_id:
                result_json[str(id)] = results[id]

            update_body = {"doc": {update_field: json.dumps(result_json)}}
            update_response = es.update(
                index=index_name, id=document_id, body=update_body
            )
            print(f"Document updated in Prompt Result: ")

            return f"Document updated in Prompt Result: {update_response}"

        else:

            document_body = {"cld_id": cld_id, update_field: json.dumps(results)}
            create_response = es.index(index=index_name, body=document_body)
            print(f"Document created in Prompt Result: ")

            return f"Document created in Prompt Result: "
    except Exception as e:
        logging.error(f"Error in updating or creating document: {e}")
        print(f"Error in updating or creating document: {e}")
        return str(e)


def check_and_update_document_overview_new(
    es, cld_id, update_field, results, index_name=prompts_result_index
):
    try:
        query = {"query": {"term": {"cld_id": cld_id}}}
        response = es.search(index=index_name, body=query, size=1)

        if response["hits"]["total"]["value"] > 0:
            document_id = response["hits"]["hits"][0]["_id"]
            prev_response = response["hits"]["hits"][0]["_source"]
            if "overview" in prev_response:
                result_json = prev_response["overview"]
            else:
                result_json = {}

            if isinstance(result_json, str):
                result_json = json.loads(result_json)

            result_json.update(results)

            update_body = {"doc": {update_field: json.dumps(result_json)}}
            update_response = es.update(
                index=index_name, id=document_id, body=update_body
            )
            return f"Document updated in Prompt Result: {update_response}"
        else:

            document_body = {"cld_id": cld_id, update_field: json.dumps(results)}
            create_response = es.index(index=index_name, body=document_body)
            return f"Document created in Prompt Result"
    except Exception as e:
        logging.error(f"Error in updating or creating document")
        return str(e)


def check_and_update_document_overview_initial_population(
    es, cld_id, update_field, results, index_name=prompts_result_index
):
    try:
        query = {"query": {"term": {"cld_id": cld_id}}}
        response = es.search(index=index_name, body=query, size=1)

        all_records = {}

        for question, answer in results.items():
            if "answer_builder" in answer:
                answer_data = answer["answer_builder"]["answers"][0].data
                documents = answer["answer_builder"]["answers"][0].documents
                data_type = answer["data_type"]
                tags = answer["tags"]
                category = answer["category"]
                original_links = []
                for doc in documents:
                    original_links.append(doc.meta["file_url"])
                row = {
                    "data_type": data_type,
                    "tags": tags,
                    "category": category,
                }
                all_records[tags] = row

        if response["hits"]["total"]["value"] > 0:
            document_id = response["hits"]["hits"][0]["_id"]
            prev_response = response["hits"]["hits"][0]["_source"]
            if "overview" in prev_response:
                result_json = prev_response["overview"]
            else:
                result_json = {}

            if isinstance(result_json, str):
                result_json = json.loads(result_json)

            result_json.update(all_records)

            update_body = {"doc": {update_field: json.dumps(result_json)}}
            update_response = es.update(
                index=index_name, id=document_id, body=update_body
            )
            return f"Document updated in Prompt Result: {update_response}"
        else:

            document_body = {"cld_id": cld_id, update_field: json.dumps(all_records)}
            create_response = es.index(index=index_name, body=document_body)
            return f"Document created in Prompt Result: "
    except Exception as e:
        logging.error(f"Error in updating or creating document: {e}")
        return str(e)


def check_and_update_document_degree_initial_population(
    es, cld_id, update_field, results, degree_id, index_name="prompts_result"
):
    try:
        query = {"query": {"term": {"cld_id": cld_id}}}
        response = es.search(index=index_name, body=query, size=1)

        all_records = {degree_id: {}}

        for question, answer in results.items():
            if "answer_builder" in answer:
                data_type = answer["data_type"]
                tags = answer["tags"]
                category = answer["category"]
                row = {
                    "data_type": data_type,
                    "tags": tags,
                    "category": category,
                }
                all_records[degree_id][tags] = row
        if response["hits"]["total"]["value"] > 0:
            document_id = response["hits"]["hits"][0]["_id"]
            prev_response = response["hits"]["hits"][0]["_source"]
            if "degree_level" in prev_response:
                result_json = prev_response["degree_level"]
            else:
                result_json = {}
            if isinstance(result_json, str):
                result_json = json.loads(result_json)

            result_json.update(all_records)

            update_body = {"doc": {update_field: json.dumps(result_json)}}
            update_response = es.update(
                index=index_name, id=document_id, body=update_body
            )
            return f"Document updated in Prompt Result: {update_response}"
        else:

            document_body = {"cld_id": cld_id, update_field: json.dumps(all_records)}
            create_response = es.index(index=index_name, body=document_body)
            return f"Document created in Prompt Result: "
    except Exception as e:
        logging.error(f"Error in updating or creating document: {e}")
        return str(e)


def check_and_update_document_degree(
    es, cld_id, update_field, results, degree_id, index_name="prompts_result"
):
    try:
        query = {"query": {"term": {"cld_id": cld_id}}}
        response = es.search(index=index_name, body=query, size=1)

        all_records = {}

        for question, answer in results.items():
            if "answer_builder" in answer:
                answer_data = answer["answer_builder"]["answers"][0].data
                documents = answer["answer_builder"]["answers"][0].documents
                data_type = answer["data_type"]
                tags = answer["tags"]
                category = answer["category"]

                # Initialize lists to store details of each document
                # contents = []
                # source_ids = []
                # file_paths = []
                # document_ids = []
                # scores = []
                # s3_links = []
                original_links = []
                # Collect details for each document associated with the current answer
                for doc in documents:
                    # contents.append(doc.content)
                    #                     source_ids.append(doc.meta["source_id"])
                    # file_paths.append(doc.meta["file_path"])
                    # document_ids.append(doc.id)
                    # scores.append(doc.score)
                    # s3_links.append(doc.meta["s3_url"])
                    original_links.append(doc.meta["file_url"])
                row = {
                    "query": question,
                    "answer": answer_data,
                    "data_type": data_type,
                    "category": category,
                    "tags": tags,
                    # "s3_links": s3_links,
                    "original_links": original_links,
                    # "scores": scores,
                    # "contents": " ".join(contents)[:49990],
                    # "source_ids": source_ids,
                    # # "file_paths": file_paths,
                    # "document_ids": document_ids,
                }
                all_records[tags] = row

        if response["hits"]["total"]["value"] > 0:
            document_id = response["hits"]["hits"][0]["_id"]
            degree_level_data = response["hits"]["hits"][0]["_source"].get(
                "degree_level", "{}"
            )
            try:

                if isinstance(degree_level_data, str):
                    current_degree_level = json.loads(degree_level_data)
                else:
                    current_degree_level = degree_level_data
            except (json.JSONDecodeError, TypeError):

                current_degree_level = {}
            current_degree_level[degree_id] = all_records

            update_body = {"doc": {update_field: json.dumps(current_degree_level)}}
            update_response = es.update(
                index=index_name, id=document_id, body=update_body
            )
            return f"Document updated: {update_response}"
        else:
            # Document does not exist, so create a new one
            document_body = {
                "cld_id": cld_id,
                update_field: json.dumps({degree_id: all_records}),
            }
            create_response = es.index(index=index_name, body=document_body)
            return f"Document created"
    except Exception as e:
        logging.error(f"Error in updating or creating document: {e}")
        return str(e)


def add_in_prompt_result(es,cld_id,index_name,results,update_field):

    try:
        query = {"query": {"term": {"cld_id": cld_id}}}
        response = es.search(index=index_name, body=query, size=1)

        if response["hits"]["total"]["value"] > 0:
            document_id = response["hits"]["hits"][0]["_id"]
            prev_response = response["hits"]["hits"][0]["_source"]
            if "overview" in prev_response:
                result_json = prev_response["overview"]
            else:
                result_json = {}

            if isinstance(result_json, str):
                result_json = json.loads(result_json)

            result_json.update(results)

            update_body = {"doc": {update_field: json.dumps(result_json)}}
            update_response = es.update(
                index=index_name, id=document_id, body=update_body
            )
            return f"Document updated in Prompt Result: {update_response}"
        else:

            document_body = {"cld_id": cld_id, update_field: json.dumps(results)}
            create_response = es.index(index=index_name, body=document_body)
            return f"Document created in Prompt Result"
    except Exception as e:
        logging.error(f"Error in updating or creating document")
        return str(e)
    

def check_and_update_document_course_level_post_transform(es, cld_id, update_field, results, index_name=prompts_result_index):
    try:
        query = {"query": {"term": {"cld_id": cld_id}}}
        response = es.search(index=index_name, body=query, size=1)

        if response["hits"]["total"]["value"] > 0:
            document_id = response["hits"]["hits"][0]["_id"]
            prev_response = response["hits"]["hits"][0]["_source"]
            if "specialization_level" in prev_response:
                result_json = prev_response["specialization_level"]
            else:
                result_json = {}

            if isinstance(result_json, str):
                result_json = json.loads(result_json)

            
            if isinstance(results, str):
                results = json.loads(results)

            cld_degree_id = list(results.keys())

            if result_json == None:
                result_json = {}

            for id in cld_degree_id:
                result_json[str(id)] = results[id]

            update_body = {"doc": {update_field: json.dumps(result_json)}}
            update_response = es.update(
                index=index_name, id=document_id, body=update_body
            )
            print(f"Document updated in Prompt Result: ")

            return f"Document updated in Prompt Result: {update_response}"

        else:

            document_body = {"cld_id": cld_id, update_field: json.dumps(results)}
            create_response = es.index(index=index_name, body=document_body)
            print(f"Document created in Prompt Result: ")

            return f"Document created in Prompt Result: "
    except Exception as e:
        logging.error(f"Error in updating or creating document: {e}")
        print(f"Error in updating or creating document: {e}")
        return str(e)
