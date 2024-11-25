from datetime import datetime
from haystack_integrations.components.retrievers.elasticsearch import (
    ElasticsearchEmbeddingRetriever,
    ElasticsearchBM25Retriever,
)
from haystack.components.embedders import OpenAITextEmbedder
from haystack.components.joiners import DocumentJoiner
from haystack.components.builders.prompt_builder import PromptBuilder
from haystack.components.builders.answer_builder import AnswerBuilder
from haystack.components.generators import OpenAIGenerator, AzureOpenAIGenerator
from haystack import Pipeline
from haystack.utils import Secret
from dotenv import load_dotenv
import os
import pandas as pd
from haystack_integrations.document_stores.elasticsearch import (
    ElasticsearchDocumentStore,
)
from .save_results_to_es import check_and_update_document_overview_initial_population
from elasticsearch import Elasticsearch
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import warnings
import logging
from utils.elastic import update_prompts_institute
from .custom_component import AIChunkCompressing 
import json

load_dotenv()  # Load the .env file
log_files_folder = os.environ.get("LOG_FILES_FOLDER")
open_ai_key = os.environ.get("OPENAI_API_KEY")
fireworks_api_key = os.environ.get("FIREWORKS_API_KEY")
es_host = os.environ.get("ELASTIC_SEARCH_HOST")
es_user = os.getenv("ELASTICSEARCH_USER")
es_password = os.getenv("ELASTICSEARCH_PASSWORD")
prompts_institute_index = os.environ.get("PROMPTS_INSTITUTE_INDEX")
prompts_index = os.environ.get("PROMPTS")
prompts_run_log_index = os.environ.get("PROMPTS_RUN_LOGS")
prompts_result_index = os.environ.get("PROMPTS_RESULT")
azure_4omini_endpoint = os.environ.get("AZURE_4OMINI_ENDPOINT")
azure_4omini_key = os.environ.get("AZURE_4OMINI_KEY")
fireworks_api_url = "https://api.fireworks.ai/inference/v1"
drive_folder_id_external = os.environ.get("DRIVE_FOLDER_ID_EXTERNAL")
drive_folder_id_internal = os.environ.get("DRIVE_FOLDER_ID_INTERNAL")

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


def query_pipeline_answer_builder(
    q,
    inst_id,
    answer_data_type,
    document_store,
    search_terms=None,
    num_chunks=None,
    model="4o-mini",
    sys_prompt=None,
    grammar=False,
    response_type="text",
):
    """
    Constructs and runs a query pipeline with hybrid retrieval methods, generating an answer using OpenAI's model.

    Parameters:
    - q (str): The text query to be processed.
    - answer_data_tpye (str): The type of data to be extracted from the documents.
    - inst_id (str): Institution identifier to filter documents in the retrieval.
    - document_store (ElasticsearchDocumentStore): The Elasticsearch store to retrieve and store documents.
    - model (str): The model to be used for answer generation. Values are 3.5t mixtral7b mixtral22b llama. defaults to 3.5t
    Returns:
    - dict: A dictionary containing the results from running the pipeline on the query.

    This function sets up a pipeline with text embedding and retrieval components, followed by document joining,
    prompt building, and answer generation using OpenAI's language model.
    """
    if not search_terms:
        search_terms = q

    models = {
        "3.5t": "gpt-3.5-turbo-0125",
        "mixtral7b": "accounts/fireworks/models/mixtral-8x7b-instruct",
        "mixtral22b": "accounts/fireworks/models/mixtral-8x22b-instruct",
        "llama": "accounts/fireworks/models/llama-v3-70b-instruct",
        "4t": "gpt-4-turbo",
        "4o-mini": "gpt-4o-mini",
    }
    model_key = models.get(model, None)
    model_to_chunk_size = {"default": 7, "llama": 7, "mixtral22b": 10, "4t": 20}
    if not num_chunks:
        chunk_size = model_to_chunk_size.get(model, model_to_chunk_size["default"])
    else:
        chunk_size = num_chunks

    if model in ["3.5t", "4t"]:
        api_key = open_ai_key
    else:
        api_key = fireworks_api_key
    base_urls = {
        "3.5t": None,
        "4t": None,
        "mixtral7b": "https://api.fireworks.ai/inference/v1",
        "mixtral22b": "https://api.fireworks.ai/inference/v1",
        "llama": "https://api.fireworks.ai/inference/v1",
        "4o-mini": azure_4omini_endpoint,
    }
    base_url = base_urls.get(model, None)
    prompt_template = """
    You are given a user's query in the Question field. Respond appropriately to the user's input using only the documents in the Documents field. If you can not find the answer, return "I do not have that information."
    \nDocuments:
    {% for doc in documents %}
        {{ doc.content }}
    Source: {{ doc.id }}
    {% endfor %}

    \nQuestion: {{query}}
    \nAnswer:
    """
    if not sys_prompt:
        system = f"""You are a professional data fetcher to fetch college details. Give answer and the source ids from which you concluded the answer.
        DO NOT INCLUDE ANYTHING EXCEPT THE ANSWER REQUESTED.
        
        Give Answer in the format given below:
        {{
          "answer" : "Lovely Professional university is a very popular university for higher education."
          "sources" : [41412412412bh4jh1b2c,43rnfjknffxcasjkcnasjc]
        }}
        """
    else:
        system = sys_prompt
    query_pipeline = Pipeline()
    query_pipeline.add_component(
        "text_embedder",
        OpenAITextEmbedder(
            Secret.from_token(f"{open_ai_key}"),
            model="text-embedding-3-large",
        ),
    )

    system_prompt_answer_formatter = f"""
    You are a highly professional formatter, who formats a given json object fields value into the format shown in the Format field returning only that.
    \nFormat: {answer_data_type} format.
    """
    prompt_template_answer_formatter = """
    You are the second stage in a two-LLM pipeline. Your task is to reformat the answer provided by the first LLM according to a specified format. You will receive the original query and the output from the first LLM.
    
    Input:
    - Query: {{query}}
    - Result: {{results}} (This is the output from the first LLM, containing an answer and sources)
    - Format: {{answer_data_type}} format
    
    Instructions:
    1. Extract the "answer" field from the Result.
    2. Reformat this answer according to the specified Format.
    3. Do not alter the content of the answer; your task is purely reformatting.
    4. Do not add any additional information, explanations, or text.
    5. Preserve the "sources" field from the Result without modification.
    
    Output your response in this JSON format:
    {
      "answer": [Reformatted answer in the specified Format],
      "sources": [Array of sources from the input Result]
    }
    
    Note: If the input Result is an empty string or doesn't contain an answer, return an empty string for the "answer" field and an empty array for the "sources" field.    """

    # filters = {"field": "meta.institute_id", "operator": "==", "value": f"{inst_id}"}
    filters = {
        "operator": "AND",
        "conditions": [
            {"field": "meta.institute_id", "operator": "==", "value": f"{inst_id}"},
            {"field": "meta.status", "operator": "==", "value": True},
        ],
    }
    query_pipeline.add_component(
        "retriever",
        ElasticsearchEmbeddingRetriever(
            document_store=document_store, top_k=20, filters=filters
        ),
    )
    query_pipeline.add_component(
        "bm25_retriever",
        ElasticsearchBM25Retriever(
            document_store=document_store, top_k=20, filters=filters
        ),
    )

    query_pipeline.add_component(instance=AIChunkCompressing(name="Embedding Retriever", query=q, inst_id=inst_id), name="embedding_chunk_compressing")
    query_pipeline.add_component(instance=AIChunkCompressing(name="BM25 Retriever", query=q, inst_id=inst_id), name="bm25_chunk_compressing")

    query_pipeline.add_component(
        "joiner",
        DocumentJoiner(
            join_mode="reciprocal_rank_fusion", top_k=chunk_size, weights=[0.2, 0.8]
        ),
    )
    query_pipeline.add_component(
        instance=PromptBuilder(template=prompt_template), name="prompt_builder"
    )
    if grammar:
        grammar = """
        root ::= city
        """
        generation_kwargs = {
            "temperature": 0,
            "extra_body": {"response_format": {"type": "grammar", "grammar": grammar}},
        }
    else:
        generation_kwargs = {"temperature": 0}
    query_pipeline.add_component(
        instance=AzureOpenAIGenerator(
            api_key=Secret.from_token(f"{azure_4omini_key}"),
            system_prompt=system,
            azure_deployment="gpt-4o-mini",
            azure_endpoint=base_url,
            generation_kwargs=generation_kwargs,
        ),
        name="llm",
    )

    query_pipeline.add_component(
        instance=AzureOpenAIGenerator(
            api_key=Secret.from_token(f"{azure_4omini_key}"),
            system_prompt=system_prompt_answer_formatter,
            azure_deployment="gpt-4o-mini",
            azure_endpoint=base_url,
            generation_kwargs = {
                "temperature": 0,
                "response_format": {"type": "json_object"}
            },
        ),
        name="llm_answer_formatter",
    )

    query_pipeline.add_component(
        instance=PromptBuilder(template=prompt_template_answer_formatter),
        name="prompt_builder_answer_formatter",
    )
    query_pipeline.add_component(instance=AnswerBuilder(), name="answer_builder")

    query_pipeline.connect("text_embedder.embedding", "retriever.query_embedding")
    query_pipeline.connect("retriever", "embedding_chunk_compressing")
    query_pipeline.connect("bm25_retriever", "bm25_chunk_compressing")
    query_pipeline.connect("embedding_chunk_compressing", "joiner")
    query_pipeline.connect("bm25_chunk_compressing", "joiner")
    query_pipeline.connect("joiner", "prompt_builder.documents")
    query_pipeline.connect("prompt_builder", "llm")
    query_pipeline.connect("llm_answer_formatter.meta", "answer_builder.meta")
    query_pipeline.connect("joiner", "answer_builder.documents")
    query_pipeline.connect("llm.replies", "prompt_builder_answer_formatter.results")
    query_pipeline.connect("prompt_builder_answer_formatter", "llm_answer_formatter")
    query_pipeline.connect("llm_answer_formatter.replies", "answer_builder.replies")

    logging.info(
        f"Starting query pipeline for query: '{q}' and institution ID: '{inst_id}'"
    )

    result = query_pipeline.run(
        {
            "text_embedder": {"text": search_terms},
            "bm25_retriever": {"query": search_terms},
            "prompt_builder": {"query": q},
            "prompt_builder_answer_formatter": {
                "answer_data_type": answer_data_type,
                "query": q,
            },
            "answer_builder": {"query": q},
        }
    )
    logging.info(f"Query pipeline execution completed for query: '{q}'")

    return result


def get_gspread_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.file",
    ]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        "/opt/central_datarepo_project/utils/centralrepo-aba6b4b4b90e.json", scope
    )

    client = gspread.authorize(credentials)
    return client


def save_results_to_sheets(results, inst_id, model, level="college"):
    # TODO Need to decide where to store the final results/update auto upload to google sheets.

    """

    Parameters:
    - results (dict): The results from the query pipeline.
    - inst_id (str): The identifier for the institution, used to name the output Excel file.

    This function parses the results from the query pipeline and saves them into aa google drive folder.
    """
    internal_flag = True  # Set this to False if you want to save only external results
    data_internal = []
    data_external = []
    for query, answer in results.items():
        if "answer_builder" in answer:
            answer_data = answer["answer_builder"]["answers"][0].data
            documents = answer["answer_builder"]["answers"][0].documents
            data_type = answer["data_type"]
            tags = answer["tags"]
            category = answer["category"]

            # Initialize lists to store details of each document
            contents = []
            source_ids = []
            # file_paths = []
            document_ids = []
            scores = []
            s3_links = []
            original_links = []
            # Collect details for each document associated with the current answer
            for doc in documents:
                contents.append(doc.content)
                source_ids.append(doc.meta["source_id"])
                # file_paths.append(doc.meta["file_path"])
                document_ids.append(doc.id)
                scores.append(doc.score)
                try:
                    s3_links.append(doc.meta["s3_url"])
                except:
                    s3_links.append("")
                original_links.append(doc.meta["file_url"])
            # Append a new row to the DataFrame
            if internal_flag == True:
                row = {
                    "tags": tags,
                    "category": category,
                    "query": query,
                    "answer": answer_data,
                    "data_type": data_type,
                    "s3_links": s3_links,
                    "original_links": original_links,
                    "scores": scores,
                    "contents": " ".join(contents)[:49990],
                    "source_ids": source_ids,
                    # "file_paths": file_paths,
                    "document_ids": document_ids,
                }
                data_internal.append(row)

            row_external = {
                "query": query,
                "answer": answer_data,
                "tags": tags,
                "category": category,
            }
            data_external.append(row_external)
        else:
            answer_data = "Error"
            documents = ""
            data_type = answer["data_type"]
            tags = answer["tags"]
            category = answer["category"]

            # Initialize lists to store details of each document
            contents = []
            source_ids = []
            # file_paths = []
            document_ids = []
            scores = []
            s3_links = []
            original_links = []
            # Collect details for each document associated with the current answer
            # for doc in documents:
            contents = ""
            source_ids = ""
            # file_paths.append(doc.meta["file_path"])
            document_ids = ""
            scores = ""
            s3_links = ""
            original_links = ""
            # Append a new row to the DataFrame
            if internal_flag == True:
                row = {
                    "query": query,
                    "answer": answer_data,
                    "data_type": data_type,
                    "tags": tags,
                    "category": category,
                    "s3_links": s3_links,
                    "original_links": original_links,
                    "scores": scores,
                    "contents": " ".join(contents)[:49990],
                    "source_ids": source_ids,
                    # "file_paths": file_paths,
                    "document_ids": document_ids,
                }
                data_internal.append(row)

            row_external = {
                "query": query,
                "answer": answer_data,
                "tags": tags,
                "category": category,
            }
            data_external.append(row_external)
    current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
    title = f"{inst_id}_answers_{level}_{current_time}_{model}"
    client = get_gspread_client()
    sheet_external = client.create(f"{title}", folder_id=drive_folder_id_external)
    df_external = pd.DataFrame(data_external)
    df_external.fillna("", inplace=True)
    worksheet_external = sheet_external.get_worksheet(0)
    worksheet_external.update(
        [df_external.columns.values.tolist()] + df_external.astype(str).values.tolist()
    )
    logging.info("Results successfully saved to Google Sheets.")

    if internal_flag == True:
        sheet_internal = client.create(f"{title}", folder_id=drive_folder_id_internal)
        df_internal = pd.DataFrame(data_internal)
        df_internal.fillna("", inplace=True)
        worksheet_internal = sheet_internal.get_worksheet(0)
        worksheet_internal.update(
            [df_internal.columns.values.tolist()]
            + df_internal.astype(str).values.tolist()
        )


def fetch_overall_prompts_from_es(es, index_name):

    try:
        query = {
            "bool": {
                "filter": [
                    {"term": {"status": True}},
                    {"term": {"degree_specific": False}},
                    {
                        "term": {
                            "specialization_specific": False,
                        }
                    },
                ]
            }
        }
        response = es.search(index=index_name, query=query, size=200)
        return dict(map(lambda x: (x["_id"], x), response["hits"]["hits"]))
    except Exception as e:
        logging.error(f"Error fetching prompts from Elasticsearch: {e}")
        return []


def get_prompts_to_run(es, inst_id, prompts_obj_id_dict):
    try:
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"status": True}},
                        {"match": {"institute_id": inst_id}},
                        {"match": {"run_status": True}},
                    ]
                }
            },
            "size": 500,
        }
        response = es.search(index=prompts_institute_index, body=query)
        hits = response["hits"]["hits"]
        prompts_already_done = list(map(lambda x: x["_source"]["prompt_id"], hits))
        prompts_id_keys_list = list(prompts_obj_id_dict.keys())
        prompts_to_run = list(set(prompts_id_keys_list) - set(prompts_already_done))
        return prompts_to_run
    except Exception as e:
        logging.error(f"Failed to GET Prompts from Index, {e}")


def add_prompts_institute_entry(es, inst_id, prompt_id):
    try:
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
        logging.error(f"Failed to ADD Prompts Institute from Index, {e}")


def add_prompts_run_logs_entry(es, prompt_id, inst_id, result, data_type):
    """
    Adds a new entry to the prompts_run_logs index.

    Parameters:
    - es (Elasticsearch): The Elasticsearch client.
    - prompt_id (str): The identifier for the prompt.
    - inst_id (str): The identifier for the institution.
    - result (dict): The result from the query pipeline.
    - data_type (str): The data type of the answer.
    """
    try:
        es.index(
            index=prompts_run_log_index,
            body={
                "prompt_id": prompt_id,
                "institute_id": inst_id,
                "answer": result,
                "answer_data_type": data_type,
                "validation_status": True,
                "status": True,
            },
        )

    except Exception as e:
        logging.error(f"Failed to ADD Prompts RUN LOGS from Index, {e}")


def add_answer_in_ip_answer(es, ip_id, answer, data_type, original_links, tags, source_links):
    try:
        ip_answer_obj = es.index(
            index="ip_answer",
            body={
                "ip_id": ip_id,
                "answer": answer,
                "answer_data_type": data_type,
                "validation_run_status": False,
                "status": True,
                "original_links": str(original_links),
                "tags": tags,
                "citations": str(source_links)  
            },
        )
        logging.info("Added answer in ip_answer table ")

        return {"_id": ip_answer_obj["_id"], "answer": answer, "ip_id": ip_id}
    except Exception as e:
        logging.error(f"Failed to ADD Prompts RUN LOGS from Index, {e}")

    
def run_query_pipeline(inst_id, index, model="mixtral22b"):
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
    update_prompts_institute(inst_id)
    prompts_obj_id_dict = fetch_overall_prompts_from_es(es, prompts_index)
    results = {}
    logging.info(
        f"Running query pipeline for institution ID: '{inst_id}' using model: '{model}'"
    )

    new_prompt_status = False
    prompts_to_run_list = get_prompts_to_run(es, inst_id, prompts_obj_id_dict)

    ip_answer_objs = []

    for prompt_id in prompts_to_run_list:
        try:
            prompt_obj = prompts_obj_id_dict[prompt_id]
            source = prompt_obj["_source"]
            new_prompt_status = True
            prompt = source["prompt"]
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
            results[prompt] = query_pipeline_answer_builder(
                prompt,
                inst_id,
                data_type,
                document_store,
                search_terms=search_terms,
                num_chunks=int(num_chunks),
                model=model,
                response_type=response_type,
            )
            answer_obj = (
                results[prompt].get("answer_builder", {}).get("answers", [])[0].data
            )

            answer_obj = json.loads(answer_obj)
            answer = answer_obj['answer']
            sources = answer_obj['sources']

            documents = (
                results[prompt]
                .get("answer_builder", {})
                .get("answers", [])[0]
                .documents
            )

            results[prompt].update(
                {"data_type": data_type, "tags": tags, "category": category}
            )

            original_links = []
            source_links = []

            for doc in documents:
                if str(doc.id) in sources:
                    source_links.append(doc.meta["file_url"])
                original_links.append(doc.meta["file_url"])

            ip_id = add_prompts_institute_entry(es, inst_id, prompt_id)
            ip_answer_obj = add_answer_in_ip_answer(
                es, ip_id, answer, data_type, original_links, tags, source_links
            )
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
