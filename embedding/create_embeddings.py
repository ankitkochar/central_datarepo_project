from dotenv import load_dotenv
import os
from haystack.utils import Secret
import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from haystack.document_stores.types import DuplicatePolicy
from .custom_converters import (
    URLToDocumentConverterMarkdownify,
    DocxToTextConverter,
    PDFToDocumentConverter,
)

from haystack import Pipeline
from haystack.components.preprocessors import DocumentCleaner
from haystack.components.writers import DocumentWriter
from haystack.components.embedders import (
    AzureOpenAIDocumentEmbedder,
    OpenAIDocumentEmbedder,
)
from haystack_integrations.document_stores.elasticsearch import (
    ElasticsearchDocumentStore,
)
from .custom_doc_splitter import CustomDocumentSplitter
import logging

load_dotenv()  # Load the .env file
es_host = os.environ.get("ELASTIC_SEARCH_HOST")
es_user = os.getenv("ELASTICSEARCH_USER")
es_password = os.getenv("ELASTICSEARCH_PASSWORD")

temp_dw_folder = os.environ.get("TEMP_DW_FOLDER")
open_ai_key = os.environ.get("OPENAI_API_KEY")
log_files_folder = os.environ.get("LOG_FILES_FOLDER")
azure_endpoint = os.environ.get("AZURE_ENDPOINT")
azure_openai_api_key = os.environ.get("AZURE_OPENAI_API_KEY")
azure_embedding_deployment_model = os.environ.get("AZURE_EMBEDDING_DEPLOYMENT_MODEL")
try:
    log_file_path = os.path.join(log_files_folder, "indexing.log")
    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format="%(asctime)s - %(module)s - %(funcName)s - %(levelname)s - %(message)s",
    )
    logging.info("Logging started successfully.")
except Exception as e:
    print(f"Failed to set up logging: {e}")


def write_documents(url_details, inst_id, document_store, index_type):
    """
    Processes a given document by converting, cleaning, splitting, embedding, and writing it to a document store.

    Parameters:
    - file_path (Path): The path to the document file.
    - inst_id (str): An identifier for the institution to which the document belongs.
    - document_store (ElasticsearchDocumentStore): The document store where the processed documents will be stored.

    Raises:
    - Exception: Propagates exceptions that might occur during document processing.
    """

    file_path = url_details["actual_url"]

    pipeline = Pipeline()
    logging.info(f"Starting processing for document at path: {file_path}")

    file_url = file_path
    s3_url = url_details["s3_url"]
    file_path = s3_url
    # print("file_path", file_path)

    if file_path.lower().endswith(".pdf"):
        pipeline.add_component("converter", PDFToDocumentConverter())
        filetype = "pdf"
    elif file_path.lower().endswith(".html"):
        # pipeline.add_component("converter", HTMLToDocument())
        pipeline.add_component("converter", URLToDocumentConverterMarkdownify())

        filetype = "html"
    elif file_path.lower().endswith(".docx"):
        pipeline.add_component("converter", DocxToTextConverter())
        filetype = "docx"
    elif file_path.lower().endswith(".doc"):
        pipeline.add_component("converter", DocxToTextConverter())
        filetype = "doc"
    else:
        return
    pipeline.add_component("cleaner", DocumentCleaner())

    if index_type == "passage":
        pipeline.add_component(
            "splitter",
            CustomDocumentSplitter(
                split_by="passage", split_length=2, split_overlap=0, chunk_length=500
            ),
        )
    elif index_type == "sentence":
        pipeline.add_component(
            "splitter",
            CustomDocumentSplitter(
                split_by="sentence", split_length=50, split_overlap=4, chunk_length=500
            ),
        )
    else:
        pipeline.add_component(
            "splitter",
            CustomDocumentSplitter(
                split_by="word", split_length=500, split_overlap=30, chunk_length=500
            ),
        )
    #    pipeline.add_component(
    #        "embedder",
    #        AzureOpenAIDocumentEmbedder(
    #            azure_endpoint=azure_endpoint,
    #            api_key=Secret.from_token(azure_openai_api_key),
    #            azure_deployment=azure_embedding_deployment_model,
    #        ),
    #    )
    pipeline.add_component(
        "embedder",
        OpenAIDocumentEmbedder(
            api_key=Secret.from_token(open_ai_key),
            model="text-embedding-3-large",
            progress_bar=False,
        ),
    )
    pipeline.add_component(
        "writer",
        DocumentWriter(document_store=document_store, policy=DuplicatePolicy.OVERWRITE),
    )

    pipeline.connect("converter", "cleaner")
    pipeline.connect("cleaner", "splitter")
    pipeline.connect("splitter", "embedder")
    pipeline.connect("embedder", "writer")

    # print(f"writing document {file_path}")
    try:
        pipeline.run(
            {
                "converter": {
                    "sources": [s3_url],
                    "meta": {
                        "institute_id": inst_id,
                        "filetype": filetype,
                        "file_url": file_url,
                        "s3_url": s3_url,
                        "status": True,
                    },
                }
            }
        )
        logging.info(f"Document processing completed for: {file_path}")

    except Exception as e:
        logging.error(f"Failed to process document {file_path}: {e}")


async def process_documents_async(url_details, inst_id, executor, index, index_type):
    """
    Asynchronously processes documents by creating a document store and running the document writing operations in a thread pool.

    Parameters:
    - file_path (Path): The path to the document file.
    - inst_id (str): An identifier for the institution to which the document belongs.
    - executor (ThreadPoolExecutor): The executor to run asynchronous tasks.
    - index (str): The index name in the Elasticsearch document store.

    Returns:
    None
    """
    # print(f"Writing document {file_path.name}")
    loop = asyncio.get_running_loop()
    document_store = ElasticsearchDocumentStore(
        hosts=es_host, index=index, basic_auth=(es_user, es_password)
    )

    # Run the synchronous function using a thread pool
    await loop.run_in_executor(
        executor,
        write_documents,
        url_details,
        inst_id,
        document_store,
        index_type,
    )


async def process_all_documents(scrape_data, inst_id, index_type, index):
    """
    Processes all documents in the given directories asynchronously.

    Parameters:
    - directories (list): A list of directories containing the documents to process.
    - inst_id (str): An identifier for the institution to which the documents belong.
    - index (str): The index name in the Elasticsearch document store.

    Returns:
    None
    """

    # nest_asyncio.apply()
    executor = ThreadPoolExecutor(max_workers=1)
    tasks = []
    print(f"Writing documents for instid {inst_id}")

    for url_details in scrape_data:
        task = process_documents_async(
            url_details,
            inst_id,
            executor,
            index,
            index_type,
        )
        tasks.append(task)
    await asyncio.gather(*tasks)
    executor.shutdown()
