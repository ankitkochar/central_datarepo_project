from copy import deepcopy
from typing import Dict, List, Literal, Tuple
import os

from more_itertools import windowed
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from haystack import Document, component
import tiktoken

load_dotenv()
es_host = os.getenv("ELASTIC_SEARCH_HOST")
es_user = os.getenv("ELASTICSEARCH_USER")
es_password = os.getenv("ELASTICSEARCH_PASSWORD")

es = Elasticsearch(es_host, basic_auth=(es_user, es_password))

import re

MAX_SPLIT_LENGTH = 25000
MAX_TOKENS = 8000
ENCODING = tiktoken.get_encoding("cl100k_base")


def get_degrees():
    degree_names = []

    query = {"query": {"match_all": {}}, "_source": ["name", "short_name"]}

    response_degree = es.search(index="degree", body=query, size=10000)
    for doc in response_degree["hits"]["hits"]:
        name = doc["_source"].get("name", "No name available")
        short_name = doc["_source"].get("short_name", "No short name available")
        degree_names.append((name, short_name))
    return degree_names


def get_degrees_variations(degree_names):

    degree_dict = {}

    for full_name, short_name in degree_names:
        normalized_short_name = short_name.lower().replace(".", "")
        degree_dict.setdefault(short_name, normalized_short_name)

        if "(" in full_name and "+" not in full_name:
            pre_parenthesis = full_name.split("(")[0].lower()
            pre_parenthesis_2 = full_name.split("(")[0].strip(". ").lower()

            if pre_parenthesis != "":
                degree_dict.setdefault(pre_parenthesis, normalized_short_name)
            if pre_parenthesis_2 != "":
                degree_dict.setdefault(pre_parenthesis_2, normalized_short_name)
    return degree_dict


def replace_degrees(text, degree_dict):
    sorted_keys = sorted(degree_dict.keys(), key=len, reverse=True)
    pattern = r"\b(" + "|".join(map(re.escape, sorted_keys)) + r")\b"

    def replace_func(match):
        match_text = match.group(0).lower().replace(".", "")
        return degree_dict.get(match_text, match_text)

    txt = re.sub(pattern, replace_func, text, flags=re.IGNORECASE)
    txt = re.sub(r"\.{2,}", ".", txt)
    return re.sub(r"[^A-Za-z0-9 .!%&=*#@+_():|{}<>/:\\\-\[\]\n]", "", txt)


@component
class CustomDocumentSplitter:
    """
    Splits a list of text documents into a list of text documents with shorter texts.

    Splitting documents with long texts is a common preprocessing step during indexing.
    This allows Embedders to create significant semantic representations
    and avoids exceeding the maximum context length of language models.
    """

    def __init__(
        self,
        split_by: Literal["word", "sentence", "page", "passage"] = "word",
        split_length: int = 200,
        split_overlap: int = 0,
        chunk_length: int = 1,
    ):
        """
        Initialize the DocumentSplitter.

        :param split_by: The unit by which the document should be split. Choose from "word" for splitting by " ",
            "sentence" for splitting by ".", "page" for splitting by "\\f" or "passage" for splitting by "\\n\\n".
        :param split_length: The maximum number of units in each split.
        :param split_overlap: The number of units that each split should overlap.
        """

        self.split_by = split_by
        if split_by not in ["word", "sentence", "page", "passage"]:
            raise ValueError(
                "split_by must be one of 'word', 'sentence', 'page' or 'passage'."
            )
        if split_length <= 0:
            raise ValueError("split_length must be greater than 0.")
        self.split_length = split_length
        if split_overlap < 0:
            raise ValueError("split_overlap must be greater than or equal to 0.")
        self.split_overlap = split_overlap
        self.chunk_length = chunk_length

    @component.output_types(documents=List[Document])
    def run(self, documents: List[Document]):
        """
        Split documents into smaller parts.

        Splits documents by the unit expressed in `split_by`, with a length of `split_length`
        and an overlap of `split_overlap`.

        :param documents: The documents to split.

        :returns: A dictionary with the following key:
            - `documents`: List of documents with the split texts. A metadata field "source_id" is added to each
            document to keep track of the original document that was split. Another metadata field "page_number" is added to each number to keep track of the page it belonged to in the original document. Other metadata are copied from the original
            document.

        :raises TypeError: if the input is not a list of Documents.
        :raises ValueError: if the content of a document is None.
        """

        if not isinstance(documents, list) or (
            documents and not isinstance(documents[0], Document)
        ):
            raise TypeError("DocumentSplitter expects a List of Documents as input.")

        split_docs = []
        for doc in documents:
            if doc.content is None:
                raise ValueError(
                    f"DocumentSplitter only works with text documents but document.content for document ID {doc.id} is None."
                )
            units = self._split_into_units(doc.content, self.split_by)
            text_splits, splits_pages = self._concatenate_units(
                units, self.split_length, self.split_overlap
            )
            metadata = deepcopy(doc.meta)
            metadata["source_id"] = doc.id
            split_docs += self._create_docs_from_splits(
                text_splits=text_splits, splits_pages=splits_pages, meta=metadata
            )
        return {"documents": split_docs}

    def _split_into_units(
        self, text: str, split_by: Literal["word", "sentence", "passage", "page"]
    ) -> List[str]:
        # First, extract tables and replace them with placeholders
        tables = re.findall(r"\[TABLE\].*?\[/TABLE\]", text, re.DOTALL)
        for i, table in enumerate(tables):
            text = text.replace(table, f"[TABLE_PLACEHOLDER_{i}]")

        if split_by == "page":
            split_at = "\f"
        elif split_by == "passage":
            split_at = "\n\n"
        elif split_by == "sentence":
            split_at = "."
        elif split_by == "word":
            split_at = " "
        else:
            raise NotImplementedError(
                "DocumentSplitter only supports 'word', 'sentence', 'page' or 'passage' split_by options."
            )

        degrees = get_degrees()
        degree_variation = get_degrees_variations(degrees)
        text = replace_degrees(text, degree_variation)

        units = text.split(split_at)
        for i in range(len(units) - 1):
            units[i] += split_at

        final_units = []
        placeholder_pattern = re.compile(r"\[TABLE_PLACEHOLDER_(\d+)\]")

        for unit in units:
            placeholders = placeholder_pattern.findall(unit)

            if placeholders:
                for placeholder in placeholders:
                    table_index = int(placeholder)
                    if 0 <= table_index < len(tables):
                        unit = unit.replace(
                            f"[TABLE_PLACEHOLDER_{table_index}]", tables[table_index]
                        )

            final_units.append(unit)

        return final_units

    def _concatenate_units(
        self, elements: List[str], split_length: int, split_overlap: int
    ) -> Tuple[List[str], List[int]]:
        text_splits = []
        splits_pages = []
        cur_page = 1
        segments = windowed(elements, n=split_length, step=split_length - split_overlap)

        current_split = ""
        current_split_page = cur_page
        current_token_count = 0

        for i, seg in enumerate(segments):
            current_units = [unit for unit in seg if unit is not None]

            for unit in current_units:
                unit_tokens = len(ENCODING.encode(unit))

                if current_token_count + unit_tokens > MAX_TOKENS:
                    if current_split:
                        text_splits.append(current_split)
                        splits_pages.append(current_split_page)
                    current_split = ""
                    current_token_count = 0

                current_split += unit
                current_token_count += unit_tokens

                while current_token_count > MAX_TOKENS:
                    encoded_split = ENCODING.encode(current_split)
                    text_splits.append(ENCODING.decode(encoded_split[:MAX_TOKENS]))
                    splits_pages.append(current_split_page)
                    current_split = ENCODING.decode(encoded_split[MAX_TOKENS:])
                    current_token_count = len(ENCODING.encode(current_split))

            if self.split_by == "page":
                num_page_breaks = len(current_units)
            else:
                num_page_breaks = sum(unit.count("\f") for unit in current_units)
            cur_page += num_page_breaks

        if current_split:
            text_splits.append(current_split)
            splits_pages.append(current_split_page)

        return text_splits, splits_pages

    @staticmethod
    def _create_docs_from_splits(
        text_splits: List[str], splits_pages: List[int], meta: Dict
    ) -> List[Document]:
        """
        Creates Document objects from text splits enriching them with page number and the metadata of the original document.
        """
        documents: List[Document] = []

        for i, txt in enumerate(text_splits):
            if len(txt.strip()) <= 50:
                continue
            meta = deepcopy(meta)
            doc = Document(content=txt, meta=meta)
            doc.meta["page_number"] = splits_pages[i]
            documents.append(doc)
        return documents
