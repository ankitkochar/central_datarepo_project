from haystack.dataclasses import Document
from typing import List
from haystack import component
from utils.auth_utils import get_response_from_gpt
"""
This is Custom Haystack Component which takes retrieved chunks from ElasticEmbedding and BM25 retriever and Prints It.
This chunks Can be further manipulated in any way possible and Return desired chunks.
"""
@component
class AIChunkCompressing:
    def __init__(self, name: str, query: str, inst_id : int):
        self.name = name
        self.query = query
        self.inst_id = inst_id

    @component.output_types(chunks=List[Document])
    def run(self, chunks: List[Document]):
        updated_chunks = []
        for i, chunk in enumerate(chunks, 1):
            content = chunk.content
            prompt = f"""
                        You are given a query and context. 
                        Determine whether the context contains information that can answer the query. 
                        Respond only with "Yes" or "No", and do not include any additional explanation.

                        ### Query Start
                        {self.query}
                        ### Query End

                        ### Context Start
                        {content}
                        ### Context End
                    """
            answer = get_response_from_gpt(prompt)
            if answer.lower() == 'yes':
                updated_chunks.append(chunk)     

        return {"chunks": updated_chunks}
