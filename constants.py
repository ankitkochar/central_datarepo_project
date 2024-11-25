es_institute_index_name = 'institute'
es_institute_index_mapping = {
    "properties": {
        "name" : {
            "type": "text",
        },
        "url": {
             "type": "text",
        },
        "cld_id": {
             "type": "integer",
        },
        "embedding_generated" : {
            "type": "boolean",
        },
        "prompt_output_generated" : {
            "type": "boolean",
        },
        "created_at" : {
            "type": "date",
        },
        "updated_at" : {
            "type": "date",
        }
    }
}