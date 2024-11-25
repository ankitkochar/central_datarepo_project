import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Optional
from .utils import (
    get_chunk_index,
    check_id_already_exists,
    generate_embedding,
    update_institute_embedding_status,
)


def process_single_institute(
    inst_id: str, chunk_index: str, index_type: str, force: bool
) -> tuple[str, str]:
    try:
        if not force:
            already_exist = check_id_already_exists(inst_id)
            if already_exist:
                return inst_id, "Already Exist"

        if force:
            update_institute_embedding_status(inst_id, chunk_index)

        result = generate_embedding(inst_id, chunk_index, index_type)
        return inst_id, result
    except Exception as e:
        logging.error(f"Error processing institute {inst_id}: {str(e)}")
        return inst_id, f"Error: {str(e)}"


def extract_and_save_college_data(item, default=None, max_workers: int = 5):
    if default is not None:
        institute_ids = default["institute_ids"]
        index_type = default["index"]
        force = default["force"]
    else:
        institute_ids = item.institute_ids
        index_type = item.index
        force = item.force

    chunk_index = get_chunk_index(index_type)
    response = {}

    try:
        # Use ProcessPoolExecutor for parallel processing
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Create future tasks for all institutes
            future_to_inst = {
                executor.submit(
                    process_single_institute, inst_id, chunk_index, index_type, force
                ): inst_id
                for inst_id in institute_ids
            }

            # Process completed tasks as they finish
            for future in as_completed(future_to_inst):
                inst_id, result = future.result()
                response[inst_id] = result
                logging.info(f"Completed processing institute {inst_id}: {result}")

    except Exception as e:
        logging.error(f"Error in parallel processing: {str(e)}")
        # Fallback to sequential processing in case of parallel processing failure
        logging.info("Falling back to sequential processing")
        for inst_id in institute_ids:
            inst_id, result = process_single_institute(
                inst_id, chunk_index, index_type, force
            )
            response[inst_id] = result

    return response
