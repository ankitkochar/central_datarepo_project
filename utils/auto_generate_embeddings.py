import time
import logging
from multiprocessing import Process
from queue import Queue, Empty
from typing import Dict, Set
from embedding.controller import generate_embedding
from utils.elastic import fetch_institute_for_embedding
from dotenv import load_dotenv
import os

load_dotenv()
log_files_folder = os.environ.get("LOG_FILES_FOLDER")


try:
    log_file_path = os.path.join(log_files_folder, "download.log")
    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format="%(asctime)s - %(module)s - %(funcName)s - %(levelname)s - %(message)s",
    )
    logging.info("Logging started successfully.")
except Exception as e:
    print(f"Failed to set up logging: {e}")


def auto_run():
    in_queue: Queue = Queue()
    currently_running: Dict[str, Process] = {}
    processed_institutes: Set[str] = set()
    max_available_slots = 5

    logging.info("Starting auto_run for embeddings")

    try:
        while True:
            if in_queue.empty():
                institutes = fetch_institute_for_embedding()
                for institute in institutes:
                    if (
                        institute not in currently_running
                        and institute not in processed_institutes
                    ):
                        in_queue.put(institute)
                        logging.info(f"Added institute {institute} to queue")

            for inst_id, process in list(currently_running.items()):
                if not process.is_alive():
                    process.join()
                    del currently_running[inst_id]
                    processed_institutes.add(inst_id)
                    logging.info(f"Finished processing institute {inst_id}")

            while len(currently_running) < max_available_slots and not in_queue.empty():
                try:
                    inst_id = in_queue.get(block=False)
                    process = Process(
                        target=generate_embedding,
                        args=(inst_id, "chunk_by_sentence", "sentence"),
                    )
                    process.start()
                    currently_running[inst_id] = process
                    logging.info(f"Started processing institute {inst_id}")
                except Empty:
                    break

            if in_queue.empty() and len(currently_running) == 0:
                logging.info("All institutes processed. Exiting auto_run.")
                break

            time.sleep(300)

    except Exception as e:
        logging.error(f"Error in auto_run: {str(e)}")
    finally:
        for process in currently_running.values():
            process.terminate()
            process.join()
        logging.info("auto_run completed")
