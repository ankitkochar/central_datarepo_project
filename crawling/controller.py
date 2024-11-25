# Libraries
import time
from dotenv import load_dotenv
from multiprocessing import Process
from queue import Queue
from typing import Set, Dict
from concurrent.futures import ProcessPoolExecutor, as_completed
import logging

# Modules
from .utils import (
    check_already_downloaded,
    update_scrape_data_status,
    scrape_institute_data,
    run_institute,
    fetch_institute_for_scrapping,
)

load_dotenv()

# Initialization
in_queue: Queue = Queue()
currently_running: Dict[str, Process] = {}
processed_institutes: Set[str] = set()
max_available_slots = 40


def process_institute(inst_id, input_url, enable_javascript, force):
    logging.warning(f"INSTITUTE ID:- {inst_id}")

    if not force:
        if check_already_downloaded(inst_id):
            return inst_id, "Already Downloaded"

    update_scrape_data_status(inst_id, "scraper_info")
    result = scrape_institute_data(inst_id, input_url, enable_javascript)
    return inst_id, result


def download_and_save_scrape_data(item, max_workers: int = 40):
    institute_ids = item.institute_ids
    enable_javascript = item.enable_javascript
    input_url = item.url
    force = item.force
    response = {}

    try:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_inst = {
                executor.submit(
                    process_institute, inst_id, input_url, enable_javascript, force
                ): inst_id
                for inst_id in institute_ids
            }

            for future in as_completed(future_to_inst):
                inst_id, result = future.result()
                response[inst_id] = result
                logging.info(f"Completed processing institute {inst_id}: {result}")

    except Exception as e:
        logging.error(f"Error in parallel processing: {str(e)}")
        logging.info("Falling back to sequential processing")
        for inst_id in institute_ids:
            inst_id, result = process_institute(
                inst_id, input_url, enable_javascript, force
            )
            response[inst_id] = result

    return response


def auto_run_scrapper():

    run_auto_scrapper = True

    while run_auto_scrapper:
        if in_queue.empty():
            institutes = fetch_institute_for_scrapping()
            for institute in institutes:
                if (
                    institute not in currently_running
                    and institute not in processed_institutes
                ):
                    in_queue.put(institute)

        finished = [
            inst_id
            for inst_id, process in currently_running.items()
            if not process.is_alive()
        ]

        for inst_id in finished:
            process = currently_running.pop(inst_id)
            process.join()
            processed_institutes.add(inst_id)

        if in_queue.empty() and len(currently_running) == 0:
            run_auto_scrapper = False

        if not in_queue.empty() and len(currently_running) < max_available_slots:
            available_slots = max_available_slots - len(currently_running)
            for _ in range(min(available_slots, in_queue.qsize())):
                inst_id = in_queue.get()

                if inst_id not in currently_running:
                    process = Process(
                        target=run_institute,
                        args=(inst_id,),
                    )
                    process.start()
                    currently_running[inst_id] = process

        if len(currently_running) > 0:
            time.sleep(300)
