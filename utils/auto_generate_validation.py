import time
from multiprocessing import Process
from queue import Queue
from typing import Set, List, Dict
from utils.elastic import fetch_ip_answer
from utils.validation_check import validation_model

in_queue: Queue = Queue()
currently_running: Dict[str, Process] = {}
processed_institutes: Set[str] = set()
max_available_slots = 20


def auto_run_validation():
    while True:
        if in_queue.empty():
            ip_answer_objs = fetch_ip_answer()
            for ip_answer_obj in ip_answer_objs:
                ip_answer_obj_id = ip_answer_obj["_id"]
                if (
                    ip_answer_obj_id not in currently_running
                    and ip_answer_obj_id not in processed_institutes
                ):
                    in_queue.put(ip_answer_obj)

        finished = [
            ip_answer_obj_id
            for ip_answer_obj_id, process in currently_running.items()
            if not process.is_alive()
        ]
        for ip_answer_obj_id in finished:
            process = currently_running.pop(ip_answer_obj_id)
            process.join()  # Ensure the process is fully cleaned up
            processed_institutes.add(ip_answer_obj_id)
        if not in_queue.empty() and len(currently_running) < max_available_slots:
            available_slots = max_available_slots - len(currently_running)
            for _ in range(min(available_slots, in_queue.qsize())):
                ip_answer_obj = in_queue.get()
                ip_answer_obj_id = ip_answer_obj["_id"]
                if ip_answer_obj_id not in currently_running:
                    process = Process(target=validation_model, args=([ip_answer_obj]))
                    process.start()

                    currently_running[ip_answer_obj_id] = process

        time.sleep(5)
