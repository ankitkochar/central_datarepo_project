# import time
# from multiprocessing import Process
# from queue import Queue
# from typing import Set, List, Dict
# from utils.elastic_search import fetch_ipa_validation
# from utils.transformation_run import transformation_run

# in_queue: Queue = Queue()
# currently_running: Dict[str, Process] = {}
# processed_institutes: Set[str] = set()
# max_available_slots = 1


# def auto_run_transformation():
#     while True:
#         if in_queue.empty():
#             ipa_validation_objs = fetch_ipa_validation()
#             for ipa_validation_obj in ipa_validation_objs:
#                 ipa_validation_obj_id = ipa_validation_obj["_id"]
#                 if (
#                     ipa_validation_obj_id not in currently_running
#                     and ipa_validation_obj_id not in processed_institutes
#                 ):
#                     in_queue.put(ipa_validation_obj)

#         finished = [
#             ipa_validation_obj_id
#             for ipa_validation_obj_id, process in currently_running.items()
#             if not process.is_alive()
#         ]
#         for ipa_validation_obj_id in finished:
#             process = currently_running.pop(ipa_validation_obj_id)
#             process.join()
#             processed_institutes.add(ipa_validation_obj_id)
#         available_slots = max_available_slots - len(currently_running)
#         if not in_queue.empty() and len(currently_running) < max_available_slots:
#             available_slots = max_available_slots - len(currently_running)
#             print("available slots", available_slots)
#             for _ in range(min(available_slots, in_queue.qsize())):
#                 ipa_validation_obj = in_queue.get()
#                 ipa_validation_obj_id = ipa_validation_obj["_id"]
#                 if ipa_validation_obj_id not in currently_running:
#                     print(f"ipa_validation_id: {ipa_validation_obj_id}")
#                     process = Process(
#                         target=transformation_run, args=([ipa_validation_obj])
#                     )
#                     process.start()

#                     currently_running[ipa_validation_obj_id] = process

#         time.sleep(0.5)


import time
from queue import Queue
from typing import Set, Dict
from utils.elastic import fetch_ipa_validation
from utils.transformation_run import transformation_run

in_queue: Queue = Queue()
processed_institutes: Set[str] = set()


def auto_run_transformation():
    while True:
        if in_queue.empty():
            ipa_validation_objs = fetch_ipa_validation()
            for ipa_validation_obj in ipa_validation_objs:
                ipa_validation_obj_id = ipa_validation_obj["_id"]
                if ipa_validation_obj_id not in processed_institutes:
                    in_queue.put(ipa_validation_obj)

        if not in_queue.empty():
            ipa_validation_obj = in_queue.get()
            ipa_validation_obj_id = ipa_validation_obj["_id"]
            # print(f"Processing ipa_validation_id: {ipa_validation_obj_id}")

            # Run the transformation synchronously
            # print(ipa_validation_obj)
            transformation_run(ipa_validation_obj)

            processed_institutes.add(ipa_validation_obj_id)

        time.sleep(0.1)
