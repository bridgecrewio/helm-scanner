"""
Multithread or Multiprocess Engine
================================


"""

import concurrent.futures 
import math
import os

def multiprocessit(
    func, key, list, num_of_workers = None
):
    if not num_of_workers:
        num_of_workers = math.ceil(os.cpu_count() / 2)
        print(f"Creating {num_of_workers} workers from {os.cpu_count()}")
    if num_of_workers > 0:
        with concurrent.futures.ProcessPoolExecutor(max_workers=num_of_workers) as executor:
            futures = {executor.submit(func, key, item): item for item in list}
            wait_result = concurrent.futures.wait(futures)
            if wait_result.not_done:
                raise Exception(f"failed to perform {func.__name__}")
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    raise e

def multithreadit(
    func, key, list, num_of_workers = None
):
    if not num_of_workers:
        num_of_workers = math.ceil(os.cpu_count() / 2)
        print(f"Creating {num_of_workers} workers from {os.cpu_count()}")
    if num_of_workers > 0:
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_of_workers) as executor:
            futures = {executor.submit(func, key, item): item for item in list}
            wait_result = concurrent.futures.wait(futures)
            if wait_result.not_done:
                raise Exception(f"failed to perform {func.__name__}")
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    raise e
