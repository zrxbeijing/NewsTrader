"""
Adapted from https://leimao.github.io/blog/Python-tqdm-Multiprocessing/
"""

from tqdm import tqdm
from multiprocessing import Pool
from multiprocessing import set_start_method
from multiprocessing.pool import ThreadPool


def run_multitasking(func, argument_list, num_workers, thread_or_process):
    """
    Run async multiprocessing and show the progress.
    Note that this function may encounter some problem in MacOS
    :param func: function to be executed
    :param argument_list: argument for the function func
    :param num_workers: number of workers
    :param thread_or_process: run multiprocessing or multithreading
    :return: processing results of the function func
    """
    set_start_method('spawn')
    if thread_or_process == 'thread':
        pool = ThreadPool(processes=num_workers)
    else:
        pool = Pool(processes=num_workers)

    jobs = [pool.apply_async(func=func, args=(*argument,)) if isinstance(argument, tuple)
            else pool.apply_async(func=func, args=(argument,)) for argument in argument_list]
    pool.close()
    result_list_tqdm = []
    for job in tqdm(jobs):
        result_list_tqdm.append(job.get())

    return result_list_tqdm
