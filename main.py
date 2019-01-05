from Celery import celery
from multiprocessing import Process

current_supplier = "Richner"

if __name__ == '__main__':
  num_workers = 1
  worker_processes = []

  for i in range(num_workers):
    worker_process = Process(target=celery.run_worker)
    worker_process.start()
    worker_processes.append(worker_process)
  
  beat_process = Process(target=celery.run_beat)
  beat_process.start()
  
  for worker_process in worker_processes:
    worker_process.join()

  beat_process.join()
