from multiprocessing import Process
import time

def count(n):
    total = 0
    for i in range(n):
        total += i

def work():
    count(100_000_000)

if __name__ == '__main__':
    start = time.time()

    processes = []
    for i in range(2):
        p = Process(target = work)
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    end = time.time()

    print(end - start)
