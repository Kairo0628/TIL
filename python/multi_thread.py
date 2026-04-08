import threading
import time

def count(n):
    total = 0
    for i in range(n):
        total += i

def work():
    count(100_000_000)

start = time.time()

threads = []
for i in range(2): # 스레드 개수, 2개
    t = threading.Thread(target = work)
    threads.append(t)
    t.start()

# 모든 스레드 종료 대기
for t in threads:
    t.join()

end = time.time()

print(end - start)
