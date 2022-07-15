import functools
import queue
import threading

from candidatos.current import Current
from candidatos.logs import log


def trabajador(queue):
    while True:
        item = queue.get()
        size = queue.qsize()
        log.debug("queue.trabajador", size=size, priority=item.priority)
        res = item.fun()
        queue.task_done()


def main() -> int:
    q = queue.PriorityQueue()
    for _ in range(10):
        threading.Thread(target=functools.partial(trabajador, q), daemon=True).start()
    current = Current(q)
    current.load()
    q.join()
    return 0
