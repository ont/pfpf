from tqdm import tqdm
from collections import Counter
from multiprocessing import Value


class Stat:
    def __init__(self):
        self.total = Value('L')
        self.cerr = Counter()

    def inc(self):
        with self.total.get_lock():
            self.total.value += 1

    def err(self, id):
        self.cerr[id] += 1

    def print(self):
        tqdm.write('-------------------------------')
        self.print_errors()

    def print_errors(self):
        tqdm.write('%-20s | %s' % ('function', 'errors'))
        tqdm.write('---------------------+---------')
        for id, cnt in self.cerr.most_common():
            tqdm.write('%-20s | %d' % (id, cnt))

        tqdm.write('Processed lines: %d (%0.2f%% errors)' % (
            self.total.value, sum(self.cerr.values())/self.total.value * 100
        ))
        tqdm.write('')
