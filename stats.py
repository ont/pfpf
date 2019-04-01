from tqdm import tqdm
from collections import Counter


class Stat:
    def __init__(self):
        self.total = 0
        self.c = Counter()

    def inc(self):
        self.total += 1

    def err(self, id):
        self.c[id] += 1

    def print(self):
        tqdm.write('')
        tqdm.write('%-20s | %s' % ('function', 'errors'))
        tqdm.write('---------------------+---------')
        for id, cnt in self.c.most_common():
            tqdm.write('%-20s | %d' % (id, cnt))

        tqdm.write('Processed lines: %d (%0.2f%% errors)' %
                   (self.total, sum(self.c.values())/self.total * 100))
        tqdm.write('')
