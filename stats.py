from tqdm import tqdm
from collections import Counter, defaultdict


class Stat:
    def __init__(self):
        self.total = 0
        self.cerr = Counter()
        self.cqueue = defaultdict(lambda: (0., 0., 0., 0, 0.))

    def inc(self):
        self.total += 1

    def err(self, id):
        self.cerr[id] += 1

    def qsize(self, id, size):
        smin, smax, smean, cnt, _ = self.cqueue[id]
        self.cqueue[id] = (
            min(size, smin),
            max(size, smax),
            smean * (cnt/(cnt + 1)) + size/(cnt + 1),
            cnt + 1,
            size
        )

    def print(self):
        tqdm.write('-------------------------------')
        self.print_errors()
        self.print_qsizes()

    def print_errors(self):
        tqdm.write('%-20s | %s' % ('function', 'errors'))
        tqdm.write('---------------------+---------')
        for id, cnt in self.cerr.most_common():
            tqdm.write('%-20s | %d' % (id, cnt))

        tqdm.write('Processed lines: %d (%0.2f%% errors)' %
                   (self.total, sum(self.cerr.values())/self.total * 100))
        tqdm.write('')

    def print_qsizes(self):
        tqdm.write('%-20s | %5s %5s %8s %5s' % ('function', 'min', 'max', 'mean', 'now'))
        tqdm.write('---------------------+----------------------------')
        for id, (smin, smax, smean, cnt, now) in self.cqueue.items():
            tqdm.write('%-20s | %5d %5d %8.2f %5d' % (id, smin, smax, smean, now))
        tqdm.write('')
