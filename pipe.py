import pickle
from processor import Processor
from multiprocessing import Queue
from collections import Counter


class Pipe:
    """Base class for DSL"""

    def __init__(self):
        self.queue = Queue(1000)
        self.qerr = Queue()  # WARN: unlimited queue for errors
        self.procs = []

    def __or__(self, func):
        if not callable(func):
            raise Exception('%s is not callable' % func)

        qin = self.procs[-1].qout if self.procs else self.queue
        proc = Processor(len(self.procs), func, qin, Queue(1000), self.qerr)

        self.procs.append(proc)
        return self

    def start(self):
        for proc in self.procs:
            proc.start()

        self.run()

        for proc in self.procs:
            self.queue.put(None)

        for proc in self.procs:
            proc.join()

        self.end()

    def run(self):
        raise NotImplementedError

    def end(self):
        raise NotImplementedError


class FilePipe(Pipe):
    def __init__(self, fname, strip=True):
        Pipe.__init__(self)
        self.fname = fname
        self.strip = strip
        self.total = 0

    def run(self):
        with open(self.fname, 'rb') as f:
            seek = 0
            for line in f:
                sline = line.strip() if self.strip else line

                self.queue.put((seek, sline))
                seek += len(line)  # NOTE: length of original line!
                self.total += 1

    def end(self):
        path = self.fname + '.err'
        poses = []
        c = Counter()
        while not self.qerr.empty():
            id, pos = self.qerr.get()
            poses.append(pos)
            c[id] += 1

        if poses:
            with open(path, 'wb') as f:
                pickle.dump((
                    self.fname, self.strip, poses
                ), f, protocol=pickle.HIGHEST_PROTOCOL)

            print('%-20s | %s' % ('function', 'errors'))
            print('---------------------+---------')
            for id, cnt in c.most_common():
                print('%-20s | %d' % (self.procs[id].func_name(), cnt))

            print('Processed lines: %d (%0.2f%% errors)' %
                  (self.total, sum(c.values())/self.total * 100))


class ErrsPipe(Pipe):
    def __init__(self, fname, strip=True):
        Pipe.__init__(self)
        self.fname = fname
        self.strip = strip
        self.total = 0

    def run(self):
        with open(self.fname, 'rb') as ferrs:
            fname, strip, poses = pickle.load(ferrs)

            with open(fname, 'rb') as flines:
                for pos in poses:
                    flines.seek(pos)
                    line = flines.readline()
                    if strip:
                        line = line.strip()

                    self.queue.put((pos, line))

    def end(self):
        pass


pfile = FilePipe
perrs = ErrsPipe
