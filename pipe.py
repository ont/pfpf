import os
import pickle
from tqdm import tqdm
from processor import Processor
from threading import Thread
from multiprocessing import Queue
from stats import Stat


class Pipe:
    """Base class for DSL"""

    def __init__(self, stat=Stat):
        self.queue = Queue(1000)
        self.qerr = Queue()  # WARN: unlimited queue for errors
        self.procs = []
        self.errs = []
        self.stat = Stat()

    def __or__(self, func):
        if not callable(func):
            raise Exception('%s is not callable' % func)

        qin = self.procs[-1].qout if self.procs else self.queue
        proc = Processor(len(self.procs), func, qin, Queue(1000), self.qerr)

        self.procs.append(proc)

        # TODO: return cloned version of self (immutable pipe). It will allow:
        #       subpipe = pfile(...) | some | route
        #       pipe1 = subpipe | route1
        #       pipe2 = subpipe | route2
        return self

    def start(self):
        for proc in self.procs:
            proc.start()

        ehandler = Thread(target=self.handle_errors)
        ehandler.start()

        self.run()

        for proc in self.procs:
            self.queue.put(None)

        for proc in self.procs:
            proc.join()

        self.qerr.put(None)  # stop error handler thread

        self.end()

    def handle_errors(self):
        while True:
            pair = self.qerr.get()

            if not pair:
                break

            id, pos = pair

            self.errs.append(pos)

            self.stat.err(self.procs[id].func_name())

    def process(self, pos, data):
        """Must be called by run()"""
        self.queue.put((pos, data))
        self.stat.inc()

    def run(self):
        # call process(line) here for each line
        raise NotImplementedError

    def end(self):
        # display statistic here, process errors
        raise NotImplementedError


class FilePipe(Pipe):
    def __init__(self, fname, strip=True):
        Pipe.__init__(self)
        self.fname = fname
        self.strip = strip

    def run(self):
        with open(self.fname, 'rb') as f:
            seek = 0
            with tqdm(total=os.path.getsize(self.fname)) as pbar:
                for n, line in enumerate(f):
                    if n > 0 and n % 100000 == 0:
                        self.stat.print()

                    sline = line.strip() if self.strip else line
                    self.process(seek, sline)
                    seek += len(line)  # NOTE: length of original line!
                    pbar.update(seek)

    def end(self):
        path = self.fname + '.err'

        if self.errs:
            with open(path, 'wb') as f:
                pickle.dump((
                    self.fname, self.strip, self.errs
                ), f, protocol=pickle.HIGHEST_PROTOCOL)

        self.stat.print()


class ErrsPipe(Pipe):
    def __init__(self, fname, strip=True):
        Pipe.__init__(self)
        self.fname = fname
        self.strip = strip

    def run(self):
        with open(self.fname, 'rb') as ferrs:
            fname, strip, poses = pickle.load(ferrs)

            with open(fname, 'rb') as flines:
                for pos in poses:
                    flines.seek(pos)
                    line = flines.readline()
                    if strip:
                        line = line.strip()

                    self.process(pos, line)

    def end(self):
        self.stat.print()


pfile = FilePipe
perrs = ErrsPipe
