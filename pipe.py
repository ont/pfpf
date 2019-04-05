import os
import time
import pickle
from tqdm import tqdm
from datetime import datetime
from threading import Thread
from multiprocessing import Queue, cpu_count

from .stats import Stat
from .processor import Processor
from .chain import Chain


class Pipe:
    """Base class for DSL"""

    def __init__(self, stat=Stat):
        self.queue = Queue(10000)
        self.qerr = Queue()  # WARN: unlimited queue for errors
        self.chains = []
        self.procs = []
        self.errs = []
        self.stat = Stat()
        self.running = False

    def __or__(self, func):
        if not callable(func):
            raise Exception('%s is not callable' % func)

        proc = Processor(len(self.procs), func, self.qerr)

        self.procs.append(proc)

        # TODO: return cloned version of self (immutable pipe). It will allow:
        #       subpipe = pfile(...) | some | route
        #       pipe1 = subpipe | route1
        #       pipe2 = subpipe | route2
        return self

    def start(self, fname):
        self.running = True
        ehandler = Thread(target=self.handle_errors)
        ehandler.start()

        stat_printer = Thread(target=self.print_stat)
        stat_printer.start()

        size = os.path.getsize(fname)

        # TODO: magic + n
        ncpu = cpu_count() + 4

        chunk = size / ncpu

        tqdm.write('total bytes %d, chunk size %0.2f' % (size, chunk))

        self.chains = []
        for n in range(ncpu):
            chain = Chain(
                n, self.stat, self.procs, fname, int(n*chunk), int((n+1)*chunk)
            )
            self.chains.append(chain)

        for n in range(1, len(self.chains)):
            self.chains[n-1].seek_end = self.chains[n].align()

        for chain in self.chains:
            tqdm.write('worker %d will process bytes from %d to %d ...' % (
                chain.id, chain.seek_start, chain.seek_end
            ))

            chain.start()

        for chain in self.chains:
            chain.join()

        self.qerr.put(None)
        self.running = False

        self.end()

    def handle_errors(self):
        while True:
            pair = self.qerr.get()

            if not pair:
                break

            id, pos = pair

            self.errs.append(pos)

            self.stat.err(self.procs[id].func_name())

    def print_stat(self):
        t = datetime.now()
        while self.running:
            time.sleep(0.1)
            if (datetime.now() - t).seconds > 10:
                self.stat.print()
                t = datetime.now()

    def end(self):
        # display statistic here, process errors
        raise NotImplementedError


class FilePipe(Pipe):
    def __init__(self, fname, strip=True):
        Pipe.__init__(self)
        self.fname = fname
        self.strip = strip

    def start(self):
        Pipe.start(self, self.fname)

    def end(self):
        path = self.fname + '.err'

        if self.errs:
            with open(path, 'wb') as f:
                pickle.dump((
                    self.fname, self.strip, self.errs
                ), f, protocol=pickle.HIGHEST_PROTOCOL)

        print('\n' * cpu_count())
        self.stat.print()


# class ErrsPipe(Pipe):
#     def __init__(self, fname, strip=True):
#         Pipe.__init__(self)
#         self.fname = fname
#         self.strip = strip
#
#     def run(self):
#         with open(self.fname, 'rb') as ferrs:
#             fname, strip, poses = pickle.load(ferrs)
#
#             with open(fname, 'rb') as flines:
#                 for pos in poses:
#                     flines.seek(pos)
#                     line = flines.readline()
#                     if strip:
#                         line = line.strip()
#
#                     self.process(pos, line)
#
#     def end(self):
#         self.stat.print()


pfile = FilePipe
# perrs = ErrsPipe
