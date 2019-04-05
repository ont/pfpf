from tqdm import tqdm
from multiprocessing import Process


class Chain(Process):
    def __init__(self, id, stat, procs, fname, start, end):
        Process.__init__(self)
        self.id = id
        self.stat = stat
        self.procs = procs

        self.src = open(fname, 'rb')

        self.seek_start = start
        self.seek = start
        self.seek_end = end
        self.src.seek(start)

    def align(self):
        if self.seek_start != 0:
            self.src.readline()  # move to beginning of next line
            self.seek_start = self.src.tell()
            self.seek = self.seek_start
        return self.seek_start

    def run(self):
        self.prepare_procs()
        with tqdm(total=self.seek_end - self.seek, position=self.id) as pbar:
            while self.seek < self.seek_end:
                line = self.src.readline()
                self.process(self.seek, line)
                self.seek += len(line)
                pbar.update(len(line))

        for proc in self.procs:
            proc.end()

    def prepare_procs(self):
        for proc in self.procs:
            proc.start(self.id)

    def process(self, pos, data):
        input, output = [(pos, data)], []
        for proc in self.procs:
            for pos, data in input:
                for pos, odata in proc.process(pos, data):
                    output.append((pos, odata))

            input, output = output, []

        self.stat.inc()

    def __repr__(self):
        return 'Chain<%d>' % self.id
