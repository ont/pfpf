import types
from tqdm import tqdm
from multiprocessing import Process


class Processor(Process):
    """Wrapper around callable"""

    def __init__(self, id, func, qin, qout, qerr):
        Process.__init__(self)
        self.id = id
        self.func = func
        self.qin = qin
        self.qout = qout
        self.qerr = qerr

    def run(self):
        while True:
            pair = self.qin.get()
            if not pair:
                self.qout.put(None)  # send stop signal further
                break

            pos, data = pair

            try:
                res = self.func(data)
                if not res:
                    # res = data
                    continue  # data was filtered out

                if isinstance(res, types.GeneratorType):
                    # multiple result for single input
                    for sres in res:
                        self.qout.put((pos, sres))
                else:
                    self.qout.put((pos, res))

            except Exception as e:
                tqdm.write('[!] %s' % repr(e))
                tqdm.write('[>] %s' % repr(data))
                tqdm.write('')
                self.qerr.put((self.id, pos))  # send error position back

        if getattr(self.func, 'end', None):
            self.func.end()  # call end on object

    def func_name(self):
        """Returns name of callable"""
        names = [
            getattr(self.func, '__name__', None),
            type(self.func).__name__
            # repr(self.func)
        ]
        for name in names:
            if name:
                return name
