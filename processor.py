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
                    res = data

                self.qout.put((pos, res))
            except Exception:
                self.qerr.put((self.id, pos))  # send error position back

    def func_name(self):
        """Returns name of callable"""
        return getattr(self.func, '__name__', repr(self.func))
