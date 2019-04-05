import types
from tqdm import tqdm


class Processor:
    """Wrapper around callable"""

    def __init__(self, id, func, qerr):
        self.id = id
        self.func = func
        self.qerr = qerr

    def process(self, pos, data):
        try:
            res = self.func(data)
            if not res:
                # res = data
                return  # data was filtered out

            if isinstance(res, types.GeneratorType):
                # multiple result for single input
                for sres in res:
                    yield pos, sres
            else:
                yield pos, res

        except Exception as e:
            tqdm.write('[!] %s: %s' % (self.func_name(), repr(e)))
            tqdm.write('[>] %s: %s' % (self.func_name(), repr(data)))
            tqdm.write('')
            self.qerr.put((self.id, pos))  # send error position back

    def start(self, id):
        if getattr(self.func, 'start', None):
            self.func.start(id)

    def end(self):
        if getattr(self.func, 'end', None):
            self.func.end()

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
