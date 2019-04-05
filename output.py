import json


class Json:
    __name__ = 'json'

    def __init__(self, fname):
        self.fname = fname

    def start(self, id):
        self.f = open(self.fname.replace('*', str(id)), 'w')

    def __call__(self, data):
        self.f.write(json.dumps(data, ensure_ascii=False))
        self.f.write('\n')

    def end(self):
        print('done!')

json_out = Json  # noqa
