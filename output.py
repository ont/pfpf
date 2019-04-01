import json


class Json:
    __name__ = 'json'

    def __init__(self, fname):
        self.f = open(fname, 'w')

    def __call__(self, data):
        self.f.write(json.dumps(data, ensure_ascii=False))
        self.f.write('\n')

    def end(self):
        print('done!')

json_out = Json  # noqa
