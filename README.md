# Pipes for processing files (pfpf)

This is small experimental toolkit for processing **large text files**
(hundred of gigabytes) line by line through pipeline of parallel workers.

It also provides: 

* real-time statistic for workers
* ability to debug and rerun code for lines which cause exceptions

## How to build pipeline?
Simple example of creating pipeline with 3 workers:
```python
import json
from pfpf import *

def from_json(line):
    return json.loads(line)

def only_events(data):
    if data.get('type') == 'event':
        return data

def get_name(data):
    return data['name']

pipe = pfile('/tmp/test.json') | from_json | only_events | get_name | json_out('/tmp/names.json')
pipe.start()
```

In fact any callable object can be appended to pipeline through `|` operator.

## How exceptions are handled
Exceptions in workers doesn't stop script execution. After processing all
input from `/tmp/test.json` pipeline will save all errors offsets in special `/tmp/test.json.err`
file. This file can be used later to debug and rerun code:
```python
import json
from pfpf import *

...

pipe = perrs('/tmp/test.json.err') | from_json | only_events | get_name | json_out('/tmp/names-fixed.json')
pipe.start()
```
