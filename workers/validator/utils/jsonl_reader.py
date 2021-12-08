import json
from .gzip_utils import open_for_read

def each_in_json_array(filename, open_kwargs={}):
    with open_for_read(filename, **open_kwargs) as f:
        dataset = json.loads(f.read())
        for record in dataset:
            yield record

def each_in_jsonl(filename, open_kwargs={}):
    with open_for_read(filename, **open_kwargs) as f:
        for line in f:
            yield json.loads(line)

def yield_in_batches(generator, batch_size):
    batch = []
    for obj in generator:
        batch.append(obj)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if len(batch) > 0:
        yield batch
