import json
import os
from datetime import datetime


from elasticsearch import Elasticsearch, helpers
from datasets import load_dataset

def get_metadata(lang):
    if lang.startswith("jupyter"):
        lang = "jupyter"
    with open(f"data/metadata/{lang}/data.json") as f:
        c = f.readlines()
    c = [json.loads(i.strip()) for i in c]
    return {dictionary.pop("repo_name"):dictionary for dictionary in c}

es = Elasticsearch(timeout=100, max_retries=10, retry_on_timeout=True, http_compress=True, maxsize=1000)

print(es.ping())

settings = {"index": {"number_of_shards": 30, "number_of_replicas": 0},
            "analysis": {
                "analyzer": {
                    "code_analyzer": {
                        "type": "custom",
                        "tokenizer": "code_tokenizer",
                        "filter": ["lowercase", "asciifolding"]
                    }
                },
                "tokenizer": {
                    "code_tokenizer": {"type": "ngram", "min_gram": 3, "max_gram":3}
                }
            }
}

mappings = {


        "properties" : {
            "content": {"type": "text", "analyzer": "code_analyzer"},
            "language": {"type": "keyword"},
            "username": {"type": "keyword"},
            "repository": {"type": "object", "enabled": False},
            "license": {"type": "keyword"},
            "path": {"type": "object", "enabled": False}
        }
}

es.indices.create(index="bigcode-stack-march-no-pii", mappings=mappings, settings=settings)

def doc_generator():
    languages = os.listdir("./data/stack-march-no-pii")
    for language in ['jupyter-scripts-dedup-filtered']:
        # metadata = get_metadata(language)
        dset = load_dataset("data/stack-march-no-pii", data_dir=language, split="train", streaming=True)
        for row in dset:
            yield {
                "_index": "bigcode-stack-march-no-pii",
                "_source": {
                        "content": row["content"],
                        "language": language,
                        "username": row["max_stars_repo_name"].split("/")[0],
                        "repository":row["max_stars_repo_name"],
                        "license": eval(row["max_stars_repo_licenses"]),
                        "path": row["max_stars_repo_path"]
                        }
            }

print("Now indexing")
now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Current Time =", current_time)
for success, info in helpers.parallel_bulk(es, doc_generator(), thread_count=16, chunk_size=1000, max_chunk_bytes=104857600, queue_size=8):
    if not success:
        print("A document failed:", info)
now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Current Time =", current_time)
