import json
import logging
from elasticsearch import Elasticsearch

cols = (("alias", "string", True),
        ("hadoop_jobs", "string", True),
        ("index_prod", "string", True),
        ("index_uat", "string", True),
        ("source", "string", True),
        ("index_description", "string", True),
        ("ingestion_logic", "string", True),
        ("index_type", "string", True),
        ("functional_owner", "string", True),
        ("data_owner", "string", True),
        ("technical_owner", "string", True),
        ("current_size", "string", True),
        ("yearly_growth", "string", True),
        ("retention_period", "string", True),
        ("retention_logic", "string", True),
        ("stakeholder", "string", False),
        ("buh", "string", False))


def read_csv(filename):
    with open(filename, "rt", encoding="UTF-8") as f:
        next(f)
        for line in f:
            tup = line.strip().split(",")
            # print(tup)
            record = {}
            for i in range(len(cols)):
                name, type, include = cols[i]
                if tup[i] != "" and include:
                    if type in ("int", "long"):
                        record[name] = int(tup[i])
                    elif type == "double":
                        record[name] = float(tup[i])
                    elif type == "string":
                        record[name] = tup[i]
            return json.dumps(record, ensure_ascii=False)


def connect_elasticsearch():
    _es = None
    _es = Elasticsearch(host="localhost", port=9200,
                        http_auth=("elastic", "elastic"))
    if _es.ping():
        print('Connected to Elasticsearch')
    else:
        print('Could not connect')
    return _es


def create_index(es, index_name):
    created = False
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }
    try:
        if not es.indices.exists(index_name):
            es.indices.create(index=index_name, body=settings)
            created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


def index_data(es, index_name, record):
    try:
        es.index(index=index_name, doc_type="_doc", body=record)
        print('Docs are pushed into the index ', index_name)
    except Exception as ex:
        print(str(ex))


def search(es, index_name, search_body):
    res = es.search(index=index_name, body=search_body)
    return res


if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    es = connect_elasticsearch()
    index_name = 'index_ownership'
    created = create_index(es, index_name)
    if created:
        print('Index Created')
    else:
        print('Skipping Index Creation')
    json_data = read_csv("index_ownership.csv")
    index_data(es, index_name, json_data)
    search_query = {
        'query': {
            'match': {
                'data_owner': 'Nilesh'
            }
        }
    }
    search_result = search(es, index_name, json.dumps(search_query))
    print(search_result)


