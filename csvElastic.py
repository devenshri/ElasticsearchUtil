import json
import logging
from elasticsearch import Elasticsearch
from configparser import ConfigParser

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


def connect_elasticsearch(es_url, es_port, es_username, es_password):
    _es = None
    _es = Elasticsearch(host=es_url, port=es_port,
                        http_auth=(es_username, es_password))
    if _es.ping():
        logging.info('Connected to Elasticsearch')
    else:
        logging.error('Could not connect to Elasticsearch')
        exit()
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
        logging.warn(str(ex))
    finally:
        return created


def index_data(es, index_name, record):
    try:
        es.index(index=index_name, doc_type="_doc", body=record)
        logging.info('Docs are pushed into the index %s', index_name)
    except Exception as ex:
        logging.warn(str(ex))


def search(es, index_name, search_body):
    res = es.search(index=index_name, body=search_body)
    return res


if __name__ == "__main__":

    config = ConfigParser()
    config.read('config.ini')

    log_filename = config['logger']['filename']
    log_level = config['logger']['loglevel']

    es_url = config['elastic']['url']
    es_port = config['elastic']['port']
    es_username = config['elastic']['username']
    es_password = config['elastic']['password']
    es_index = config['elastic']['index']
    csv_file = config['elastic']['csv']

    logging.basicConfig(filename=log_filename, filemode='a',
                        format='%(asctime)s - %(levelname)s:%(message)s', level=log_level)
    
    logging.info('Started')

    es = connect_elasticsearch(es_url, es_port, es_username, es_password)

    created = create_index(es, es_index)
    if created:
        logging.info('Index Created %s', es_index)
    else:
        logging.info('Skipping Index Creation')

    json_data = read_csv(csv_file)
    index_data(es, es_index, json_data)

    search_query = {
        'query': {
            'match': {
                'data_owner': 'Nilesh'
            }
        }
    }
    search_result = search(es, es_index, json.dumps(search_query))
    logging.debug(search_result)
    logging.info('Finished')
