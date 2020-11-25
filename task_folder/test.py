import json
import os
import sys
import jsonschema
from jsonschema import Draft3Validator
import logging
from jsonschema import validate
from os import walk
from jsonschema.validators import Draft7Validator
import string
from jsonschema.exceptions import SchemaError, ValidationError

logging.basicConfig(handlers=[logging.FileHandler('log.log', 'w', 'utf-8')],
                    level=logging.DEBUG, format='%(asctime)s %(name)s %(levelname)s:%(message)s')

fileDir = os.path.dirname(os.path.realpath('__file__'))
path_schema = os.path.join(fileDir, 'task_folder/schema')
path_json = os.path.join(fileDir, 'task_folder/event')


def matchJsonToSchema(path_json):
    all_json = []
    events = {}
    for (_, _, filenames) in walk(path_json):
        all_json.extend(filenames)

    for i in range(len(all_json)):
        with open(os.path.join(path_json, str(all_json[i])), 'r') as f:
            data = f.read()
            data_json = json.loads(data)
            try:
                events[all_json[i]] = data_json["event"]+'.schema'
            except TypeError as e:
                logging.error(f'{all_json[i]} невалидный')
                logging.error(
                    "Несоответствие типов, ожидали данные, получили null")
            except KeyError as e:
                logging.error(f'{all_json[i]} невалидный')
                logging.error(
                    "Файл пуст или в нем отсутствует нужный ключ \"event\"")
    return events


def contains_whitespace(s):
    return True in [c in s for c in string.whitespace]


def load(json_name, schema_name):
    try:
        with open(os.path.join(path_schema, str(schema_name)), 'r') as f:
            data_value = f.read()
            schema_json = json.loads(data_value)
    except FileNotFoundError as err:
        logging.error(f'{schema_name} невалидный')
        logging.warning(
            f'{json_name} имеет невалидную ссылку на схему')
        if contains_whitespace(schema_name):
            try:
                with open(os.path.join(path_schema, str(schema_name.replace(' ', ''))), 'r') as f:
                    data_value = f.read()
                    schema_json = json.loads(data_value)
                    logging.warning(
                        f'{json_name} имеет невалидную ссылку на схему')
            except FileNotFoundError as err:
                logging.error(f'{schema_name} таки невалидный')
                logging.error(
                    f'{schema_name}, к которому обращается файл ' f'{json_name}, таки точно не существует')
        return ()

    else:
        try:
            with open(os.path.join(path_json, str(json_name)), 'r') as f:
                data_key = f.read()
                json_data = json.loads(data_key)
        except FileNotFoundError as err:
            logging.error(f'{json_name} невалидный')
        finally:
            return (json_data["data"], schema_json)


def compareSchema(path_schema, events_dict):

    for key, value in events_dict.items():
        logging.info('NEWFILE>>>>>>   '+key)
        data = load(key, value)
        if len(data) != 0:
            validateJson(data[0], data[1])
        logging.info('ENDFILE<<<<<<   '+key)


def consumeError(errors):
    for error in errors:
        print(list(error.schema_path), error.message, sep=", ")


def validateJson(jsonData, schema):
    v = Draft7Validator(schema)
    errors = sorted(v.iter_errors(jsonData), key=lambda e: e.path)
    for error in errors:
        logging.error(error.message)


if __name__ == '__main__':
    q = matchJsonToSchema(path_json)
    compareSchema(path_schema, q)
