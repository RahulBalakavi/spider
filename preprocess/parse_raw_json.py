import glob
import os, sys
import json
import sqlite3
import traceback
import argparse

import pandas

from process_sql import get_sql
import itertools

import nltk

nltk.download('punkt')

# TODO: update the following dirs
sql_path = 'spider/train.json'
db_dir = 'database/'
output_file = 'dev_new_with_values.json'
table_file = 'spider/tables.json'


class Schema:
    """
    Simple schema which maps table&column to a unique identifier
    """

    def __init__(self, schema, table):
        self._schema = schema
        self._table = table
        self._idMap = self._map(self._schema, self._table)

    @property
    def schema(self):
        return self._schema

    @property
    def idMap(self):
        return self._idMap

    def _map(self, schema, table):
        column_names_original = table['column_names_original']
        table_names_original = table['table_names_original']
        # print 'column_names_original: ', column_names_original
        # print 'table_names_original: ', table_names_original
        for i, (tab_id, col) in enumerate(column_names_original):
            if tab_id == -1:
                idMap = {'*': i}
            else:
                key = table_names_original[tab_id].lower()
                val = col.lower()
                idMap[key + "." + val] = i

        for i, tab in enumerate(table_names_original):
            key = tab.lower()
            idMap[key] = i

        return idMap


def get_schemas_from_json(fpath):
    with open(fpath) as f:
        data = json.load(f)
    db_names = [db['db_id'] for db in data]

    tables = {}
    schemas = {}
    for db in data:
        db_id = db['db_id']
        schema = {}  # {'table': [col.lower, ..., ]} * -> __all__
        column_names_original = db['column_names_original']
        column_names = []
        for item in column_names_original:
            column_names.append(item[1].strip(" \' ").lower())
        table_names_original = db['table_names_original']
        column_types = db['column_types']
        tables[db_id] = {'column_names_original': column_names_original,
                         'table_names_original': table_names_original,
                         'column_types_for_name': {k: v for k, v in
                                                   zip(column_names,
                                                       column_types)}}

        for i, tabn in enumerate(table_names_original):
            table = str(tabn.lower())
            cols = [str(col.lower()) for td, col in column_names_original if
                    td == i]
            schema[table] = cols
        schemas[db_id] = schema

    return schemas, db_names, tables
