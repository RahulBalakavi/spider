import itertools
import json
import sqlite3
import traceback

import pandas

from preprocess.parse_raw_json import get_schemas_from_json
from preprocess.parse_sql_one import Schema
from process_sql import get_sql

# TODO(Rahul): Move to args.
SQL_PATH = 'dataset/train.json'
OUTPUT_FILE = 'dataset/dataset.json'
TABLE_FILE = 'dataset/tables.json'
SQLITE_DB_BASE_PATH = "/Users/rahul.balakavi/Downloads/spider/database/"
LIMIT_ROWS = 50


def get_col_list(connection, tbl_name):
    return [fields[1].lower() for fields in
            connection.execute(
                "PRAGMA table_info(" + tbl_name + ");").fetchall()]


def populate_column_values(db_name, col_names):
    # print('db-name = ' + db_name)
    sqlite_db_path = (SQLITE_DB_BASE_PATH + "{}/{}.sqlite").format(db_name,
                                                                   db_name)
    con = sqlite3.connect(sqlite_db_path)
    cur = con.cursor()

    # Fetch tables in the db.
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cur.fetchall()
    col_name_to_values = {}
    for table_name in tables:
        table_name_str = table_name[0]
        # print("fetching column values from table " + table_name_str)
        col_name_list_from_table = get_col_list(con, table_name_str)
        cur.execute("select {} from {} limit {}".format(
            (', '.join('"' + item + '"' for item in col_name_list_from_table))
            , table_name_str, LIMIT_ROWS))
        df = pandas.DataFrame(cur.fetchall(), columns=col_name_list_from_table)
        # print(df)
        col_name_to_values.update(df.to_dict('list'))

    column_values = []
    for col_name in col_names:
        column_values.append(col_name_to_values[col_name.strip("\' ")])
    return column_values


def translate_spider_dataset_to_json():
    schemas, db_names, tables = get_schemas_from_json(TABLE_FILE)

    with open(SQL_PATH) as inf:
        sql_data = json.load(inf)

    translated_dataset = []
    for data in sql_data:
        dataset_entry = {}
        try:
            db_id = data["db_id"]
            sql = data["query"]
            schema = schemas[db_id]
            question = data["question"]
            # print("db_id: ", db_id)
            # print("tables in db = ", schema.keys())
            # print("columns in db = ", schema.values())
            dataset_entry["columns"] = list(
                set(itertools.chain.from_iterable(schema.values())))
            col_types = []
            col_types_for_dbid = tables[db_id]['column_types_for_name']
            # print(tables[db_id]['column_types_for_name'])
            for col_name in dataset_entry["columns"]:
                col_name_stripped = col_name.strip(" \' ").lower()
                # print("Stripped col name : ", col_name)
                col_types.append(col_types_for_dbid[col_name_stripped])
            dataset_entry["col_types"] = col_types
            # print(dataset_entry["columns"])
            # print(col_types)
            table = tables[db_id]
            schema_obj = Schema(schema, table)

            sql_label = get_sql(schema_obj, sql)
            dataset_entry["query"] = data["query"]
            dataset_entry["question"] = data["question"]
            dataset_entry["db_id"] = data["db_id"]
            parsed_sql = " ".join(str(i) for i in sql_label['modified_sql'])
            parsed_sql = parsed_sql.replace("<TABLE>", db_id)
            dataset_entry["sql"] = parsed_sql
            dataset_entry["column_values"] = populate_column_values(
                db_id, dataset_entry["columns"])

            # print("dataset_entry: ",  dataset_entry)
            translated_dataset.append(dataset_entry)
        except Exception as e:
            print(e)
            print(traceback.format_exc())
            print("db_id: ", db_id)
            print("sql: ", sql)
            print("question: ", question)
    return translated_dataset


def serialize_to_json(translated_dataset):
    with open(OUTPUT_FILE, 'wt') as out:
        json.dump(translated_dataset, out, sort_keys=True, indent=4,
                  separators=(',', ': '))


if __name__ == "__main__":
    dataset = translate_spider_dataset_to_json()
    print("-------------------------------------------------------------------")
    print(f"Serializing {len(dataset)} entries to json file {dataset}")
    serialize_to_json(dataset)
