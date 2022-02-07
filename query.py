import pandas_gbq
from google.cloud import bigquery
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS']='/Users/dgoldenberg/Downloads/sa-dwh-code-ds.json'
client = bigquery.Client()

def creating_query(select_cols, table, where_cond=None):
    query = "select %s from %s" %(select_cols, table)
    query = query if where_cond is None else query + ' where ' + where_cond
    return query


assets_query = creating_query("*", "`bi`.`ios_facetune2_asset_info_parsed`")

features_query = creating_query("*", "`ltx-dwh-prod-processed`.`app`.`ft2_uf_sub_features_table`",
                                "date(`editing_session_id_ts`) > cast('2021-11-15' as date)")


# TODO: Set project_id to your Google Cloud Platform project ID.
# project_id = "my-project"

sql = """
SELECT country_name, alpha_2_code
FROM `bigquery-public-data.utility_us.country_code_iso`
WHERE alpha_2_code LIKE 'A%'
"""
df = pandas_gbq.read_gbq(sql)