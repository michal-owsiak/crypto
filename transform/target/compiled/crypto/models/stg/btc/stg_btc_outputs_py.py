import pandas as pd
import simplejson

def model(dbt, session):

    dbt.config(
        materialized='incremental',
        packages=[
            'pandas',
            'simplejson'
        ]
    )

    df = dbt.ref('stg_btc_base').to_pandas()

    df['OUTPUTS'] = df['OUTPUTS'].apply(simplejson.loads)
    df_exploded = df.explode('OUTPUTS').reset_index(drop=True)
    df_outputs = pd.json_normalize(df_exploded['OUTPUTS'])[['address', 'value']]

    df_final = pd.concat([df_exploded.drop(columns=['OUTPUTS']), df_outputs], axis=1)
    df_final = df_final[df_final['address'].notnull()]
    df_final.columns = [col.upper() for col in df_final.columns]

    return df_final


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args, **kwargs):
    refs = {"stg_btc_base": "CRYPTO.DBT_MICOWS.stg_btc_base"}
    key = '.'.join(args)
    version = kwargs.get("v") or kwargs.get("version")
    if version:
        key += f".v{version}"
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {}
    key = '.'.join(args)
    return dbt_load_df_function(sources[key])


config_dict = {}


class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = "CRYPTO"
    schema = "DBT_MICOWS"
    identifier = "stg_btc_outputs_py"
    
    def __repr__(self):
        return 'CRYPTO.DBT_MICOWS.stg_btc_outputs_py'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = True

# COMMAND ----------


