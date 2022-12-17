import pandas as pd
import numpy as np

import pipeline_util as plu

def sum_contract_level(article_df):
    contract_df = article_df.groupby(["contract_id"]).sum()
    return contract_df

def sum_weighted_auths(pl):
    # Load the weighted auths from the previous step
    article_df = pd.read_pickle(pl.get_weighted_auth_fpath(extension="pkl"))
    # Sum up to contract level
    contract_df = sum_contract_level(article_df)
    plu.safe_to_stata(contract_df, pl.get_contract_auth_fpath(extension="dta"))
    plu.safe_to_pickle(contract_df, pl.get_contract_auth_fpath(extension="pkl"))
    plu.safe_to_csv(contract_df, pl.get_contract_auth_fpath(extension="csv"))