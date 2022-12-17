import pandas as pd
import numpy as np

import pipeline_util as plu

def combine_subnorm_auths(pl):
    main_sns = pl.subnorm_list
    main_sns.remove("other")
    subnorm_dfs = [pd.read_pickle(pl.get_weighted_snauth_fpath(sn)) for sn in main_sns]
    full_df = pd.concat(subnorm_dfs, axis=0, ignore_index=True)
    plu.safe_to_pickle(full_df, pl.get_weighted_auth_fpath(extension="pkl"))