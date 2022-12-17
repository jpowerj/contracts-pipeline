import pandas as pd
import numpy as np

import pipeline_util as plu

def construct_subnorm_auths(pl):
    """ Right now we have a statement-level dataset with just "raw" authority
    measure counts. We need to "sum" to the article level, constructing subnorm-
    specific auth measures along the way """
    auth_fpath = pl.get_auth_fpath(extension="pkl")
    auth_df = pd.read_pickle(auth_fpath)
    measures = pl.AUTH_MEASURES
    for cur_subnorm in pl.subnorm_list:
        print("Processing " + str(cur_subnorm))
        # Extract just the rows with subnorm == cur_subnorm
        subnorm_df = auth_df[auth_df["subnorm"] == cur_subnorm]
        rename_dict = {cur_measure: cur_subnorm+"_"+cur_measure for cur_measure in measures}
        subnorm_df.rename(index=str, columns=rename_dict, inplace=True)
        print(subnorm_df.columns)
        # BUT now we have to "sum" to the article level before saving
        sum_df = subnorm_df.groupby(["contract_id","article_num"]).sum()
        plu.safe_to_pickle(sum_df, pl.get_subnorm_auth_fpath(subnorm=cur_subnorm, extension="pkl"))

        ### OLD: fancy lambda functions, but no splitting, so MemoryErrors :(
        #def sn_match(row, subnorm):
        #    return row["subnorm"] == subnorm
        #auth_df[subnorm_ob] = auth_df.apply(lambda row: row["obligation"] if sn_match(row,cur_subnorm) else 0, axis=1)
        #print("ob computed")
        #auth_df[subnorm_con] = auth_df.apply(lambda row: row["constraint"] if sn_match(row,cur_subnorm) else 0, axis=1)
        #print("const computed")
        #auth_df[subnorm_perm] = auth_df.apply(lambda row: row["permission"] if sn_match(row,cur_subnorm) else 0, axis=1)
        #print("perm computed")
        #auth_df[subnorm_ent] = auth_df.apply(lambda row: row["entitlement"] if sn_match(row,cur_subnorm) else 0, axis=1)
        #print("ent computed")