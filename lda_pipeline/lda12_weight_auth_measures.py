import itertools

import pandas as pd
import numpy as np

import pipeline_util as plu

def weight_auth_measures(pl):
    pl.iprint("weight_auth_measures()")
    # Load the weights
    weight_df = pd.read_pickle(pl.get_lda_weights_fpath(extension="pkl"))
    # Load the unweighted auth measures

    # that's the wrong path
    #auth_df = pd.read_pickle(pl.get_subnorm_auth_fpath(extension="pkl"))

    # this one works
    auth_df = pd.read_pickle(pl.get_snauth_statement_fpath(extension="pkl"))
    # Make sure contract_id and article_num are "real" variables, not index vars
    auth_df.reset_index(inplace=True)
    # For memory purposes, we need to drop ALL the vars besides the auth measures
    main_sns = pl.subnorm_list
    print(main_sns)
    if "other" in main_sns:
        main_sns.remove("other")
    auth_list = pl.AUTH_MEASURES
    auth_var_pairs = itertools.product(main_sns, auth_list)
    vars_to_keep = [sn + "_" + auth for (sn,auth) in auth_var_pairs]
    vars_to_keep.extend(["contract_id","article_num"])
    print ("columns", auth_df.columns)
    print ("vars to keep", vars_to_keep)
    auth_df = auth_df[vars_to_keep]
    # this is SO memory inefficient, question is: optimize or not?
    for cur_topic_num in range(pl.num_topics):
        print (cur_topic_num)
        cur_topic_str = str(cur_topic_num)
        pl.iprint("Processing topic #" + cur_topic_str)
        cur_topic_var = "topic_weight_" + cur_topic_str
        cur_topic_weights = weight_df[cur_topic_var]
        # (Also copy it into auth_df, since we'll need it later)
        auth_df[cur_topic_var] = cur_topic_weights
        for cur_subnorm in main_sns:
            print (cur_subnorm)
            pl.iprint("Subnorm: " + str(cur_subnorm))
            cur_ob_var = cur_subnorm + "_obligation"
            cur_const_var = cur_subnorm + "_constraint"
            cur_perm_var = cur_subnorm + "_permission"
            cur_ent_var = cur_subnorm + "_entitlement"
            # Make combined versions
            cur_obconst_var = cur_subnorm + "_obconst"
            auth_df[cur_obconst_var] = auth_df[cur_ob_var] + auth_df[cur_const_var]
            cur_perment_var = cur_subnorm + "_perment"
            auth_df[cur_perment_var] = auth_df[cur_perm_var] + auth_df[cur_ent_var]
            # Now the actual weighted vars
            cur_weighted_obconst_var = cur_subnorm + "_weighted_obconst_" + cur_topic_str
            auth_df[cur_weighted_obconst_var] = cur_topic_weights * auth_df[cur_obconst_var]
            cur_weighted_perment_var = cur_subnorm + "_weighted_perment_" + cur_topic_str
            auth_df[cur_weighted_perment_var] = cur_topic_weights * auth_df[cur_perment_var]
    pkl_fpath = pl.get_weighted_auth_fpath(extension="pkl")
    pl.iprint("Saving pickle to " + str(pkl_fpath))
    plu.safe_to_pickle(auth_df, pkl_fpath)
    csv_fpath = pl.get_weighted_auth_fpath(extension="csv")
    pl.iprint("Saving csv to " + str(csv_fpath))
    plu.safe_to_csv(auth_df, csv_fpath)
