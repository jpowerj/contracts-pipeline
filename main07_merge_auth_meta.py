# Merges the "basic" metadata file <corpus>_meta.csv with the latest
# contract-level authority measure dataset (<corpus_name>_summed.csv)
import pandas as pd
import numpy as np
import os
import pipeline_util as plu

# (In case you're looking for combineSubjectAuths(), it's not here anymore since
# the <corpus_name>_summed.csv file is now produced by main04_compute_auth.py
# -JJ, 2018-11-17)
  
#def merge_text_meta(pl):
def merge_auth_meta(pl):
    pl.dprint("merge_text_meta()")
    # Merges the full contract-level dataset created by generateContractAuth()
    # with the metadata in <corpus_name>_meta.csv
    authsums_pkl_fpath = pl.get_authsums_fpath(extension="pkl")
    pl.iprint("Loading authsums file " + authsums_pkl_fpath)
    authsums_df = pd.read_pickle(authsums_pkl_fpath)
    # And make sure the contract_id is the index (unique key)
    # Fuck it. Pandas is so finicky I'm just gonna ALWAYS ignore the index. So
    # all columns will just be regular columns, and we'll never look at or care
    # about the index column. Rrrrrgh.
    #contract_df.set_index(plu.UID_CONTRACT,inplace=True)
    # Also the contract_id column here sometimes gets loaded as float64, whereas
    # in the other file it gets loaded as int64. Again, rrrrrgh.
    # TODO: figure out why the hell this happens. 264 contracts have some
    # weird hex string as its contract_id
    #authsum_df = authsum_df[~authsum_df.contract_id.str.contains('\xa0')]
    print ("authsums_df", authsums_df)
    #authsums_df["contract_id"] = pd.to_numeric(authsums_df["contract_id"],errors='coerce')
    
    meta_fpath = pl.get_meta_fpath()
    print (meta_fpath)
    #meta_fpath= "/cluster/work/lawecon/Work/dominik/powerparser/data/canadian_meta.csv"
    meta_fpath = os.path.join(pl.get_corpus_path(), pl.get_corpus_name() + "_meta.csv")
    #meta_fpath = "../data_sample/canadian_meta.csv"
    #meta_fpath = "~/Dropbox/ash-macleod-naidu/Canadian-Contracts/data/canadian_meta.csv"
    #meta_fpath = "~/Dropbox/ash-macleod-naidu/Canadian-Contracts/data/canadian_meta.csv"
    pl.iprint("Loading meta file " + str(meta_fpath))
    meta_df = plu.safe_read_data(meta_fpath)
    # contract_id unique key
    #meta_df.set_index(plu.UID_CONTRACT,inplace=True)
    
    # Merge on contract_id
    def split(x):
        return x.split("_")[0]
        try:
            x = x.split("_")[0]
        except:
            pass
        return x
    #for i in authsums_df["contract_id"]:
    #    print (i)
    #    print ( split(i))
    authsums_df["contract_id"] = authsums_df["contract_id"].apply(split)
    print ("authsums_df", authsums_df)
    print ("meta_df", meta_df)
    print (authsums_df)
    print (meta_df)
    #input("")
    merged_df = authsums_df.merge(meta_df, how='inner', on=['contract_id'])
    print (merged_df)
    #input("")
    output_csv_fpath = pl.get_merged_meta_fpath(extension="csv")
    output_pkl_fpath = pl.get_merged_meta_fpath(extension="pkl")
    pl.iprint("Saving " + output_pkl_fpath)
    # Make sure contract is unique id
    #merged_df.set_index(plu.UID_CONTRACT,inplace=True)
    plu.safe_to_csv(merged_df, output_csv_fpath)
    plu.safe_to_pickle(merged_df, output_pkl_fpath)
    # And update the pipeline to reflect that the basic metadata has been
    # successfully merged
    pl.last_merge_csv_fpath = output_csv_fpath
    pl.last_merge_pkl_fpath = output_pkl_fpath
  
### Called by pipeline.py, not run directly
#if __name__ == "__main__":
#    merge_contract_meta()
