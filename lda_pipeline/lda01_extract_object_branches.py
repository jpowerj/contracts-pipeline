# Uses gensim to stream the statements from <dataset>_all_statements.csv file
# and topic model the modal object branches
import os
import ast
import csv
import pandas as pd
import numpy as np

import pipeline_util as plu

# [Removed 2019-03-03 -JJ]
#def append_to_corpus(row):
#    corpus_fpath = os.path.join(OUTPUT_PATH, CORPUS_NAME + "_object_branches.csv")
#    with open(corpus_fpath, "a") as f:
#        writer = csv.writer(f)
#        writer.writerow(row)
  
def extract_object_branches(pl):
    # Basically loop over the object_branches column and transform each entry from
    # a weird array into a document ready to be fed into gensim, along with the
    # obligation_constraint and permission_entitlement vars
    # So normally here we should load the pdata pkl file


    # this file here does not exist, it's a directory of pkl files...
    #pdata_pkl_fpath = pl.get_pdata_fpath()

    #if os.path.isfile(pdata_pkl_fpath):
    #    pdata_df = pd.read_pickle(pdata_pkl_fpath)
    pdata_pkl_fpath = pl.get_pdata_path()
    pl.iprint("Loading pdata file " + str(pdata_pkl_fpath))

    if os.path.isdir(pdata_pkl_fpath):
        for chunk_num, cur_chunk_df in pl.stream_pdata():
            if chunk_num == 0:
                pdata_df = cur_chunk_df
                pdata_df = pdata_df[["contract_id","article_num","sentence_num",
                          "statement_num","object_branches"]]
            else:
                cur_chunk_df = cur_chunk_df[["contract_id","article_num","sentence_num",
                          "statement_num","object_branches"]]
                pdata_df = pd.concat([pdata_df, cur_chunk_df], ignore_index=True)
    else:
        # But for "backwards compatibility" with the version without pkls
        pdata_csv_fpath = pdata_pkl_fpath[:-4] + ".csv"
        pdata_df = pd.read_csv(pdata_csv_fpath)
    #pl.iprint("pdata_df loaded. Columns: " + str(pdata_df.columns))
    object_df = pdata_df[["contract_id","article_num","sentence_num",
                          "statement_num","object_branches"]]
    object_df = pdata_df
    pl.dprint(object_df.head(n=20))
    num_obj_branches = len(object_df)
    # Transforms the object_branches string into a single space-separated string
    def parse_obj_branch(obj_branch_str):
        # this line converts a string like "[['a','b'],['c']]" to a Python list
        # this is not needed anymore, the items in the pkl file are already lists
        #branch_list = ast.literal_eval(obj_branch_str)
        all_words = [' '.join(word_list) for word_list in obj_branch_str]
        all_words_str = ' '.join(all_words)
        return all_words_str
    # Convert the object_branches string into a Python list
    object_df["obj_branch_doc"] = object_df["object_branches"].apply(parse_obj_branch)
    del object_df["object_branches"]
    # But now we need to "sum" (concatenate) up to the article level
    # First to the sentence level. agg(lambda col: " ".join(col)) is crucial here
    sent_df = object_df.groupby(["contract_id","article_num","sentence_num"]).agg(lambda col: " ".join(col))
    sent_df.reset_index(inplace=True)
    # And then the article level
    art_df = sent_df.groupby(["contract_id","article_num"]).agg(lambda col: " ".join(col))
    art_df.reset_index(inplace=True)
    # Now we have a df of object branches at the article level!
    obranch_pkl_fpath = pl.get_obranch_fpath(extension="pkl")
    obranch_csv_fpath = pl.get_obranch_fpath(extension="csv")
    pl.iprint("Object branches parsed. Saving " + str(obranch_pkl_fpath))
    plu.safe_to_pickle(art_df, obranch_pkl_fpath)
    plu.safe_to_csv(art_df, obranch_csv_fpath)
    
    ### No more loops
    #for row_num, row_data in object_df.iterrows():
    #    if row_num % 1000:
    #        print("*** Processing object branch #" + str(row_num) + " of " + str(num_obj_branches))
    #    branch_str = row_data["object_branches"]
    #    #object_df.set_value(row_num, "object_text", all_words_str)
    #    cur_row = [row_data["contract_num"],row_data["section_num"],
    #        row_data["sentence_num"],row_data["statement_num"],all_words_str]
    #    appendToCorpus(cur_row)

# For testing only
#if __name__ == "__main__":
#    #os.chdir(statements_dir)
#    #if not (os.path.isfile("./canadian_eng_worker.csv") and os.path.isfile("./canadian_eng_manager.csv")):
#    #    cleanStatements()
#    
#    #generate_corpus()
