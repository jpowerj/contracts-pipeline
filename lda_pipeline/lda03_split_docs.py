# Split the full set of docs up into smaller chunks, so corpora can be constructed
# for each chunk (on different AWS instances) and then combined at the end
import pandas as pd
import numpy as np
import gensim

#import aws_api
import pipeline_util as plu

def txt_to_doc(cur_txt, dictionary):
    # Takes in a string containing the plaintext document, and outputs a gensim
    # format doc
    return dictionary.doc2bow(cur_txt.lower().split())

def split_docs(pl):
    pl.iprint("split_docs()")
    # New version: I'm just going to load all the object branches, tokenize them,
    # and pickle them
    # The extra float('inf') element is just to make the loop simpler
    if not pl.num_lda_subsets:
        raise Exception("num_lda_subsets must be specified in the Pipeline "
            + "constructor if you are running the LDA pipeline")
    if pl.lda_on_obj_branches:
        # It's a bit easier in object branch mode, since we just split a big df
        obranch_df_fpath = pl.get_preprocessed_df_fpath()
        pl.iprint("Loading " + str(obranch_df_fpath))
        obranch_df = pd.read_pickle(obranch_df_fpath)
        # And just split this bad boi
        fpath_list = split_and_save(pl, obranch_df)
    else:
        auth_fpath = pl.get_auth_fpath(extension="pkl")
        pl.iprint("Loading " + str(auth_fpath))
        auth_df = pd.read_pickle(auth_fpath)
        # We need the id vars to be NON-index columns, because of Pandas stupidness,
        # so reset_index converts them into "regular" columns here
        auth_df.reset_index(inplace=True)
        vars_to_keep = ["contract_id","article_num","sentence_num","statement_num","full_sentence"]
        auth_df = auth_df[vars_to_keep]
        pl.iprint(str(len(auth_df)) + " rows (statements) in auth file")
        # First "sum" to the sentence level, which really just means dropping duplicate
        # sentences
        sent_df = auth_df.groupby(["contract_id","article_num","sentence_num"]).first()
        del sent_df["statement_num"]
        pl.iprint("Summed to sentence level. Now " + str(len(sent_df)) + " sentences.")
        # Now concatenate the sentences together, "summing" up to the article level
        art_df = sent_df.groupby(["contract_id","article_num"]).agg(lambda col: " ".join(col))
        art_df.rename(index=str, columns={'full_sentence':'full_article'}, inplace=True)
        pl.iprint("Summed to article level. Now " + str(len(art_df)) + " articles.")
        # Preprocess and convert the full_article strings into lists of tokens
        def preprocess_contract(contract_str):
            # First we should remove our custom stopwords
            custom_stopwords = pl.gen_stopwords()
            filtered_str = " ".join(w for w in contract_str.split() if w not in custom_stopwords)
            # Now use gensim's parsing library
            final_str = gensim.parsing.preprocess_string(filtered_str)
            return final_str
        art_df["art_tokens"] = art_df["full_article"].apply(preprocess_contract)
        del art_df["full_article"]
        split_and_save(pl, art_df)
        
def split_and_save(pl, art_df):
    # Now split into a list of dfs, one for each subcorpus, with pl.num_lda_subsets
    # subcorpora total
    fpath_list = []
    df_list = np.array_split(art_df, pl.num_lda_subsets)
    df_lens = [len(df) for df in df_list]
    pl.iprint("DF split into " + str(pl.num_lda_subsets) + " pieces, lengths: " + str(df_lens))
    # Finally, save each one to a separate pickle file
    for doclist_num, cur_doclist in enumerate(df_list):
        output_fpath = pl.get_lda_doclist_fpath(doclist_num)
        plu.safe_to_pickle(cur_doclist, output_fpath)
        pl.iprint("Saved " + str(output_fpath))
        fpath_list.append(output_fpath)
    return fpath_list
