# Uses gensim instead of spacy to run the topic model
import codecs
import csv
import os
from six import iteritems
import string

import gensim
import joblib
import pandas as pd
import numpy as np

import pipeline_util as plu

#from global_functions import (debugPrint, doc2tokens, genStopwords, safePickle,
#    streamObjectBranches)
#from global_vars import LDA_DICT_FILEPATH

def construct_dictionary(pl):
    # Initialize the dictionary
    contract_dict = gensim.corpora.Dictionary()
    if not pl.lda_on_obj_branches:
        # Stream the split contracts as the input (send verbose=True to stream_articles()
        # to have it print the loaded articles one-by-one)
        article_stream = pl.stream_split_contracts()
    else:
        # We can just directly load the .pkl file of the exported df from
        # extract_object_branches
        obranch_docs_fpath = pl.get_obranch_fpath(extension="pkl")
        obranch_df = pd.read_pickle(obranch_docs_fpath)
        article_stream = obranch_df["obj_branch_doc"]

    # Now use this generator to add all the documents with preprocessing
    # We also want to save the preprocessed articles (so we don't have to run
    # through and preprocess them twice) so we accumulate them into a big list
    preprocessed_articles = []
    def stream_preprocessed():
        c = 0
        for cur_article in pl.pbar(article_stream):
            #well, preprocess_doc cannot be found in the codebase, but we have preprocess_text instead which looks reasonable to me
            #article_clean = pl.preprocess_doc(cur_article)
            #if c > 10000:
            #    break
            #c += 1
            article_clean = pl.preprocess_text(cur_article)
            preprocessed_articles.append(article_clean)
            yield article_clean
    contract_dict.add_documents(stream_preprocessed())
    # Final preprocessing step: remove tokens that appear in too many contracts
    if pl.remove_top_n > -1:
        contract_dict.filter_n_most_frequent(pl.remove_top_n)
    dict_fpath = pl.get_lda_dict_fpath()
    plu.safe_dict_save(contract_dict, dict_fpath)
    # And save the list of preprocessed docs as well
    joblib.dump(preprocessed_articles, pl.get_preprocessed_fpath())
    # AND if we're working with the object branches, add it as a column in the
    # original object branch df and save the new version as well
    if pl.lda_on_obj_branches:
        obranch_df["doc_clean"] = preprocessed_articles
        plu.safe_to_pickle(obranch_df, pl.get_preprocessed_df_fpath())
    
    # [OLD: copied from gensim tutorial. Removed 2019-03-04 -JJ]
    ## remove stop words and words that appear only once
    #stoplist = pl.gen_stopwords()
    #stop_ids = [dictionary.token2id[stopword] for stopword in stoplist
    #               if stopword in dictionary.token2id]
    #once_ids = [tokenid for tokenid, docfreq in iteritems(dictionary.dfs) if docfreq == 1]
    #dictionary.filter_tokens(stop_ids + once_ids)  # remove stop words and words that appear only once
    #dictionary.compactify()  # remove gaps in id sequence after words that were removed
    #pl.iprint("Stopwords and rare words removed from LDA dictionary")
    #plu.safe_dict_save(dictionary, dict_filepath)
 
# For testing only 
#if __name__ == "__main__":
#    # Compute dictionary
#    corpus_dict = constructDictionary(streamObjectBranches)
#    saveDictionary(corpus_dict)
