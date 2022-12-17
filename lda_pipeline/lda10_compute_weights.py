import codecs
import csv
import itertools

import gensim
from joblib import Parallel, delayed
import pandas as pd
import numpy as np

import pipeline_util as plu

def compute_chunk_weights(doc_chunk, lda_model, num_topics):
    ## The function to be run in parallel.
    chunk_weights = [lda_model[branch_vec] for branch_vec in doc_chunk]
    ## Convert to a dictionary [topic->weight]
    weight_dicts = [{elt[0]:elt[1] for elt in branch_weights} for branch_weights in chunk_weights]
    ## Export the weights in the appropriate csv "slots". Important to note here
    ## that lda[bag_of_words] only returns topic probabilities if they're ABOVE
    ## some minimum probability. So if a topic doesn't show up in the list, then
    ## we can assume the doc's probability weight for that topic is (approx) 0.
    weight_lists = [[weight_dict[n] if (n in weight_dict) else 0 for n in range(num_topics)] for weight_dict in weight_dicts]
    chunk_rows = [weight_list for weight_list in weight_lists]
    return chunk_rows

def compute_weights(pl):
    pl.iprint("compute_weights()")
    num_topics = pl.num_topics
    ## Load the corpus
    corpus_fpath = pl.get_lda_corpus_fpath()
    corpus = gensim.corpora.MmCorpus(corpus_fpath)
    ## Load the LDA model
    lda_model_fpath = pl.get_lda_model_fpath()
    lda_model = gensim.models.ldamulticore.LdaMulticore.load(lda_model_fpath)
    
    # Loop through <dataset>_object_branches.csv and generate the topic weights
    # for each row, then output to <dataset>_branch_weights.csv
    # Update: load 1million lines at a time (to not break RAM) and process the
    # million lines in parallel via joblib
    # UPDATE: loop over the pre-processed .mm file to avoid re-doing the doc2bow
    # transformations
    
    # Use multiprocessing to process the batch in parallel
    batch_size = 3000
    corpus_chunks = (corpus[i:i+batch_size] for i in range(0,len(corpus),batch_size))
    par_obj = Parallel(n_jobs=7, verbose=5)
    result_chunks = par_obj(delayed(compute_chunk_weights)(cur_chunk, lda_model, num_topics) for cur_chunk in corpus_chunks)
    # Now flatten result_chunks
    result_list = list(itertools.chain(*result_chunks))
    result_df = pd.DataFrame(result_list)
    result_df.columns = ["topic_weight_" + str(tnum) for tnum in range(pl.num_topics)]
    # And save to file
    weights_pkl_fpath = pl.get_lda_weights_fpath(extension="pkl")
    plu.safe_to_pickle(result_df, weights_pkl_fpath)
    pl.iprint("Saved weights data to " + str(weights_pkl_fpath))
    # And csv (slower)
    weights_csv_fpath = pl.get_lda_weights_fpath(extension="csv")
    plu.safe_to_csv(result_df, weights_csv_fpath)
    
    # OLD: serial processing
    #for branch_data in streamObjectBranches(include_ids=True):
    #  if row_num % 100 == 0:
    #    print("***** Processing row #" + str(row_num))
    #  cur_branch = branch_data["object_branch"]
    #  branch_bow = dictionary.doc2bow(doc2tokens(cur_branch))
    #  branch_weights = lda[branch_bow]
    #  # Convert to a dictionary [topic->weight]
    #  weight_dict = {elt[0]:elt[1] for elt in branch_weights}
    #  # Export the weights in the appropriate csv "slots". Important to note here
    #  # that lda[bag_of_words] only returns topic probabilities if they're ABOVE
    #  # some minimum probability. So if a topic doesn't show up in the list, then
    #  # we can assume the doc's probability weight for that topic is (approx) 0.
    #  weight_list = [weight_dict[n] if (n in weight_dict) else 0 for n in range(num_topics)]
    #  cur_row = [branch_data["contract_id"],branch_data["section_num"],branch_data["sentence_num"],branch_data["statement_num"],cur_branch] + weight_list
    #  appendRow(cur_row)
    #  row_num += 1