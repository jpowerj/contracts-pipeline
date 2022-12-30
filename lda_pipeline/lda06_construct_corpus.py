# Uses the multiprocessing library joblib to parallelize the construction of
# the corpora.
import codecs
import csv
import itertools
import logging
import os
import pickle
import sys

import pandas as pd
import numpy as np
import gensim
import joblib
from joblib import Parallel, delayed

import pipeline_util as plu

#from global_functions import (debugPrint, genStopwords, loadCorpus, loadDictionary,
#   safePickle)
#from global_vars import (BRANCH_FILEPATH, CORPUS_NAME, LDA_CORP_FILEPATH,
#   LDA_DICT_FILEPATH, LDA_NUM_SUBSETS, LDA_PATH)

# *Streaming* corpus classes
class CanadianCorpus(object):
    def __init__(self, dictionary):
        self.dictionary = dictionary
  
    def __iter__(self):
        return (self.dictionary.doc2bow(f.lower().split()) for f in streamFiles())
    
class CanadianObjectBranchCorpus(object):
    def __init__(self, dictionary):
        self.dictionary = dictionary
    
    def __iter__(self):
        return (self.dictionary.doc2bow(f.lower().split()) for f in streamObjectBranches())

# [NOTE: Used to be construct_static_corpus(), until 2019-02-19 -JJ]
def construct_corpus(pl, num_cores=8):
    # Construct the static corpus in parallel, to make the weight computations
    # quicker
    # Get the subcorpus number for this AWS/TL instance
    subcorp_num = pl.lda_subcorp_num

    # Load the doclist for this subcorpus number
    doclist_df = load_doclist_df(pl, subcorp_num)
    doc_list = list(doclist_df["doc_clean"])
    pl.iprint("doclist loaded")

    # Now load the dictionary so we have the doc2bow function
    dictionary = joblib.load(pl.get_lda_dict_fpath())
    pl.iprint("Dictionary loaded")
    
    # Use multiprocessing to process batches of docs in parallel
    batch_size = 1000
    doc_chunks = [doc_list[i:i+batch_size] for i in range(0,len(doc_list),batch_size)]
    num_workers = num_cores - 1
    par_obj = Parallel(n_jobs=num_workers, verbose=5)
    chunk_vectors = par_obj(delayed(vectorize_docs)(dictionary, cur_chunk) for cur_chunk in doc_chunks)
    # Now put the chunks together and save in mm format
    all_docs = []
    for chunk in chunk_vectors:
        all_docs.extend(chunk)
    pl.iprint("Construction complete. Saving corpus")
    save_static_corpus(pl, all_docs, subcorp_num)

def construct_streaming_corpus(pl, dictionary):
    # Sets up the corpus and dictionary for topic modeling
    #print("constructCorpus()")
    corpus = CanadianCorpus(dictionary)
    #corpus = CanadianObjectBranchCorpus(dictionary)
    return corpus

def load_doclist_df(pl, doclist_num):
    # Loads the doc list created by serialize_docs(). On TL this will be in the
    # lda subfolder, whereas on AWS it will just be in the working directory
    serialized_fpath = pl.get_lda_doclist_fpath(doclist_num=doclist_num)
    doc_df = pd.read_pickle(serialized_fpath)
    return doc_df


def save_static_corpus(pl, subcorpus, subcorp_num):
    # Save the *actual* subcorpora (i.e., we're done with the doclists now)
    # Using current directory since this will be run on AWS
    if subcorp_num >= 0:
        pl.iprint("Saving subcorpus #" + str(subcorp_num))
        static_fpath = pl.get_lda_subcorp_fpath(subcorp_num)
    else:
        # Full corpus
        pl.iprint("Saving full corpus")
        static_fpath = pl.get_lda_corpus_fpath()
    plu.safe_serialize_corpus(static_fpath, subcorpus)

### [OLD CODE using csv library... removed 2019-02-26 -JJ]
# def serialize_articles(pl):
    # subcorp_ranges = [get_subcorp_range(i) for i in range(pl.num_lda_subsets)] + [(float('inf'),float('inf'))]
    # cur_subcorp_num = 0
    # doc_list = []
    # with codecs.open(pl.get_sumstats_filepath(extension="csv"),'r','utf-8') as f:
    #     reader = csv.reader(f)
    #     for row_num, row in enumerate(reader):
    #         # Check if we've hit the start row of the next subcorpus
    #         if row_num == corpus_ranges[cur_corpus+1][0]:
    #             # Save corpus
    #             print("***** On row " + str(row_num) + ", Saving corpus " +
    #                str(cur_corpus) + " before continuing")
    #             serialized_fpath = pl.get_corpus_fpath(corpus_num=cur_corpus_num)
    #             plu.safe_to_pickle(doc_list, serialized_fpath)
    #             # Clear doc_list
    #             doc_list = []
    #             # Switch to next corpus
    #             cur_corpus = cur_corpus + 1
    #         if row_num % 1000000 == 0:
    #             debugPrint("*** Processing doc #" + str(row_num))
    #         doc_list.append(row[4].lower().split())
    # # Save the last corpus
    # print("***** Saving corpus " + str(cur_corpus))
    # safePickle(doc_list, pl.get_lda_corpus_filepath())

def stream_to_static_corpus(pl, dictionary):
    # Loads all of the object branches using streamObjectBranches but saves
    # them into a static (serializable, non-streaming) corpus
    corpus = [dictionary.doc2bow(f.lower().split()) for f in pl.stream_object_branches()]
    return corpus
  
def vectorize_docs(dictionary, cur_doc_list):
    # Converts the list of documents from strings to bag-of-words vectors
    return [dictionary.doc2bow(l) for l in cur_doc_list]
    
# FOR TESTING ONLY
# if __name__ == "__main__":
#     if len(sys.argv) == 2 and sys.argv[1] == "generate":
#         # Just generate the serialized sub-corpora files, to be sent to AWS for
#         # actual corpora construction
#         serializeObjectBranches()
#         quit()
#     elif len(sys.argv) == 2 and sys.argv[1] == "combine":
#         # Combine the separate corpora parts generated on AWS
#         combineCorpora()
#         quit()
#     elif len(sys.argv) == 3:
#         # To run this on AWS, you just need this .py file, the serialized_docs files,
#         # and the _dict.pkl files. Then run from the command line using
#         # python lda3-construct_corpus.py <corpus_num> <num_cores>
#         corpus_num = int(sys.argv[1])
#         num_cores = int(sys.argv[2])
#     else:
#         print("<corpus_num> <num_cores> arguments required")
#         quit()
#         # Default values for textlab
#         #corpus_num = 1
#         #num_cores = 8
    
#     # Logging initialization for joblib multiprocessing
#     logging.basicConfig(format='%(levelname)s : %(message)s', level=logging.INFO)
  
#     # Load the dictionary produced by lda2-construct_dictionary.py. Since this
#     # part will be run on AWS, we *DON'T* use LDA_DICT_FILEPATH. Instead we just
#     # use the current directory
#     dictionary = loadDictionary(os.path.basename(LDA_DICT_FILEPATH))
  
#     # Uncomment the following if you're using a streaming corpus
#     # Construct corpus (there's no corpus file here, since the corpus is actually
#     # just a stream from the disk)
#     #corpus = constructStreamingCorpus(dictionary)
  
#     # Code for constructing a static corpus
#     construct_static_corpus(dictionary,corpus_num,num_cores)
