# Trains an LDA model using gensim
import logging
import os

import gensim
import joblib

import pipeline_util as plu

#from global_functions import (debugPrint, loadCombinedCorpus, loadDictionary,
#    safePickle)
#from global_vars import (CORPUS_NAME, LDA_PATH, LDA_TOPICS, LDA_WORKERS, OUTPUT_PATH)

# [From old version. -JJ, 2019-02-19]
### Where to save the serialized model
#lda_model_file = os.path.join(LDA_PATH, CORPUS_NAME + "_lda.pkl")
### Where to save the similarity scores
#sims_file = os.path.join(LDA_PATH, CORPUS_NAME + "_lda_sims.csv")

### Pairs file, for similarity computation (maps each contract_num to the previous
### contract's contract_num)
#pairs_file = os.path.join(OUTPUT_PATH, CORPUS_NAME + "_prev_ids.csv")

def compute_lda_similarity(lda_model, doc1, doc2):
    lda1 = lda_model[doc1]
    lda2 = lda_model[doc2]
    sim = gensim.matutils.cossim(lda1, lda2)
    return sim

def compute_sims(lda_model, dictionary):
    # Loads in a list of pairs and computes similarities between the docs based
    # on the topic proportions
    pairs_df = pd.read_csv(pairs_file)
    pairs_df["sim_lda"] = np.nan
    for row_num, row_data in pairs_df.iterrows():
        debugPrint("*** Pair #" + str(row_num))
        debugPrint(row_data)
        # Now get the corresponding docs and compute their LDA sims
        contract_id = int(row_data["contract_id"])
        prev_id = int(row_data["prev_id"])
        cur_doc = txtToDoc(loadTxtById(contract_id), dictionary)
        prev_doc = txtToDoc(loadTxtById(prev_id), dictionary)
        sim = computeLDASimilarity(lda_model, cur_doc, prev_doc)
        pairs_df.set_value(row_num, "sim_lda", sim)
    return pairs_df
  
def launch_lda(pl, corpus, dictionary):
    pl.iprint("Launching LDA model with " + str(pl.num_topics) + " topics")
    print(corpus)
    print(dictionary)
    model = gensim.models.LdaMulticore(corpus, id2word=dictionary, 
                        num_topics=pl.num_topics, workers=pl.num_lda_workers,
                        iterations=100)
    return model
  
def load_lda(filename):
    model = gensim.models.LdaMulticore.load(filename)
    return model

def pairwise_similarities(pl):
    ## Compute similarities
    sims = computeSims(model, canadian_dict)
    saveSims(sims, sims_file)

def print_lda(lda_model):
    all_topics = lda_model.show_topics(num_topics=20,num_words=20)
    for topic in all_topics:
        print(topic[0],topic[1])
        print("-----")

# TODO: Split into "regular" LDA run and then the pairwise contract similarities
# run (i.e., make two separate pipelines for these two separate tasks)
def run_lda(pl):
    ## Load the dictionary and corpus
    lda_dict_fpath = pl.get_lda_dict_fpath()
    pl.iprint("Loading LDA dictionary from " + str(lda_dict_fpath))
    lda_dict = joblib.load(lda_dict_fpath)
    # Load the combined corpus
    corpus_fpath = pl.get_lda_corpus_fpath()
    pl.iprint("Loading LDA corpus from " + str(corpus_fpath))
    corpus = gensim.corpora.MmCorpus(corpus_fpath)
    # Construct LDA model
    if os.path.isfile(pl.get_lda_model_fpath()):
        if pl.force_overwrite:
            pl.iprint("OVERWRITING LDA MODEL")
        else:
            input("LDA MODEL ALREADY EXISTS. Press Enter to continue and overwrite it "
                  + "or Ctrl+C to kill this script...")
    model = launch_lda(pl, corpus, lda_dict)
    save_lda(pl, model)
  
    print_lda(model)
  
def save_lda(pl, model):
    model.save(pl.get_lda_model_fpath())

def save_sims(pairs_df, filename):
    pairs_df.to_csv(filename,index=False)

def similarity_test(lda_model):
    ## Similarity test
    dictionary = loadDictionary(os.path.join(LDA_PATH, CORPUS_NAME + "_dict.pkl"))
    with codecs.open("/home/ubuntu/mongo_txts/00001_eng.txt",'r','utf-8') as f:
        cur_txt = f.read()
        doc1 = dictionary.doc2bow(cur_txt.lower().split())
    with codecs.open("/home/ubuntu/mongo_txts/00002_eng.txt",'r','utf-8') as f:
        cur_txt = f.read()
        doc2 = dictionary.doc2bow(cur_txt.lower().split())
    doc1_lda = lda_model[doc1]
    doc2_lda = lda_model[doc2]
    same_sim = gensim.matutils.cossim(doc1_lda,doc1_lda)
    cur_sim = gensim.matutils.cossim(doc1_lda,doc2_lda)
    print(same_sim)
    print(cur_sim)
