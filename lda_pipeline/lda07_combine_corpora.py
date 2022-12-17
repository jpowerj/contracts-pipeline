# Takes the separate corpora created on the various AWS instances and combines
# them into one final corpus for use by the LDA model in the next step
import itertools

import gensim

def load_subcorp(pl, subcorp_num):
    subcorp_fpath = pl.get_lda_subcorp_fpath(subcorp_num=subcorp_num)
    pl.iprint("Loading subcorpus #" + str(subcorp_num) + " from " + str(subcorp_fpath))
    subcorp = gensim.corpora.MmCorpus(subcorp_fpath)
    return subcorp

def combine_corpora(pl):
    # See http://thread.gmane.org/gmane.comp.ai.gensim/1842
    # (after a longer-than-needed search :( )
    corpora = [load_subcorp(pl, i) for i in range(pl.num_lda_subsets)]
    #gensim.corpora.MmCorpus.serialize("./canadian_obj_v2_corpus.mm", itertools.chain(corpus0,corpus1))
    # Note the * before corpora, so that each corpus gets passed in as an argument to the
    # itertools.chain() function separately
    gensim.corpora.MmCorpus.serialize(pl.get_lda_corpus_fpath(),
                                      itertools.chain(*corpora))