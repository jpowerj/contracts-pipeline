# -*- coding: utf-8 -*-
"""
Created on Thu Apr 28 10:19:34 2016

@author: elliott
"""

# Input: The "sections" field within collection_name, produced by
# 1-extract_sections.py
# Output: Each document within the collection will now also have a "sections"
# array, where each element i contains data for the i-th section
import argparse
import logging
import glob
import json
import os

root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)

# 3rd party imports
import joblib
import spacy
import neuralcoref
import sys

def articles_as_strlist(contract_data, tuples=True):
    """
    Takes in a list of article_data dictionaries and returns just a list of the
    plaintext of the articles
    """
    def line_as_str(line_dict):
        line_tokens = line_dict["tokens"]
        line_tokens_str = [token["text"] for token in line_tokens]
        return " ".join(line_tokens_str)

    def article_as_str(article_dict):
        art_lines = article_dict["lines"]
        art_lines_str = [line_as_str(cur_line) for cur_line in art_lines]
        return " ".join(art_lines_str)

    contract_id = contract_data["contract_id"]
    art_list = contract_data["articles"]
    if tuples:
        article_strs = [(article_as_str(cur_art), {'contract_id': contract_id, 'article_num':art_num}) 
                        for art_num, cur_art in enumerate(art_list)]
    else:
        article_strs = [article_as_str(cur_art) for cur_art in art_list]
    return article_strs



# "Internal" imports
from collections import defaultdict
#import pipeline_util as plu

## These will be removed in a future version -- replaced by the Pipeline class
#from global_vars import (data_db_name, collection_name, CORPUS_DICT, DEBUG,
#  LANG_CODES, PICKLE_PATH, USE_MONGO)
#from global_functions import (debugPrint, fixCornerCases, genFilePrefix,
#  safePickle, streamSections)
class Pipeline(object):
    def iprint(self, msg):
        #if self.use_logging:
        #    logging.info(msg)
        print (msg)
    def dprint(self, msg):
        if self.debug:
            # Find out which file called it
            cur_frame = inspect.currentframe()
            outer_frames = inspect.getouterframes(cur_frame, 2)
            # It's really the filepath, so get just the basename
            caller = os.path.basename(outer_frames[1].filename)
            if self.use_logging:
                logging.debug(msg)
            else:
                print("[DEBUG: " + str(caller) + "] " + str(msg))


    def json_load(self, fpath):
        with open(fpath, 'r') as f:
            data = json.load(f)
        return data
    def __init__(self, corpus_name, output_path=None, batch_range=None):
        self.corpus_name = corpus_name 
        self.output_path = output_path
        self.artsplit_path = os.path.join(output_path, "01_artsplit")
        self.batch_range = batch_range
        self.num_articles = len(os.listdir(self.artsplit_path))
        self.use_logging= False

    def stream_art_data(self, verbose=False, start=None, end=None):
        """ Used by the LDA pipeline, to stream individual Articles rather than
        Contracts. The latter is a bit janky since you then have to go in and
        extract the article list from the contract object. """
        #print(start,end)

        for cur_contract_data in self.stream_split_contracts(verbose=verbose, start=start, end=end):
            #print(cur_contract_data["contract_id"])
            #print(start,end)
            #input("")
            # Get the list of articles
            art_data_list = articles_as_strlist(cur_contract_data, tuples=True)
            print ("articles loaded")
            # And now yield each element of *this* list itself (hence difference
            # between this function and stream_split_contracts())
            for cur_art_data in art_data_list:
                #print (cur_art_data)
                #if sum(1 for i in cur_art_data[0].split() if len(i) == 1) / len(cur_art_data[0].split()) > 0.25:
                #    print (cur_art_data)
                #    input("")

                yield cur_art_data

    def stream_split_contracts(self, verbose=False, ext="json", start=None, end=None):
        # Loads+yields the next artsplit file (which contains the contract_id
        # and language along with the plaintext and artsplits)
        """
        if self.batch_range:
            self.iprint("Using provided batch_range")
            start = self.batch_range[0]
            end = self.batch_range[1]
        else:
            self.iprint("Streaming full set of split contracts")
            start = 0
            # end = None means just go to the end of the list
            end = None
        """
        #print(start,end)
        #input("")
        #pkl_fpaths = os.listdir(self.get_artsplit_path()).sort()
        #pkl_fpaths = glob.glob(os.path.join(self.artsplit_path,f"*.{ext}"))
        pkl_fpaths = glob.glob(os.path.join(self.artsplit_path,"*." + ext))
        pkl_fpaths.sort()
        print (len(pkl_fpaths))

        if start is not None and end is not None:
            path_sublist = pkl_fpaths[start:end]
        else:
            path_sublist = pkl_fpaths
        print (len(path_sublist))
        #self.iprint(f"Loading {ext} files from {self.artsplit_path}")
        self.iprint("Loading " + ext + " files from " + self.artsplit_path)
        """
        pkl_fpaths = glob.glob(os.path.join(self.get_artsplit_path(),f"*.{ext}"))
        pkl_fpaths.sort()
        self.iprint(f"Loading {ext} files from {self.get_artsplit_path()}")
        if end is not None:
            path_sublist = [fpath for fnum, fpath in enumerate(pkl_fpaths) 
                            if (fnum >= start) and (fnum <= end)]
        else:
            path_sublist = [fpath for fnum, fpath in enumerate(pkl_fpaths) if fnum >= start]
        # (The actual loading + yielding)
        """ 
        c = 0
        for json_fpath in path_sublist:
            if not json_fpath.endswith("_eng.json"):
                continue
            #print (json_fpath)
            c += 1
            #if c > 3: 
            #    break
            if verbose:
                self.iprint("Loading " + str(json_fpath))
            # Load the pickle
            contract_data = pl.json_load(json_fpath)
            yield contract_data
    def save_parsed_statements(self, statement_list, fn=""):
        """
        Since the parallelism requires us to process all of the statements in all of the
        contracts in a giant batch (that it internally optimizes), at the end we need to
        save a big .pkl file containing all the parse data here.
        """
        #parses_fpath = os.path.join(self.output_path,f"test_02_{self.corpus_name}_parsed.pkl")
        parses_fpath = os.path.join(self.output_path,"test_02_" + self.corpus_name + "_parsed.pkl") 
        #parses_fpath = self.get_parses_fpath()
        if fn != "":
            parses_fpath = parses_fpath[:-4] + "_" + fn + ".pkl" 
            print (parses_fpath) 
        joblib.dump(statement_list, parses_fpath)



subdeps = ['nsubj','nsubjpass', 'expl']

maindeps = ['nsubj','nsubjpass', 
                            'expl', # existential there as subject
                            'advmod', 
                            'dobj',
                            'prep',
                            'xcomp',
                            'dative', # indirect object
                            'advcl',
                            'agent',
                            'ccomp',
                            
                            'acomp',
                            'attr']
        
def get_branch(t,sent,include_self=True):        
    branch = recurse(t)
    if include_self:
        branch += [t]
            
    #branch = [m for m in branch if m.dep_ != 'punct' and not m.orth_.isdigit()]
    branch = [w for w in sent if w in branch]# and w.dep_ in include]

    lemmas = []
    tags = []
    
    for token in branch:
        lemma = token.lemma_.lower()
        #if len(lemma) <= 2:
        #    continue
        if any([char.isdigit() for char in lemma]):
            continue
        if any(punc in lemma for punc in ['.',',',':',';', '-']):
            continue
        lemmas.append(lemma)
        tags.append(token.tag_)
    
    #tags = [w.tag_ for w in sent if w in mods]
    return lemmas, tags

def get_statements(art_nlp, contract_id, art_num):
    #print("get_statements()")
    statement_list = []
    time_in_pbs = 0
    # For now, since spaCy neural coref is super buggy, need to check if
    # there are any coref clusters in the doc
    any_corefs = art_nlp._.coref_clusters is not None
    #print (art_nlp._.coref_clusters)
    #print (any_corefs)
    #any_corefs = False
    for sentence_num, sent in enumerate(art_nlp.sents):
        tokcheck = str(sent).split()
        if any([x.isupper() and len(x) > 3 for x in tokcheck]):
            # Don't parse this sentence
            continue
        
        #pbs_start = timer()
        sent_statements = parse_by_subject(sent, resolve_corefs=any_corefs)
        #pbs_end = timer()
        #pbs_elapsed = pbs_end - pbs_start
        #time_in_pbs += pbs_elapsed
        
        for statement_num, statement_data in enumerate(sent_statements):
            full_data = statement_data.copy()
            full_data['contract_id'] = contract_id
            full_data['article_num'] = art_num
            full_data['sentence_num'] = sentence_num
            full_data['statement_num'] = statement_num
            full_data['full_sentence'] = str(sent)
            # Note to self: statement_data contains a "full_statement"
            # key, so that gets "transferred" over to full_data.
            #print("Data to put in the db:")
            #print(data)
            statement_list.append(full_data)
    #print("Loop over sentences took " + str(total_pbs))
    return statement_list

def parallel_parse(pl, nlp_eng):
    # See https://spacy.io/api/language#pipe for how I'm incorporating metadata here
    statement_list = []
    # Debugging
    #print(next(pl.stream_art_data()))
    #for article_nlp in nlp_eng.pipe(art_list, n_threads=8):
    if pl.batch_range is not None:
        batchsize = pl.batch_range
        for i in range(pl.num_articles // batchsize + 1):
            if i <= int(sys.argv[1]):
                continue
            art_data = pl.stream_art_data(start=i * batchsize, end=(i+1) * batchsize, verbose=True)
            #art_data = (i for i in [('My sister has a dog.', {"contract_id": 0, "article_num":0}), ('She loves him.', {"contract_id": 0, "article_num":1})])
            for text, art_meta in art_data:
                #print (text, art_meta)
                #text = 'My sister has a dog. She loves him.'
                #text = "My sister has a dog. She loves him."
                art_nlp = nlp_eng(text)
                #print(art_nlp._.coref_clusters)
                contract_id = art_meta["contract_id"]
                art_num = art_meta["article_num"]
                art_statements = get_statements(art_nlp, contract_id, art_num)
                statement_list.extend(art_statements)
                #input("")
            #for art_nlp, art_meta in nlp_eng.pipe(art_data, as_tuples=True):
            print (i)
            pl.save_parsed_statements(statement_list, fn=str(i))
            input("")
            statement_list = []
    else:
        art_data = pl.stream_art_data(verbose=True)
        #for art_nlp, art_meta in nlp_eng.pipe(art_data, as_tuples=True, n_process=8):
        #for art_nlp, art_meta in nlp_eng.pipe(art_data, as_tuples=True):
        for art_nlp, art_meta in nlp_eng.pipe(art_data, as_tuples=True):
        #for art_nlp, art_meta in nlp_eng.pipe(art_data, as_tuples=True):
            #print (art_nlp)
            contract_id = art_meta["contract_id"]
            art_num = art_meta["article_num"]
            art_statements = get_statements(art_nlp, contract_id, art_num)
            statement_list.extend(art_statements)

    """
    for art, art_meta in art_data:
        print (art, art_meta)
        art_nlp = nlp_eng(art)
        contract_id = art_meta["contract_id"]
        #print(f"Processing contract_id {contract_id}")
        art_num = art_meta["article_num"]
        art_statements = get_statements(art_nlp, contract_id, art_num)
        statement_list.extend(art_statements)
    """
    return statement_list

# For debugging only! Processes the contract sentences one-by-one, so you
# can do things like print and see exactly where you're at in the order
# (unlike the parallel version. Printing would be a bit chaotic, tho doable)
def serial_parse(art_list, nlp_eng):
    #print("serial_parse()")
    statement_list = []
    for art_num, cur_art in enumerate(art_list):
        art_nlp = nlp_eng(cur_art)	
        art_statements = get_statements(art_nlp, art_num)
        statement_list.extend(art_statements)
    return statement_list


def parse_articles(pl, parallel=True):
    # Basically gets everything ready and then calls parallel_parse() as
    # a subroutine
    pl.iprint("Starting parse_articles()")
    #breakpoint()
    # Uncomment this to clear the pickles/statements folder out, in case there's junk
    # from prior runs there. The downside is if you accidentally re-run the .py
    # it deletes all previous .pkls :(
    #plu.safe_clear_path(pl.get_parsed_pkl_path())

    # OLD: Just does "standard" parses
    # nlp_eng = spacy.load('en')
    # NEW: Also does coreference detection+resolution
    pl.iprint("Loading spaCy core model")
    #nlp_eng = spacy.load('en_core_web_md', disable=["ner"]) # cannot install this module, thus go to en_core_web_sm
    nlp_eng = spacy.load('en_core_web_sm', disable=["ner"])
    pl.iprint("Loading spaCy coref model. May take a while...")

    neuralcoref.add_to_pipe(nlp_eng)
    
    # adding neuralcoref yields TypeError: can not serialize 'spacy.tokens.span.Span' object
    # following workaround proposed in https://github.com/huggingface/neuralcoref/issues/82#issuecomment-569431503
    """
    def remove_unserializable_results(doc):
        doc.user_data = {}
        for x in dir(doc._):
            if x in ['get', 'set', 'has']: continue
            setattr(doc._, x, None)
        for token in doc:
            for x in dir(token._):
                if x in ['get', 'set', 'has']: continue
                setattr(token._, x, None)
        return doc

    nlp_eng.add_pipe(remove_unserializable_results, last=True)
    """



    if parallel:
        statement_list = parallel_parse(pl, nlp_eng)
    else:
        statement_list = serial_parse(pl, nlp_eng)
    #breakpoint()
    pl.save_parsed_statements(statement_list)

def parse_by_subject(sent, resolve_corefs=True):
    ### TODO: SMART THINGS like splitting the tree into *segments*
    ### such that each segment is the "phrase" for its subject.
    ### e.g. if sent has more than one subject:
    ### (1) find the HEAD subject
    ### (2) "cut off" all the other subtrees of *non-HEAD* subjects.
    ###     In other words everywhere you see a subject, besides the
    ###     HEAD, "clip" the tree at that node. Pull that node out of
    ###     the tree and keep it separate
    ### (3) Iterating over all the subtrees of nodes in (2) gives you
    ###     the non-HEAD phrases. Now to get the HEAD phrase you take
    ###     all the tokens that haven't been "covered" yet, e.g., any
    ###     token whose ONLY subject ancestor is HEAD.
    #all_tokens = [t for t in sent]
    subjects = [t for t in sent if t.dep_ in subdeps]

    ## Only for debugging
    #for cur_sub in subjects:
    #    if "Board" == str(cur_sub):
    #        print(all_tokens)

    datalist = []

    # Each subject corresponds to a statement that it is the subject of.
    # Hence this is a loop over *statements*
    for obnum, subject in enumerate(subjects):   
        subdep = subject.dep_
        
        # Again, debugging
        #if str(subject) == "Board" or str(subject) == "claim":
        #    print(subject)
        #    print(subject.head)
        #    print(list(subject.head.subtree))
        
        mlem = None
        verb = subject.head
        if not verb.tag_.startswith('V'):
            continue        
                
        vlem = verb.lemma_
        
        tokenlists = defaultdict(list)
        
        #if 'if' in tokcheck:
        #    print(sent)
        #    raise
                        
        neg = ''
        for t in verb.children:
            if t.tag_ == 'MD':
                mlem = t.orth_.lower()
                continue
            dep = t.dep_
            if dep in ['punct','cc','det', 'meta', 'intj', 'dep']:
                continue
            if dep == 'neg':
                neg = 'not'                
            #elif t.dep_ == 'auxpass':
            #    vlem = t.orth_.lower() + '_' + vlem
            elif t.dep_ == 'prt':
                vlem = vlem + '_' + t.orth_.lower()                    
            #elif dep in maindeps:
            #    tokenlists[dep].append(t)
            else:
                #pass
                #print([modal,vlem,t,t.dep_,sent])
                #dcount[t.dep_] += 1
                tokenlists[dep].append(t)
                
        slem = subject.lemma_

        #print("subject lemma: " + str(slem))
        in_coref = False
        cr_subject = subject
        cr_slem = slem
        num_clusters = 0
        coref_replaced = False
        if resolve_corefs:
            in_coref = subject._.in_coref
            # Now check if it's *different* from the coref cluster's main coref
            # TODO: Right now we take the first cluster. Instead, take the cluster
            # with the *closest* main to the subject
            if in_coref:
                coref_clusters = subject._.coref_clusters
                num_clusters = len(coref_clusters)
                first_cluster = coref_clusters[0]
                # Get the main of this first cluster
                cluster_main_lem = first_cluster.main.lemma_
                if slem != cluster_main_lem:
                    # Replace it with main!
                    cr_slem = cluster_main_lem
                    coref_replaced = True

        data = {'orig_subject': subject.text,
                'orig_slem': slem,
                'in_coref': in_coref,
                'subject': cr_subject.text,
                'slem': cr_slem,
                'coref_replaced': coref_replaced,
                'modal':mlem,
                'neg': neg,
                'verb': vlem,
                #'full_sentence': str(sent),
                #'subfilter': 0,
                'passive': 0,
                'md': 0}
        
        if subdep == 'nsubjpass':
            data['passive'] = 1
        if mlem is not None:
            data['md'] = 1
        
        subphrase, subtags = get_branch(subject,sent)                                        
        
        data['subject_branch'] = subphrase        
        data['subject_tags'] = subtags
        
        object_branches = []
        object_tags = []
        
        for dep, tokens in tokenlists.items():
            if dep in subdeps:
                continue
            for t in tokens:
                tbranch, ttags = get_branch(t,sent)                
                object_branches.append(tbranch)
                object_tags.append(ttags)
        data['object_branches'] = object_branches
        data['object_tags'] = object_tags

        # Last but not least, the full text of the statement
        # (if possible?) TODO. It's NOT trivial. So for now it's
        # just always the empty string
        data['full_statement'] = ""
        
        # So upon being added to datalist, the "data" dictionary has the following
        # keys: 'orig_subject','orig_slem','in_coref','subject', 'slem',"modal",
        # "neg","verb","passive","md","subject_branch","subject_tags",
        # "object_branches", "object_tags", "full_statement" (empty string for now)

        datalist.append(data)
    
    return datalist

# Takes in the *section* list for a contract and parses them in parallel(!)
# This inherently gives us sentences (normal sentence tokenizer)
# AND statements (dependency tree of the sentences), via spaCy.
# 2018-12-22 Update: Now it *also* gives us coreference resolution info!
def parse_contract(pl, contract_data, nlp_eng, parallel=True):
    #pl.iprint(f"parse_contract(parallel={parallel})")
    pl.iprint("parse_contract(parallel=" +  str(parallel) + ")")
    # Returns a DICTIONARY with both the number of statements found in the
    # contract AND a status message (either "Success" or a specific error message)
    if contract_data["lang"] != "eng":
        return {"status":"Doc not in English","num_statements":0}
    pl.dprint("***** contract_id #" + str(contract_data["contract_id"]))
    
    # Now loop over articles
    if contract_data["articles"] is None:
        return {"status":"No articles in the doc","num_statements":0}
    
    art_start = timer() # For timing
    art_data_list = plu.articles_as_strlist(contract_data, tuples=True)
    num_articles = len(art_data_list)
    pl.dprint("Number of articles: " + str(num_articles))
    # THE ACTUAL PARSING HAPPENS HERE
    if parallel:
        statement_list = parallel_parse(art_data_list, nlp_eng)
    else:
        statement_list = serial_parse(art_data_list, nlp_eng)
    art_end = timer()
    art_elapsed = art_end - art_start
    # Now we're at the end of the contract loop iteration - place all the
    # statements into the db at once
    num_statements = len(statement_list)
    # Save statement_data into a .pkl corresponding to this contract
    # gen_filename() pads contract_id with the correct number of
    # leading zeros
    #breakpoint()
    pl.dprint("Saving statements for contract " + str(contract_data["contract_id"]))
    json_filename = pl.gen_filename(contract_data["contract_id"], contract_data["lang"], "json")
    #logging.debug("[main02] line235 pickle_filename = " + pickle_filename)
    #logging.debug("Saving to " + pickle_filename)
    json_fpath = os.path.join(pl.get_parse_path(), json_filename)
    # What are the types of each element? Check that there are no
    # non-serializable types before pickling
    #first_statement = statement_list[0]
    #for cur_key in first_statement:
    #    print("Key = " + str(cur_key) + ", Type = " + str(type(first_statement[cur_key])))
    plu.json_dump(statement_list, json_fpath)
    
    # And a .csv version (for use by spelling mistake pipeline)
    #csv_filename = pl.gen_filename(contract.contract_id, contract.lang, "csv")
    #csv_fpath = os.path.join(pl.get_parsed_csv_fpath(), csv_filename)
    #plu.safe_csv(statement_list, )
    # Hmm... its less straightforward than the pkl because we need like
    # statement_num, article_num, etc... TODO.
    pl.dprint("Loop over " + str(num_articles) + " sections took " 
                  + str(art_elapsed))
    #pl.dprint("parse_by_subject() total: " + str(time_in_pbs))
        
    return {"status":"Success", "num_statements":num_statements}

def recurse(*tokens):
    children = []
    def add(tok):       
        sub = tok.children
        for item in sub:
            children.append(item)
            add(item)
    for token in tokens:
        add(token)    
    return children


def test_parser(contract_obj, nlp_eng):
    # FOR DEBUGGING: just parse one contract, without the parallelism
    contract_sections = contract_obj["sections"]
    # This was to test the one annoying contract that breaks everything
    #first_sections = fixCornerCases(start_index, first_sections)
    #print("Section #18: " + str(first_sections[18]))
    #problem_section = first_sections[18]
    first_section = contract_sections[0]
    print(first_section)
    nlp_result = nlp_eng(first_section)
    print(nlp_result._.coref_resolved)
    print("test_parser() complete")
if __name__ == "__main__":
    #DATA_PATH=/home/dominsta/Documents/powerparse/data_sample
    output_path="/home/dominsta/Documents/powerparse/output_coref_test"
    output_path = "/cluster/work/lawecon/Work/dominik/powerparser/output_canadian_coref"
    #pl = Pipeline("canadian_local_sample", output_path=output_path, batch_range=2000)
    #spacy.prefer_gpu()
    pl = Pipeline("canadian_local_sample", output_path=output_path, batch_range=10)
    parse_articles(pl)
