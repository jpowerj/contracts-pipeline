# -*- coding: utf-8 -*-
"""
Created on Thu Apr 28 10:19:34 2016

@author: elliott
"""
### Input: A Pipeline var corpus_path set to the location of the contract texts,

### Output: Pickle files within the "articles" subfolder of data_path, one for
### each contract, plus .json files if pl.artsplit_debug is True

# Python imports
import json
import logging
import multiprocessing
import os
import re

# 3rd party imports
import joblib
from tqdm import tqdm

# Internal imports
from detect_artsplits import detect_artsplits
import pipeline_util as plu
#import pipeline as pl

#from global_functions import (debugPrint, genFilePrefix,
#  getPlaintextPaths, safePickle, streamPlaintexts)
#from global_vars import (USE_MONGO, PICKLE_PATH)

# run this in stanford directory:
# java -mx4g -cp "*" edu.stanford.nlp.pipeline.StanfordCoreNLPServer

#nlp = StanfordCoreNLP('http://localhost:9000')

# This needs to be global so parallel_split() can use it
artsplit_path = None
        
def clear_collection(collection):
    """ In MongoDB mode, helper function since I can never remember how to clear
    a collection """
    collection.drop()

def export_html(pl, contract):
    """ Export a simple HTML representation of the contract, where everything is
    the same as the plaintext except that the lines marked as breakpoints are
    highlighted in yellow """
    contract_id = contract.id
    html_doc = f"<!DOCTYPE html><html><head><title>{contract_id}</title></head><body>"
    # Now just put <br> instead of \n, and add the yellow highlighting to break lines
    for line_num, cur_line in contract.pt_lines:
        if cur_line.is_breakpoint:
            html_line = "<span style='background-color: yellow;'>" + str(cur_line) + "</span><br>"
        else:
            html_line = str(cur_line) + "<br>"
        html_doc += html_line
    html_doc += "</body></html>"
    # Filename should be the same as the plaintext but with .html instead of .txt
    pt_fpath = contract.fpath
    pt_filename = os.path.basename(pt_fpath)
    pt_root = os.path.splitext(pt_filename)[0]
    html_filename = pt_root + ".html"
    html_dir = pl.get_artsplit_html_path()
    html_fpath = os.path.join(html_dir, html_filename)
    with codecs.open(html_fpath, "w", "utf-8") as f:
        f.write(html_doc) 

def get_artsplit_debug_text(split_contract):
    """
    A plaintext version of the section-split contract, showing where the splits
    were made and what headers were extracted
    """
    if split_contract.state < SEC_SPLIT:
        raise Exception("Contract must be split before debug text can be generated")
    # TODO: GET HEADER PARSER TO WORK
    #headers = section_data["headers"]
    #print("Num headers: " + str(len(headers)))
    arts = split_contract.articles
    #print("Num sections: " + str(len(sections)))
    num_arts = len(arts)
    contract_full = [("="*80)+"\n"+arts[i]+"\n"+("="*80) 
                        for i in range(num_arts)]
    return "\n\n".join(contract_full)

def get_secsplit_json(split_contract, include_plaintext=True):
    """
    Export the detected sections as JSON-format annotations (with keys "text"
    and "entities", "text" being the original text and "entities" being a list of
    [startchar,endchar,entity_type] triples)

    Inputs: split_contract is a Contract object which has an instance variable
    "sections", a list of (header, section_text, startchar, endchar) tuples

    Outputs: A JSON string, with keys "entities" and (if include_plaintext=True)
    "text"
    """
    print("get_secsplit_json()")
    secsplit_dict = {'entities':split_contract.sections}
    if include_plaintext:
        secsplit_dict['text'] = split_contract.plaintext
    secsplit_json = json.dumps(secsplit_dict)
    return secsplit_json

def init_mongo():
    """ Use MongoDB instead of pickle files to load the contracts """
    pl.iprint("Loading sections from Mongo")
    # Don't need to import MongoClient in default case
    from pymongo import MongoClient
    client = MongoClient()
    data_db = client[data_db_name]
    data_collection = data_db[collection_name]
    # Clear the sections collection if it already exists
    clear_collection(data_collection)

def parallel_split(fpath_tuple):
    fpath_num = fpath_tuple[0]
    cur_fpath = fpath_tuple[1]
    cur_contract = plu.load_txt_by_fpath(cur_fpath, as_contract=True)
    cur_id = cur_contract.contract_id
    if iter_num % 100 == 0:
        print("***** Processing contract: " + str(cur_id))
    cur_contract.articles = detect_art_splits(cur_contract)
    # Save the Contract object, in its current state, to a file
    json_fname = pl.gen_fname(cur_id, cur_contract.lang, "json")
    json_fpath = os.path.join(artsplit_path, json_fname)
    plu.safe_to_pickle(cur_contract, json_fpath)
    pl.dprint("Saved contract to " + str(json_fpath))

art_reg_str = r'$(ARTICLE|Article)'
art_reg = re.compile(art_reg_str)
section_reg_str = r'$(SECTION|Section)'
section_reg = re.compile(section_reg_str)
secnum_reg_str = r'$(([0-9]\.[0-9])|([0-9]\.[0-9][0-9]))( |^)'
secnum_reg = re.compile(secnum_reg_str)
combined_reg_str = (r'(' + art_reg_str + r')|(' + section_reg_str +
                    r')|(' + secnum_reg_str + r')')
combined_reg = re.compile(combined_reg_str)

def regex_split(pl, cur_contract):
    # Use simple regular expressions to split the contract.
    # Go line-by-line and detect whether we should split on this line
    # (Remember that cur_contract.pt_lines is a list of *CLine objects*,
    # not a list of strings)
    break_indices = []
    contract_lines = cur_contract.pt_lines
    for line_num, cur_line in enumerate(contract_lines):
        line_str = str(cur_line)
        line_match = combined_reg.match(line_str)
        if line_match:
            break_indices.append(line_num)
    # Now the breaking
    # https://stackoverflow.com/questions/10851445/splitting-a-string-by-list-of-indices
    art_list = [contract_lines[i:j] for i,j in zip(break_indices, break_indices[1:]+[None])]
    return art_list

def serial_split(pl, contract_data):
    # Skip iteration if contract is not in batch

    cur_id = contract_data["contract_id"]
    #print (cur_id)
    #print (pl.batch_range)
    if pl.batch_range and cur_id < pl.batch_range[0]:
        return None
    if pl.batch_range and cur_id > pl.batch_range[1]:
        return None
    # Here we output the raw text to a file if debug_sections is on
    if pl.debug_artsplit:
        # Get the fpath
        orig_fpath = cur_contract.fpath
        orig_filename = os.path.basename(orig_fpath)
        new_path = pl.get_plaintext_output_path()
        new_fpath = os.path.join(new_path, orig_filename)
        # And copy to output folder
        plu.safe_copy_file(orig_fpath,new_fpath)
    if pl.split_method == "parser":
        # The detect_articles() function loads the contract's plaintext and
        # performs the split. It's in a separate file since it doesn't need
        # the pipeline object to work
        # (This makes it easy to test: you just need to make a Contract
        # object and call detect_articles() on it, rather than a whole
        # Pipeline object)
        # Note that this updates the articles field *within* the passed-in Contract
        # object, so it doesn't return anything
        new_cdata = detect_artsplits(contract_data)
        if pl.break_id and contract_data["contract_id"] == pl.break_id:
            breakpoint()
    elif pl.split_method == "regex":
        # pl.split_rule = "regex" (the simpler method)
        new_cdata = regex_split(pl, contract_data)
    else:
        raise Exception("Invalid split_method in Pipeline constructor")
    pl.save_artsplit_contract(new_cdata)
    # Uncomment for more verbose output
    #pl.save_artsplit_contract(new_cdata, pkl_debug=True)

    if pl.eval_artsplit and pl.eval_artsplit == "doccano":
        # And now, if test_artsplit is "doccano", also add this contract's
        # entity list to the list of all entity lists
        contract_entities.append(cur_contract.get_artsplit_entities())
    if pl.eval_artsplit and pl.eval_artsplit == "html":
        # Simple "test" mode: just output an html file where the lines chosen
        # as breakpoints are highlighted
        export_html(pl, cur_contract)

def split_contracts(pl, use_parallel_split=False):
    global artsplit_path
    pl.iprint("Starting split_contracts()")
    # First, we clear out the 01_artsplit directory if it was already filled
    pl.iprint("artsplit_path: " + str(artsplit_path))
    try:
        os.mkdir(pl.get_artsplit_path())
    except:
        pass
    if os.path.isdir(pl.get_artsplit_path()):
        #if not pl.force_overwrite:
        #    input(f"ABOUT TO EMPTY {pl.get_artsplit_path()}. Press Enter to Continue or Ctrl+C to Exit...")
        plu.safe_clear_path(pl.get_artsplit_path())
    # (For debugging. Set to an id to make breakpoint when it is reached)
    break_id = None
    if pl.eval_artsplit:
        # import the evaluation code
        from evaluate_art_splits import evaluate_artsplit
        # A list of lists, so that each element is a single contract's entity list
        contract_entities = []
    if use_parallel_split:
        # Parallel split
        artsplit_path = pl.get_artsplit_path()
        fpath_tuples = list(enumerate(pl.get_plaintext_fpaths()))
        mp_pool = multiprocessing.Pool()
        mp_pool.map(parallel_split, fpath_tuples)
    else:
        # Serial split
        #breakpoint()
        print ("num contracts", pl.compute_num_contracts())

        for cur_contract_data in pl.pbar(pl.stream_pt_contract_data(),
                                    total=pl.compute_num_contracts()):
            serial_split(pl, cur_contract_data)

    if pl.eval_artsplit:
        # Now that we've exported, call evaluate_art_splits() directly with the
        # json string, to get the evaluation score
        # Load the manual annotations
        all_jsons = []
        manual_json_strs = plu.readlines_by_fpath(pl.get_artsplit_json_fpath())
        for cur_json_str in manual_json_strs:
            cur_json = json.loads(cur_json_str)
            all_jsons.append(cur_json)
        # Remember: contract_entities contains the *auto* splits done by the
        # algorithm, all_jsons the *manual* splits done in doccano
        eval_score = evaluate_artsplit(contract_entities)
