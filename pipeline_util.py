# Python imports
import codecs
from collections import OrderedDict
import configparser
import csv
import glob
import json
import os
from pathlib import Path
import re
from shutil import copyfile
import string

# 3rd party imports
import gensim
import joblib
import pandas as pd
import numpy as np
import unidecode

# A simple "translator" that just removes punctuation
remove_punct = str.maketrans('','',string.punctuation)

# The index variables which ensure unique IDs for datasets at various levels
# (UID = Unique ID)
UID_CONTRACT = ["contract_id"]
UID_STATEMENT = ["contract_id","article_num","sentence_num","statement_num"]

# A regex to detect if a string is a filename or folder name
# (i.e., a string holding a file/folder that *doesn't exist yet*)
# It means "a dot followed by 1 to 3 alphanum characters (or dash
# or underscore), and then the end of the string"
ext_reg = r'\.[a-zA-Z-_]{1,3}$'

# Regex to check that a .txt file has the correct format (so we can infer the
# contract id and language from it). Basically it should look like name_<lang>.<ext>
# or just name.<ext>, where in the latter case we print a warning and assume English
fname_reg_langcode = re.compile(r'[a-zA-Z0-9]+_([a-z]{2,3})\.[A-Za-z0-9]+')
fname_reg_nocode = re.compile(r'[a-zA-Z0-9]+\.[a-zA-Z0-9]+')

# Only linebreaks
LINEBREAK_REG = r'\r\n'
# Any whitespace *besides* linebreaks
SPACES_REG = r'\w'
# Any whitespace, including linebreaks
WHITESPACE_REG = r'\w'

DEFAULT_SUBNORM_MAP = {
               'employee':'worker', 'worker':'worker', 'staff':'worker',
               'teacher': 'worker', 'nurse': 'worker', 'steward': 'worker',
                             
               'employer':'firm', 'company': 'firm', 'board': 'firm', 
               'hospital': 'firm', 'corporation': 'firm', 'owner': 'firm',
               'superintendent': 'firm',
                                                            
               'union':'union', 'association': 'union', 'member': 'union',
               'representative':'union', 
                             
               'manager':'manager', 'management':'manager', 'administration':'manager',
               'administrator':'manager', 'supervisor':'manager', 'director':'manager',               
               'principal': 'manager'}

class InvalidFilenameException(Exception):
    pass

################################
### Static utility functions ###
################################
# These are functions that don't require any "internal" information
# about the particular pipeline run, so can be used by whatever file
# (e.g., can be used by pipeline.py *and* main01.py without cyclic
# imports...)

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

def clean_comp_name(comp_name):
    # Lowercase all the company names for better matching
    to_return = comp_name.lower().strip()
    # Remove extraneous whitespace *inside* the string
    to_return = " ".join(to_return.split())
    # Some replacements for off_co_name_eng that make series linking work way better
    repl_list = [("&","and"),("ltd.","limited"),("co.","company"),("inc.",""),("inc","")]
    for cur_repl in repl_list:
        to_return = to_return.replace(*cur_repl)
    return to_return

# If file exists, clears it. Otherwise, creates a new empty file with that name
def clear_file(fpath):
    if not os.path.isfile(fpath):
        # touch is the linux command that creates a blank file
        Path(fpath).touch()
    else:
        # Opening with the 'w' flag clears the file
        open(fpath, 'w').close()

def combine_list(my_list):
    # Quick helper function for save_pdata_rows()
    return " ".join(my_list)

def combine_list_of_lists(my_list):
    return " ".join([" ".join(x) for x in my_list])

def cnum_in_range(fpath, start_num, end_num):
    """ Just a quick helper function that checks whether the contract with filepath
    fpath has an id within the range start_id to end_id, where either or both can
    be None. Also note that this range is *inclusive* """
    #print(f"cnum_in_range({start_num},{end_num})")
    contract_num = get_contract_num(fpath, is_fpath=True)
    if not start_num and not end_num:
        # Everything is in the (non-existent) range
        return True
    if not start_num:
        # Only end_id is provided
        return contract_num <= end_num
    if not end_num:
        # Only start_id is provided
        return contract_num >= start_num
    # If we're here, both start_id and end_id must have been provided
    return contract_num >= start_num and contract_num <= end_num

# Takes in a list of .csv filepaths and produces a Pandas df which is just
# all the data from the individual .csvs concatenated (vertically)
def combine_csvs(fpath_list):
    # (subnorm, subnorm_df) tuples
    df_list = [pd.read_csv(cur_fpath) for cur_fpath in fpath_list]
    combined_df = pd.concat(df_list)
    return combined_df

def fix_corner_cases(contract_id, contract_sections):
    # There were some annoying weird sections that broke spacy's parser. This
    # function removes the problematic text (e.g., an OCRed schedule table)
    if contract_id == 32737:
        orig = contract_sections[18]
        schedule_start = orig.index("Schedule of work hours by classification and department")
        end_str = "Storekeeper. Greaser. 2nd shift."
        schedule_end = orig.index(end_str) + len(end_str)
        before_sched = orig[:schedule_start]
        after_sched = orig[schedule_end:]
        nosched = before_sched + after_sched
        contract_sections[18] = nosched
    return contract_sections

# Returns a (sorted) list of all the contract nums within a given plaintext
# directory. Important to have this outside pipeline.py since otherwise you
# can't specify a different path_str from the corpus' path_str (which we need
# to do when loading from a preexisting sample path)
def get_cnum_list(path_str, extensions=["txt"]):
    all_filenames = []
    for cur_extension in extensions:
        cur_fpaths = glob.glob(os.path.join(path_str, "*." + cur_extension))
        cur_filenames = [os.path.basename(fpath) for fpath in cur_fpaths]
        all_filenames.extend(cur_filenames)
    cnum_list = [get_contract_id(filename) for filename in all_filenames]
    cnum_list.sort()
    return cnum_list

bad_fname = InvalidFilenameException("Plaintext filenames have invalid format. "
            + "Needs to be of the form <contract_id>.<extension> or "
            + "<contract_id>_<lang>.<extension>, "
            + "For example \"000a.txt\" or \"00001_en.txt\".")

def get_contract_num(file_descriptor, is_fpath=True):
    cur_prefix = _get_contract_fname_prefix(file_descriptor, is_fpath)
    if cur_prefix.isdigit():
        return int(cur_prefix)
    else:
        raise Exception("You called get_contract_num(), but the filename has a contract *id* in it: " + str(file_descriptor))

def get_contract_id(file_descriptor, is_fpath=True):
    """ Uses the filename of a contract to infer its contract_num """
    # If it's a filepath we need to get the basename first (if not, basename())
    # just returns the original string
    cur_prefix = _get_contract_fname_prefix(file_descriptor, is_fpath)
    if cur_prefix.isdigit():
        raise Exception("You called get_contract_id(), but the filename has the contract *num* in it: " + str(file_descriptor))
    else:
        return cur_prefix
    

def _get_contract_fname_prefix(file_descriptor, is_fpath=True):
    """ Helper function: looks at a file like <prefix>_<suffix>.<extension> and returns
    the prefix, so we can see if it's a number (contract_num) or an id (contract_id) """
    fname = os.path.basename(file_descriptor)
    langcode_match = fname_reg_langcode.match(fname)
    nocode_match = fname_reg_nocode.match(fname)
    if not langcode_match and not nocode_match:
        print(f"Bad filename: {fname}\nlangcode_match={langcode_match},nocode_match={nocode_match}")
        raise bad_fname
    if langcode_match:
        file_parts = fname.split("_")
        cur_prefix = file_parts[0]
    else:
        cur_prefix = fname.split(".")[0]
    return cur_prefix

def get_contract_lang(file_descriptor, is_fpath=True, default_lang="en"):
    """ NOTE: If no language is specified by the filename, English (en) is
    assumed! A warning is printed but still keep this in mind. """
    fname = os.path.basename(file_descriptor)
    # Now throw an exception if the filename isn't in the right format
    langcode_match = fname_reg_langcode.match(fname)
    nocode_match = fname_reg_nocode.match(fname)
    if not langcode_match and not nocode_match:
        print(f"[pipeline_util.py] Bad filename: {fname}\nlangcode_match={langcode_match},nocode_match={nocode_match}")
        raise bad_fname
    # Filename should be <id>_<lang>.txt, so extract the lang
    if langcode_match:
        file_parts = fname.split("_")
        lang_parts = file_parts[1].split(".")
        cur_lang = lang_parts[0]
    else:
        #print("[pipeline_util.py] Filename " + str(fname) + " has no language code, "
        #    + "so assuming default_lang (" + str(default_lang) + ")")
        cur_lang = default_lang
    return cur_lang

def get_range_corpus_name(old_corpus_name, sample_range):
    return old_corpus_name + "_r" + str(sample_range[0]) + "-" + str(sample_range[1])

def get_sample_corpus_name(old_corpus_name, sample_N):
    return old_corpus_name + "_s" + str(sample_N)

# (These run on import so we can use them in get_singular())
import inflect
inflecter = inflect.engine()
def get_singular(noun):
    """
    Uses the inflect library to generate the singular version of the noun, if it
    is plural.

    Added by JJ 2019-10-13
    """
    if not noun.strip():
        return "unknown"
    inflect_result = inflecter.singular_noun(noun)
    if not inflect_result:
        return noun
    else:
        return inflect_result

def has_langcode(file_descriptor):
    fname = os.path.basename(file_descriptor)
    return fname_reg_langcode.match(fname)

def json_dump(data, fpath):
    safe_open_dir(fpath, is_fpath=True)
    with open(fpath, 'w') as f:
        json.dump(data, f)

def json_load(fpath):
    with open(fpath, 'r') as f:
        data = json.load(f)
    return data

def list_to_regex(word_list):
    """ Used to convert LIWC word lists into regular expressions """
    wildcard_reg = [w.replace('*', r'[^\s]*') for w in word_list]
    reg_str = r'\b(' + '|'.join(wildcard_reg) + r')\b'
    return reg_str

def _load_config(corpus_name):
    """ Sort of a helper function, used by both _paths and _subnorm versions.
    Loads configuration data from <corpus_name>.conf file within
    the "configs" directory and returns ConfigParser object """
    print(f"Looking for config file {corpus_name}.conf")
    config_fpath = os.path.join("configs",f"{corpus_name}.conf")
    if not os.path.isfile(config_fpath):
        # Try one more time, this time in just the main code directory
        config_fpath_alt = f"{corpus_name}.conf"
        if not os.path.isfile(config_fpath_alt):
            raise FileNotFoundError(f"Configuration file {config_fpath} not found")
        else:
            config_fpath = config_fpath_alt
    config = configparser.ConfigParser()
    config.read(config_fpath)
    return config

def load_config_pathdata(corpus_name):
    config = _load_config(corpus_name)
    if "directories" not in config:
        message = "Configuration file has no [directories] section"
        raise AttributeError(message)
    dir_data = config["directories"]
    if "CORPUS_PATH" not in dir_data:
        message = "Configuration file has no CORPUS_PATH value"
        raise AttributeError(message)
    if "DATA_PATH" not in dir_data:
        message = "Configuration file has no DATA_PATH value"
        raise AttributeError(message)
    if "OUTPUT_PATH" not in dir_data:
        message = "Configuration file has no OUTPUT_PATH value"
        raise AttributeError(message)
    # Now we can safely get the values
    return dir_data["CORPUS_PATH"], dir_data["DATA_PATH"], dir_data["OUTPUT_PATH"]

def load_config_subnorms(corpus_name):
    config = _load_config(corpus_name)
    if "subjectnorm" not in config:
        message = "Configuration file has no [subjectnorm] section"
        raise AttributeError(message)
    subnorm_data = config["subjectnorm"]
    subnorm_dict = OrderedDict()
    # Now just turn the key-value pairs into a dict
    for cur_subnorm in subnorm_data:
        # cur_subnorm = The current normalized subject
        # Now get the non-normalized values
        sub_data = subnorm_data[cur_subnorm].split(",")
        #cur_dict = {cur_sub:cur_subnorm for cur_sub in sub_data}
        cur_dict = OrderedDict((cur_sub,cur_subnorm) for cur_sub in sub_data)
        subnorm_dict.update(cur_dict)
    return subnorm_dict

def load_txt_by_fpath(fpath, as_contract=False, encoding="utf-8"):
    """ Loads the plaintext, first by lines and then combined into a single
    string, and returns both """
    with codecs.open(fpath, 'r', encoding) as f:
        txt_lines = f.readlines()
        #cur_txt = cur_txt.translate(remove_punct)
    # Combine into a single string
    txt_str = "\n".join(txt_lines)
    if as_contract:
        return Contract(fpath, plaintext=txt_str, pt_lines=txt_lines)
    return txt_str

id_map = {e['contract_num']:e['contract_id'] 
          for e in pd.read_stata("id_crosswalk.dta").to_dict('records')}
def num_to_id(contract_num):
    return id_map[contract_num]

def readlines_by_fpath(fpath, encoding="utf-8"):
    with codecs.open(fpath, 'r', encoding) as f:
        txt_lines = f.readlines()
    return txt_lines

def remove_suffix(path):
    """
    Looks in a folder of suffixed files and determines the root filename (e.g.,
    a folder with contents ["coolfile_1.csv","coolfile_2.csv","coolfile_3.csv"]
    will produce "coolfile.csv").
    """
    first_fpath = glob.glob(os.path.join(path,"*"))[0]
    first_fname = os.path.basename(first_fpath)
    fname_elts = os.path.splitext(first_fname)
    prefix_elts = fname_elts[0].split("_")
    # Remove the last part of the filename, keeping the rest
    prefix_nonum = "_".join(prefix_elts[:-1])
    new_fname = prefix_nonum + fname_elts[1]
    return new_fname

def safe_append(row, csv_fpath, header=None, encoding='utf-8'):
    # Like safe_pickle but for appending: so, it checks if the file exists,
    # and if it doesn't it creates the file and adds row as first line,
    # otherwise it appends like normal
    # But first we have to ensure that the *directory* exists using safe_open_dir
    safe_open_dir(csv_fpath, is_fpath=True)
    if not os.path.isfile(csv_fpath):
        # First row. So have to *create* the file, and then put the
        # header in if it's not None
        file_buf = codecs.open(csv_fpath, 'w', encoding)
        writer = csv.writer(file_buf)
        if header:
            writer.writerow(header)
    else:
        file_buf = codecs.open(csv_fpath, 'a', encoding)
        writer = csv.writer(file_buf)
    writer.writerow(row)
    # Have to close the buffer since we're not using "with"
    file_buf.close()

# https://stackoverflow.com/questions/185936/how-to-delete-the-contents-of-a-folder-in-python
def safe_clear_path(path):
    # First make sure the path *exists*. If it doesn't, create it
    safe_open_dir(path, is_fpath=False)
    for the_file in os.listdir(path):
        file_path = os.path.join(path, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            #elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e:
            print(e)

def safe_open_dir(descriptor, is_fpath):
    # Helper function, used by the other safe_ functions to ensure a folder
    # exists before trying to save a *file* in it
    if is_fpath:
        # Have to get the parent dir
        leaf_dir = os.path.dirname(descriptor)
    else:
        # We're already at the leaf dir
        leaf_dir = descriptor

    if not os.path.isdir(leaf_dir):
        # Need to create the subdirs first
        print("Making dirs for " + leaf_dir + " to exist")
        os.makedirs(leaf_dir)

def safe_copy_file(old_fpath, new_fpath):
    safe_open_dir(new_fpath, is_fpath=True)
    copyfile(old_fpath, new_fpath)

def safe_dict_save(lda_dict, fpath):
    """ A very specific function that ensures the subdirectories exist before
    running gensim LDA's dictionary.save() serialization """
    safe_open_dir(fpath, is_fpath=True)
    lda_dict.save(fpath)

def safe_pickle(data, fpath, use_joblib=False):
    # Checks if filepath exists. If it does, then it saves the pickle.
    # Otherwise, it creates all the necessary directories *before* saving.
    # First get everyting up to the filename
    safe_open_dir(fpath,is_fpath=True)
    if use_joblib:
        joblib.dump(data, fpath)
    else:
        with open(fpath, 'wb') as f:
            pickle.dump(data, f)

def safe_read_data(data_fpath, **kwargs):
    # Checks the extension and then calls safe_read_csv or safe_read_pickle as
    # appropriate
    file_ext = os.path.basename(data_fpath).split(".")[-1]
    if file_ext == "csv":
        return pd.read_csv(data_fpath)
    elif file_ext == "dta":
        return pd.read_stata(data_fpath)
    elif file_ext == "pkl":
        return pd.read_pickle(data_fpath)
    else:
        raise InvalidFilenameException("safe_read_data() only supports "
            + "csv, dta, and pkl files. You called it with: " + str(data_fpath))

def safe_save_data(data, data_fpath, **kwargs):
    file_ext = os.path.basename(data_fpath).split(".")[-1]
    if file_ext == "csv":
        safe_to_csv(data, data_fpath, kwargs)
    elif file_ext == "dta":
        safe_to_stata(data, data_fpath, kwargs)
    elif file_ext == "pkl":
        safe_to_pickle(data, data_fpath, kwargs)
    else:
        raise InvalidFilenameException("safe_save_data() only supports "
            + "csv, dta, and pkl files. You called it with: " + str(data_fpath))

def safe_serialize_corpus(fpath, corpus):
    safe_open_dir(fpath, is_fpath=True)
    gensim.corpora.MmCorpus.serialize(fpath, corpus)

# "unique_id" can be "contract", "statement", or a custom list
def safe_to_csv(df, csv_fpath, **kwargs):
    #ensure_unique(df, unique_id)
    safe_open_dir(csv_fpath, is_fpath=True)
    df.to_csv(csv_fpath, **kwargs)
    #print("Saved csv to " + str(csv_fpath))

def safe_to_pickle(pkl_obj, pkl_fpath, **kwargs):
    """ If it's a pandas DataFrame, use its built-in to_pickle function. Otherwise
    use joblib to pickle it as a (generic) Python object """
    safe_open_dir(pkl_fpath, is_fpath=True)
    if isinstance(pkl_obj, pd.DataFrame):
        pkl_obj.to_pickle(pkl_fpath, **kwargs)
    else:
        joblib.dump(pkl_obj, pkl_fpath)

def safe_to_stata(df, stata_fpath, **kwargs):
    safe_open_dir(stata_fpath, is_fpath=True)
    df.to_stata(stata_fpath, **kwargs)

def safe_append_to_file(text, fpath, encoding="utf-8"):
    return safe_write_to_file(text, fpath, encoding=encoding, write_mode="a")

def safe_write_to_csv(csvrows, fpath, encoding="utf-8", write_mode="w"):
    with open(fpath, write_mode) as f:
        writer = csv.writer(f)
        writer.writerows(csvrows)

def safe_write_to_file(text, fpath, encoding="utf-8", write_mode="w"):
    """ Writes out to a file, making sure that it exists and is cleared first """
    safe_open_dir(fpath, is_fpath=True)
    with codecs.open(fpath, write_mode, encoding) as f:
        f.write(text)

def smart_tokenize(text, mode="doc_tokens", orig_str_index=0):
    """
    Uses regex to detect line breaks or spaces and then records the character
    spans in between them, so that we know exactly what character
    indices (in the original string) the extracted statements lie between. We
    need this to label the annotations "correctly" (in a way compatible with
    doccano's annotation format)

    Note that there are three delimiters used in the pipeline, specified via the
    `mode` argument:
    (1) "doc_tokens": Uses r'\w' (the default), which means split on any whitespace. So, it tokenizes the
        string as if it's a full document being split into *tokens*
    (2) "sent_tokens": Uses r'\s', which splits on any whitespace *besides* linebreaks (used to
        tokenize sentences out of various substrings of a full contract string)
    (3) "doc_lines": Uses r'\r\n'. means split *only* on linebreaks (used to split a contract into
        lines)
    """
    delim_map = {"doc_tokens":r'\w', "sent_tokens":r'\s', "doc_lines":r'(\r\n)|(\n)'}
    delimiter = delim_map[mode]
    all_tokens = []
    # Split on the given delimiter
    split_iter = re.finditer(delimiter,text)
    # Now for each line/word, i.e. each part of the sentence separated by a match,
    # figure out the char number and include it in the tokens list
    last_end = 0
    for split_obj in split_iter:
        cur_span = split_obj.span()
        cur_start = cur_span[0]
        # Now capture the token between last_end and cur_start
        raw_token = text[last_end:cur_start]
        # line_num is a bit of a weird key, so for words it'll be *token_num*
        if mode == "sent_tokens":
            # We're tokenizing a sentence into individual tokens. So this is the
            # "base case" if we're tokenizing a full contract
            completed_token = {'start_index': (orig_str_index+last_end),
                               'end_index': (orig_str_index+cur_start),
                               'text': raw_token}
        else:
            # We're tokenizing a *full contract* into lines
            completed_token = {'start_index': (orig_str_index+last_end),
                               'end_index': (orig_str_index+cur_start),
                               'text': raw_token}
            # But here we have to (*recursively*) split this line into tokens
            completed_token["tokens"] = smart_tokenize(completed_token["text"],
                                        mode="sent_tokens",
                                        orig_str_index=completed_token["start_index"])
        all_tokens.append(completed_token)
        cur_end = cur_span[1]
        last_end = cur_end
    # Need to add the final span manually (from last match to end)
    last_token = text[last_end:len(text)]
    if mode == "sent_tokens":
        completed_token = {'start_index': (orig_str_index+last_end),
                           'end_index': (orig_str_index+len(text)),
                           'text': last_token}
    else:
        # Need to complete the sentence tokenization *and* tokenize the final
        # sentence
        completed_token = {'start_index': (orig_str_index+last_end),
                           'end_index': (orig_str_index+len(text)),
                           'text': last_token}
        completed_token["tokens"] = smart_tokenize(completed_token["text"],
                                    mode="sent_tokens",
                                    orig_str_index=completed_token["start_index"])
    all_tokens.append(completed_token)
    return all_tokens

def sort_by_suffix(path):
    """
    Takes in a path and returns a list of the files inside of it *sorted by suffix*
    """
    fpath_list = glob.glob(os.path.join(path,"*"))
    def get_suffix(fpath):
        fname = os.path.basename(fpath)
        # Get the filename *without* the extension
        fprefix = os.path.splitext(fname)[0]
        final_part = fprefix.split("_")[-1]
        return int(final_part)
    fpath_data = [(get_suffix(fp),fp) for fp in fpath_list]
    # Sort by the first element in the tuple
    sorted_list = sorted(fpath_data, key=lambda x: x[0])
    return sorted_list

def stopwords_from_file(fpath):
    # Returns a list of words
    with codecs.open(fpath, 'r', 'utf-8') as f:
        words = f.read().split()
    return words
