import datetime
import json
import os

# 3rd party imports
import boto3
import inflect
import joblib
import pandas as pd
from tqdm import tqdm

inflecter = inflect.engine()

class ArtMeta:
    """
    Just a quick wrapper class to make sure that we always have consistent
    metadata for each article within a contract
    """
    def __init__(self, contract_id, lang, art_num):
        self.contract_id = contract_id
        self.lang = lang
        self.art_num = art_num

    def get_contract_id(self):
        return self.contract_id

    def get_lang(self):
        return self.lang

    def get_art_num(self):
        return self.art_num


class DocMeta:
    """
    Just a quick wrapper class to make sure that we always have consistent
    metadata for each contract
    """
    def __init__(self, contract_id, lang):
        """

        """
        self.contract_id = contract_id
        self.lang = lang

    def gen_fname(self, ext):
        return f"{self.contract_id}_{self.lang}.{ext}"

    def get_contract_id(self):
        return self.contract_id

    def get_lang(self):
        return self.lang

def gen_timestamp_str():
    """
    Generates a string based on the current-time timestamp (for use in generating
    output directories)

    :return: str
    """
    return str(datetime.date.today()).replace("-", "")


def get_s3_bucket(bucket_name):
    """
    Returns a boto3 Bucket object for the bucket with the provided name

    :param bucket_name: str
        The name of the s3 bucket.
    :return: :obj:
        The corresponding boto3 Bucket object.
    """
    session = boto3.Session()
    boto_s3 = session.resource('s3')
    bucket_obj = boto_s3.Bucket(bucket_name)
    return bucket_obj


def get_s3_contents(bucket_name, bucket_prefix, lang_list=None,
                    return_excluded=True, verbose=False):
    """
    Given a boto3 bucket object and a prefix, returns a list of the filenames
    within this bucket+prefix.

    :param bucket_name: str
        The name of the s3 bucket
    :param bucket_prefix: str
        The prefix (basically the path) of the directory within the bucket
    :param lang_list: `list` of str, optional
        List of 3-letter language codes we want to keep in the pipeline.
        If not provided, all files are returned without filtering on language.
    :param return_excluded: bool, optional
        If True, returns both the remaining fnames *and* the filtered-out fnames
        as a separate object.
    :param verbose: bool, optional
    :return: `list` of str
        The filenames for each object within the bucket+prefix (besides the "."
        object).
    """
    vprint = print if verbose else lambda x: None
    bucket_obj = get_s3_bucket(bucket_name)
    all_fpaths = [file_obj.key
                  for file_obj in tqdm(bucket_obj.objects.filter(Prefix=bucket_prefix))
                  if ".txt" in file_obj.key]
    # Since objects.filter() gives us file *paths*, we need to extract just the names now
    all_fnames = [os.path.basename(fp) for fp in all_fpaths]
    # Filter on language, if needed
    if lang_list:
        orig_len = len(all_fnames)
        filtered_fnames = [fn for fn in all_fnames if any([fn.endswith(f"_{lang}.txt") for lang in lang_list])]
        filtered_len = len(filtered_fnames)
        vprint(f"Total files: {orig_len}; files after language filter ({lang_list}): {filtered_len}")
        # And get the excluded fnames if needed
        if return_excluded:
            excluded_fnames = list(set(all_fnames) - set(filtered_fnames))
            return filtered_fnames, excluded_fnames
    return all_fnames


def list_to_regex(word_list):
    """ Used to convert LIWC word lists into regular expressions """
    wildcard_reg = [w.replace('*', r'[^\s]*') for w in word_list]
    reg_str = r'\b(' + '|'.join(wildcard_reg) + r')\b'
    return reg_str


def _make_dirs(fpath):
    """
    Helper function that ensures the directories exist, to avoid Exceptions

    :param fpath:
    :return: None
    """
    # First convert the (potentially) relative path to absolute
    abs_fpath = os.path.abspath(fpath)
    # Now call makedirs on it
    dirname = os.path.dirname(abs_fpath)
    if not os.path.isdir(dirname):
        os.makedirs(dirname)


def parse_fname(fname):
    """
    Takes a contract filename, of the form <id>_<lang>.<ext>, and returns these
    pieces of info as a dict.

    :param fname: The filename (NOT filepath) of the contract.
    :return: dict
        A dictionary with keys 'id', 'lang', and 'ext'
    """
    fname_elts = os.path.splitext(fname)
    fname_prefix = fname_elts[0]
    prefix_elts = fname_prefix.split("_")
    fname_data = {
        'prefix': fname_elts[0],
        'ext': fname_elts[-1],
        'id': prefix_elts[0],
        'lang': prefix_elts[1]
    }
    return fname_data


def safe_to_json(data, json_fpath):
    """
    Saves `data` to `json_fpath` using Python's `json` library, ensuring first
    that all of the directories in `json_fpath` exist.
    :param data:
    :param json_fpath:
    :return: None
    """
    _make_dirs(json_fpath)
    with open(json_fpath, 'w', encoding='utf-8') as outfile:
        json.dump(data, outfile)

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


def safe_load_csv(csv_fpath):
    return pd.read_csv(csv_fpath)

def safe_load_pickle(pkl_fpath):
    """
    Loads from `pkl_fpath` using the same method as `safe_to_pickle()`.

    :param pkl_fpath:
    :return: `data`
    """
    return joblib.load(pkl_fpath)


def safe_to_csv(df, csv_fpath, **kwargs):
    _make_dirs(csv_fpath)
    df.to_csv(csv_fpath, **kwargs)


def safe_to_pickle(data, pkl_fpath):
    """
    Saves `data` to `pkl_fpath` using the `joblib` library, ensuring first
    that all of the directories in `pkl_fpath` exist.
    :param data:
    :param pkl_fpath:
    :return: None
    """
    _make_dirs(pkl_fpath)
    # Now we should be able to save it without error (even if
    # the directory/directories didn't exist before)
    joblib.dump(data, pkl_fpath)


def safe_to_stata(df, stata_fpath):
    _make_dirs(stata_fpath)
    df.to_stata(stata_fpath)


def stopwords_from_file(fpath):
    """
    
    :return: A list of words loaded from the file at `fpath`
    """
    with open(fpath, 'r', encoding='utf-8') as infile:
        words = infile.read().split()
    return words
