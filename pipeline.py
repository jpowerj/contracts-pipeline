# Python imports
from collections import OrderedDict
import configparser
import glob
import os

# Local imports
import plutil

# 3rd party imports
import boto3
import gensim
import numpy as np
import spacy
import unidecode


class Pipeline:
    def __init__(self, corpus_name, config_fname=None, mode="plaintext", mode_options=None,
                 output_dirname=None, batch_mode="contract", lang_list=None,
                 sample_N=None, random_seed=1948, splitter="regex",
                 use_aws=False, num_lda_chunks=-1, num_lda_topics=20,
                 verbose=False):
        """
        Object containing data needed throughout the pipeline.

        :param corpus_name: str
            The name of the corpus, used for keeping track of data+output files
        :param mode: str
            's3' or 'plaintext'.
        :param mode_options:
            If `mode` == 's3', this specifies 'bucket' and 'prefix' keys.
            If `mode` == 'plaintext', this specifies 'plaintext_path'
        :param output_dirname: str, optional
            The subdirectory, *within* the Labor_Contracts_Canadian output
            directory, where the output files will be saved.
            By default, a directory is generated based on the timestamp of when
            the constructor is run.
        :param output_path: str, optional
            This parameter, unlike `output_dirname`, overrides the default output
            directory and instead saves everything into `output_path`
        :param lang_list: `list` of str, optional
        :param sample_N: int, optional
        :param random_seed: int, optional
        :param splitter: str, optional
        :param verbose: bool, optional
        """
        if lang_list is None:
            lang_list = ["eng"]
        self.verbose = verbose
        self.vprint = print if self.verbose else lambda x: None
        # Load the config file for this corpus
        self.corpus_name = corpus_name
        if config_fname is None:
            self.config_fpath = f"./configs/{corpus_name}.conf"
        else:
            self.config_fpath = f"./configs/{config_fname}"
        # And load the config info
        # (This gets loaded in the function)
        self.path_config = self.load_path_config()
        # There needs to be at least a CONTRACTS_ROOT and OUTPUT_PATH
        self.contracts_root = self.path_config["CONTRACTS_ROOT"]
        self.output_relpath = self.path_config["OUTPUT_PATH"]
        self.output_path = os.path.join(self.contracts_root, self.output_relpath)
        self.output_dirname = plutil.gen_timestamp_str()
        if output_dirname:
            self.output_dirname = output_dirname
        self.vprint(f"Full output path: {self.get_output_path()}")
        self.lang_list = lang_list
        if mode == "plaintext":
            self.plaintext_path = mode_options['plaintext_path']
            self.txt_fnames = []
            for cur_lang in self.lang_list:
                lang_fnames = self._get_pt_fnames_lang(cur_lang)
            self.txt_fnames.extend(lang_fnames)
        elif mode == "s3":
            # In this case, the config file also needs to have BUCKET_NAME and
            # BUCKET_PREFIX entries
            self.s3_bucket_name = self.path_config['BUCKET_NAME']
            self.s3_bucket_prefix = self.path_config['BUCKET_PREFIX']
        # Need to get all the plaintext fpaths here, since (a) we need lang_list
        # for language filtering and (b) we may need to sample from it later
        # (based on the value of sample_N)
        self.sample_N = sample_N
        self.random_seed = random_seed
        self.splitter = splitter
        if self.sample_N is not None:
            # Sample N contracts
            np.random.seed(self.random_seed)
            self.vprint(f"Sampling {sample_N} contracts")
            sample_fnames = np.random.choice(self.txt_fnames, self.sample_N, replace=False)
            # Now we can replace plaintext_fpaths
            self.txt_fnames = sample_fnames
            # And update the corpus name
            self.corpus_name = self.corpus_name + "_s" + str(self.sample_N)
        self.batch_mode = batch_mode
        # We don't initialize the spaCy model here (only as needed)
        self.spacy_model = None
        # But we do load the config file
        self.subnorm_dict = self.load_subnorm_dict()
        # And this allows us to construct a static stopword list
        self.stopwords = self.gen_stopword_list()
        self.use_aws = use_aws
        self.num_lda_chunks = num_lda_chunks
        self.num_lda_topics = num_lda_topics

    def gen_stopword_list(self):
        """
        This used to be in plutil.py, but moved here since it is
        actually run-specific (since the subjectnorm keys are themselves used
        as stopwords). The final list of stopwords is the union of NLTK
        stopwords, stopwords from lextek, and list of subjects
        """
        from nltk.corpus import stopwords
        nltk_stoplist = set(stopwords.words('english'))
        #if print_stopwords:
        #    print("NLTK stoplist: " + str(sorted(list(nltk_stoplist))))
        # Load the Lextek stopword list, if a path was given in the .conf file
        lextek_stoplist = set()
        if "LEXTEK_FPATH" in self.path_config:
            lextek_rel_fpath = self.path_config["LEXTEK_FPATH"]
            lextek_fpath = os.path.join(self.contracts_root, lextek_rel_fpath)
            lextek_stoplist = set(plutil.stopwords_from_file(lextek_fpath))
        #if print_stopwords:
        #    print("Lextek stoplist: " + str(sorted(list(lextek_stoplist))))
        subject_list = set(self.get_subnorm_list())
        #if print_stopwords:
        #    print("Filtered subjects: " + str(sorted(list(subject_list))))
        # Take the union of all the above lists for our final stopword list
        word_set = nltk_stoplist.union(lextek_stoplist).union(subject_list)
        return list(word_set)

    def get_artsplit_output_path(self, ext):
        return os.path.join(self.get_output_path(), f"01_artsplit_elliott_{ext}")

    def get_allsauth_output_fpath(self, ext):
        return os.path.join(self.get_output_path(), f"05_allsauth.{ext}")

    def get_cauth_output_fpath(self, ext):
        return os.path.join(self.get_output_path(), f"05_summed_auth.{ext}")

    def get_corpus_name(self):
        return self.corpus_name

    def get_instance_num(self):
        """
        If running on AWS, this returns the environment var we set which tells
        us which instance this is (from 1 to N)
        :return:
        """
        if not self.use_aws:
            return -1
        return os.getenv("INSTANCE_NUM")

    def get_lda_corpus_fpath(self):
        return os.path.join(self.get_output_path(), f"lda06_gensim_corpus.pkl")

    def get_lda_dict_fpath(self):
        return os.path.join(self.get_output_path(), f"lda02a_gensim_dict.pkl")

    def get_lda_doclist_fpath(self, chunk_num=-1):
        """
        If lda_batch_mode is "chunks", this requires passing in the chunk_num
        for the fpath. Otherwise, a value of -1 means just an fpath without a
        suffix

        :return:
        """
        chunk_suffix = f"_{chunk_num}" if chunk_num > -1 else ""
        return os.path.join(self.get_output_path(), f"lda03_doclist{chunk_suffix}.pkl")

    def get_lda_model_fpath(self):
        return os.path.join(self.get_output_path(), f"lda08_gensim_model.pkl")

    def get_lda_output_fpath(self):
        return os.path.join(self.get_output_path(), f"lda09_topic_list.txt")

    def get_liwc_path(self):
        return os.path.join(self.contracts_root, self.path_config["LIWC_PATH"])

    def get_merged_meta_fpath(self, ext):
        return os.path.join(self.get_output_path(), f"07_merged_meta.{ext}")

    def get_meta_fpath(self):
        return os.path.join(self.contracts_root, self.path_config["META_FPATH"])

    def get_num_docs(self):
        return len(self.txt_fnames)

    def get_num_lda_chunks(self):
        if not self.use_aws:
            return -1
        return self.num_lda_chunks

    def get_obranch_fpath(self, ext):
        return os.path.join(self.get_output_path(), f"lda01_obranch.{ext}")

    def get_output_path(self):
        """

        :return: str
            The absolute path to the output directory
        """
        return self.output_path

    def get_pdata_output_path(self):
        return os.path.join(self.get_output_path(), "03b_pdata_pkl")

    def get_preprocessed_fpath(self):
        """
        :return: The fpath for the preprocessed object branch documents
        """
        return os.path.join(self.get_output_path(), "lda02b_obranch_preprocessed.pkl")

    def get_preprocessed_df_fpath(self):
        return os.path.join(self.get_output_path(), "lda02c_preprocessed_df.pkl")

    def _get_pt_fnames_lang(self, lang):
        """ Get plaintext fnames for a specific language """
        pt_glob_str = os.path.join(self.plaintext_path, f"*_{lang}.txt")
        lang_fpaths = sorted(glob.glob(pt_glob_str))
        lang_fnames = [os.path.basename(fpath) for fpath in lang_fpaths]
        return lang_fnames

    def get_sauth_output_path(self):
        return os.path.join(self.get_output_path(), "04_sauth_pkl")

    def get_sdata_output_path(self):
        return os.path.join(self.get_output_path(), "03a_sdata_pkl")

    def get_spacy_output_path(self):
        return os.path.join(self.get_output_path(), "02_spacy_pkl")

    def get_spacy_model(self):
        """
        Annoying but necessary additional step: adding "contract_id" and
        "art_num" attributes to spacy's Doc class, so that we can serialize and
        deserialize without headaches

        See https://spacy.io/usage/processing-pipelines#custom-components-attributes

        All the neuralcoref attributes for a doc, for future reference:
        * `cr_test_doc._.has_coref`
        * `cr_test_doc._.coref_resolved`
        * `cr_test_doc._.coref_clusters`
        * `cr_test_doc._.coref_scores`

        And code for looping over the clusters:
        ```
        for cluster in cr_test_doc._.coref_clusters:
            print(f"===== #{cluster.i}")
            print(cluster)
            print(f"main: '{cluster.main}'")
            print(cluster.mentions)
            for mention in cluster.mentions:
                print(mention)
                print(mention.start)
                print(mention.end)

        :return: :obj:
            The spaCy model with custom `contract_id`, `lang`, and `art_num` fields
        """
        if self.spacy_model is None:
            self.vprint("Loading spaCy core model")
            nlp_eng = spacy.load('en_core_web_md', disable=["ner"])
            # The force=True is just so that we can change (e.g.) the names or
            # default values and overwrite the extensions (otherwise this would
            # always cause an Exception)
            #spacy.tokens.Doc.set_extension("contract_id", default=None, force=True)
            #spacy.tokens.Doc.set_extension("lang", default=None, force=True)
            #spacy.tokens.Doc.set_extension("art_num", default=None, force=True)
            self.spacy_model = nlp_eng
        return self.spacy_model

    def get_subnorm_list(self):
        """
        Returns only the *unique values* of the full `subnorm_dict`.

        :return:
        """
        return list(set(self.subnorm_dict.values()))

    def get_sumstats_fpath(self, ext):
        return os.path.join(self.get_output_path(), f"06_sumstats.{ext}")

    def init_plaintext_list(self):
        """
        Call this at the beginning, to load the full list of plaintext files
        from S3 and exclude those that arent' in lang_list

        :return:
        """
        self.vprint(f"Loading filenames from {self.s3_bucket_name}/{self.s3_bucket_prefix}...")
        self.txt_fnames, self.excluded_fnames = plutil.get_s3_contents(self.s3_bucket_name, self.s3_bucket_prefix,
                                                                       lang_list=self.lang_list, return_excluded=True,
                                                                       verbose=self.verbose)
        self.vprint(f"{len(self.txt_fnames)} filenames loaded ({self.txt_fnames[0]} ... {self.txt_fnames[-1]})")

    def _load_config(self):
        """ Sort of a helper function, used by both _paths and _subnorm versions.
        Loads configuration data from <corpus_name>.conf file within
        the "configs" directory and returns ConfigParser object """
        self.vprint(f"Looking for config file {self.config_fpath}")
        if not os.path.isfile(self.config_fpath):
            # Try one more time, this time in just the main code directory
            config_fpath_alt = f"{self.corpus_name}.conf"
            if not os.path.isfile(config_fpath_alt):
                raise FileNotFoundError(f"Configuration file {self.config_fpath} not found")
            else:
                self.config_fpath = config_fpath_alt
        config = configparser.ConfigParser()
        config.read(self.config_fpath)
        return config

    def load_path_config(self):
        config = self._load_config()
        if "directories" not in config:
            message = "Configuration file has no [directories] section"
            raise AttributeError(message)
        path_config = config["directories"]
        return path_config

    def load_subnorm_dict(self):
        config = self._load_config()
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
            # cur_dict = {cur_sub:cur_subnorm for cur_sub in sub_data}
            cur_dict = OrderedDict((cur_sub, cur_subnorm) for cur_sub in sub_data)
            subnorm_dict.update(cur_dict)
        return subnorm_dict

    def normalize_subject(self, subject):
        # It would be nice if we could just use subnorm_map on its own rather
        # than a function, but we need to transform to "other" if it's not a
        # key in subnorm_map, so alas, an if statement
        # (Also, we lowercase the subject before checking. Learned the hard way)
        # Get the string representation in case it's a weird format (like NaN)
        subject = str(subject)
        # Make sure it's in normal Python-friendly unicode
        subject = unidecode.unidecode(subject)
        # Lowercase
        subject = subject.lower()
        # Transform plural to singular (if plural)
        subject = plutil.get_singular(subject)
        if subject in self.subnorm_dict:
            return self.subnorm_dict[subject]
        else:
            return "other"

    def parse_articles(self):
        import main02_parse_articles
        main02_parse_articles.parse_articles(self)

    def preprocess_text(self, contract_str):
        """
        Uses gen_stopwords() and gensim's preprocess_string() method to perform
        the full preprocessing pipeline on contract_str.

        Added 2019-03-07 by JJ
        """
        # First we should remove our custom stopwords
        filtered_str = " ".join(w for w in contract_str.split() if w not in self.stopwords)
        # Now use gensim's parsing library
        final_doc = gensim.parsing.preprocess_string(filtered_str)
        return final_doc

    def split_contracts(self):
        # Import the contract splitting code
        import main01_split_contracts
        # Do the splitting
        main01_split_contracts.split_contracts(self)
