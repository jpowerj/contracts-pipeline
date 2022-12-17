# Print and save the LDA topic list to a .txt file
import gensim

import pipeline_util as plu

def save_lda_topics(pl):
    lda_model_fpath = pl.get_lda_model_fpath()
    lda_model = gensim.models.LdaMulticore.load(lda_model_fpath)
    all_topics = lda_model.show_topics(num_topics=pl.num_topics)
    all_topics.sort(key=lambda x: x[0])
    output_buffer = ""
    for cur_topic in all_topics:
        topic_label = "Topic " + str(cur_topic[0]) + "\n"
        output_buffer += topic_label
        print(topic_label)
        topic_terms = cur_topic[1] + "\n\n"
        output_buffer += topic_terms
        print(topic_terms)
    plu.safe_write_to_file(output_buffer, pl.get_lda_output_fpath())