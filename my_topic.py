# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    my_topic.py                                        :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/05/14 11:08:14 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/03/24 19:02:48 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import json
import pprint
import re
import string
from collections import Counter
from pprint import pprint

# Gensim
import gensim
import gensim.corpora as corpora
import matplotlib
import numpy as np
import pandas as pd
import scipy

# spacy for lemmatization
import spacy
from gensim.corpora import Dictionary
from gensim.models import CoherenceModel, LdaModel
from gensim.test.utils import datapath
from gensim.utils import simple_preprocess
from nltk.corpus import stopwords
from tqdm import tqdm
from tqdm import tqdm_notebook as tqdm

from fake_identify import Are_you_IRA
from my_weapon import *
from Trump_Clinton_Classifer.TwProcess import CustomTweetTokenizer

matplotlib.rcParams["font.size"] = 14
sns.set_style("darkgrid")
ira_c = sns.color_palette("coolwarm", 8)[7]
all_c = sns.color_palette("coolwarm", 8)[0]

Putin = Are_you_IRA()

nlp = spacy.load('en', disable=['parser', 'ner'])

tokenizer = CustomTweetTokenizer(preserve_case=False,
                                 reduce_len=True,
                                 strip_handles=False,
                                 normalize_usernames=False,
                                 normalize_urls=True,
                                 keep_allupper=False)

from nltk.corpus import stopwords
stop_words = stopwords.words('english')
stop_words.extend([
    "rt", "…", "...", "URL", "http", "https", "“", "”", "‘", "’", "get", "2", "new", "one", "i'm", "make",
    "go", "good", "say", "says", "know", "day", "..", "take", "got", "1", "going", "4", "3", "two", "n",
    "like", "via", "u", "would", "still", "first", "that's", "look", "way", "last", "said", "let",
    "twitter", "ever", "always", "another", "many", "things", "may", "big", "come", "keep",
    "5", "time", "much", "_", "cound", "-", '"'
])

stop_words.extend([',', '.', ':', ';', '?', '(', ')', '[', ']', '&', '!', '*', '@', '#', '$', '%',
])


class KTopic(object):
    def __init__(self):
        print("你若相信，它便存在。")


    def load_text(self):
        print("Loading ...")
        texts_out = []

        with open("data/ira_texts.txt") as f:
            for line in f:
                w = [w for w in line.strip().split() if w != "" and w != "%"]
                if w:
                    texts_out.append(w)

        # Create Dictionary
        self.id2word = corpora.Dictionary(texts_out)

        # Create Corpus
        self.texts = texts_out

        # Term Document Frequency
        self.corpus = [self.id2word.doc2bow(text) for text in texts_out]


    def load_model(self):
        self.lda_model = LdaModel.load("model/lda-ira-78.mod")


    def lemmatization(self, sent, allowed_postags=['NOUN', 'ADJ', 'VERB', 'ADV', 'PROPN']):
        """https://spacy.io/api/annotation"""
        sent = " ".join(sent)
        sent = re.sub(r'#(\w+)', r'itstopiczzz\1', sent)
        sent = re.sub(r'@(\w+)', r'itsmentionzzz\1', sent)
        doc = nlp(sent)
        
        _d = [token.lemma_ for token in doc if token.pos_ in allowed_postags and token.lemma_ not in stop_words and token.lemma_]
        
        _d = [x.replace('itstopiczzz', '#') for x in _d]
        _d = [x.replace('itsmentionzzz', '@') for x in _d]
        return _d
    

    def predict(self, text):
        text = text.replace("\n", " ").replace("\t", " ")
        words = tokenizer.tokenize(text)
        words = [w for w in words if w not in stop_words and w]
        text = self.lemmatization(words)
        text = self.id2word.doc2bow(text)
        return self.lda_model.get_document_topics(text)


    def run(self):
        for i in range(100):
            print(f"---------------------- {i} ----------------------")
            # Can take a long time to run.
            lda_model = gensim.models.ldamodel.LdaModel(corpus=self.corpus, id2word=self.id2word, num_topics=7, chunksize=1000)
            print(lda_model.print_topics())
            # Compute Perplexity
            print('Perplexity: ', lda_model.log_perplexity(self.corpus))  # a measure of how good the model is. lower the better.

            # Compute Coherence Score
            coherence_model_lda = CoherenceModel(model=lda_model, texts=self.texts, dictionary=self.id2word, coherence='c_v')
            coherence_lda = coherence_model_lda.get_coherence()
            print('Coherence Score: ', coherence_lda)
            
            lda_model.save(f"model/lda-ira-{i}.mod")


if __name__ == "__main__":
    Lebron = KTopic()
    Lebron.load_text()
    Lebron.run()
