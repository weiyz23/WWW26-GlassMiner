# Analyse the corpus for the webpages in each cluster.
# First we use unrelated corpus and all the LG corpus to find the general keyword set.
# Then we use differential analysis for each cluster to find unique corpus, and compared them with the remaining cluster corpus. 
# We remove the intersecting parts with the overall keywords from the general keyword set.

# Import the required libraries
import json
import sys

import numpy as np
import pandas as pd
import pickle as pkl
import regex as re

# Import the customized content
from configs import *
from utils import *

from sklearn.datasets import fetch_20newsgroups
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer

def fetch_unrelated_corpus(count=15000):
    """
    Fetch the unrelated corpus from the 20 newsgroups dataset.
    If not exist, download the dataset.
    """
    list_doc = None
    try:
        with open(os.path.join(OUTPUT_DIR, "corpus_unrelated.bin"), "rb") as f:
            list_doc = pkl.load(f)
    except:
        dataset = fetch_20newsgroups(shuffle=True, random_state=1,
                                    remove=('headers', 'footers', 'quotes'))
        data_samples = dataset.data[:count]
        list_doc = [tokinize_text(desc) for desc in data_samples]
        with open(os.path.join(OUTPUT_DIR, "corpus_unrelated.bin"), "wb") as f:
            pkl.dump(list_doc, f)
    return list_doc

def load_clustered_corpus(dict_related_corpus: dict):
    """
    Load the clustered results, and then split the related corpus into different clusters.
    """
    cluster_results = json.load(open(os.path.join(OUTPUT_DIR, "hybrid_clusters.json"), "r"))
    dict_cluster_corpus = {}
    for cluster_id, cluster_info in cluster_results.items():
        dict_cluster_corpus[cluster_id] = [dict_related_corpus[url] for url in cluster_info]
    return dict_cluster_corpus


def modified_tfidf(list_related: list[list[str]], list_unrelated: list[list[str]]):
    """
    This is a modified version of the TF-IDF algorithm.
    1. Using the merged corpus, we generate the vectorizer only, and donot fit the transformer.
    2. Then we calculate the TF only use the related corpus, and calculate the IDF for the words in related corpus use the unrelated corpus.
    Therefore, we can calculate and show which words are truly unique in the related corpus.
    """
    # Note: the input documents are already tokenized words list
    list_doc = list_related + list_unrelated
    list_doc = [" ".join(doc) for doc in list_doc]
    vectorizer = CountVectorizer()
    X = vectorizer.fit_transform(list_doc)
    transformer = TfidfTransformer()
    tfidf = transformer.fit_transform(X)
    tfidf = tfidf.toarray()
    # Calculate the TF for the words in related corpus
    tf_related = tfidf[:len(list_related)]
    # Calculate the IDF for the words in unrelated corpus
    idf_unrelated = np.log((len(list_unrelated) + 1) / (np.sum(tfidf[len(list_related):] > 0, axis=0) + 1))
    # Calculate the TF-IDF for the words in related corpus
    tfidf_related = tf_related * idf_unrelated
    total_tfidf = np.sum(tfidf_related, axis=0)
    
    # Sort the words by the TF-IDF value, and get the dictionary
    dict_tfidf = {word: total_tfidf[i] for i, word in enumerate(vectorizer.get_feature_names_out())}
    dict_tfidf = {k: v for k, v in sorted(dict_tfidf.items(), key=lambda item: item[1], reverse=True)}
    return dict_tfidf

def analyse_clustered_keywords(clustered_corpus):
    """
    Analyse the keywords for each cluster.
    """
    clustered_count = 0
    dict_cluster_keyword_values = {}
    for cluster_id, list_cluster_corpus in clustered_corpus.items():
        # merge the other clusters
        list_other_corpus = []
        for other_cluster_id, other_cluster_corpus in clustered_corpus.items():
            if other_cluster_id != cluster_id:
                list_other_corpus.extend(other_cluster_corpus)
        dict_total_tfidf = modified_tfidf(list_cluster_corpus, list_other_corpus)
        # Compute the weighted TF-IDF value
        total_tfidf = np.sum(list(dict_total_tfidf.values()))
        dict_total_tfidf = {k: v / total_tfidf for k, v in dict_total_tfidf.items()}
        # Remove the words with value lower than CLUSTER_WEIGHT_THRESHOLD
        dict_total_tfidf = {k: v for k, v in dict_total_tfidf.items() if v > CLUSTER_WEIGHT_THRESHOLD}
        dict_cluster_keyword_values[cluster_id] = {k: v for k, v in sorted(dict_total_tfidf.items(), key=lambda item: item[1], reverse=True)[:15]}
        clustered_count += 1
        if clustered_count % 50 == 0:
            print(f"{clustered_count} clusters have been processed.")
    return dict_cluster_keyword_values
    

if __name__ == '__main__':
    list_unrelated_corpus = fetch_unrelated_corpus()
    dict_related_corpus = pkl.load(open(os.path.join(OUTPUT_DIR, "tokinized_content.bin"), "rb"))
    list_related_corpus = list(dict_related_corpus.values())
    dict_total_tfidf = modified_tfidf(list_related_corpus, list_unrelated_corpus)
    # Calculate the weighted of the TF-IDF value, rather than the absolute value
    total_tfidf = np.sum(list(dict_total_tfidf.values()))
    dict_total_tfidf = {k: v / total_tfidf for k, v in dict_total_tfidf.items()}
    # Remove the words with value lower than GENERAL_KEYWORD_THRESHOLD
    dict_total_tfidf = {k: v for k, v in dict_total_tfidf.items() if v > GENERAL_WEIGHT_THRESHOLD}
    # Sort the words by the weighted TF-IDF value, and get the dictionary
    general_keywords = set(dict_total_tfidf.keys())
    dict_general_keyword_values = {k: v for k, v in sorted(dict_total_tfidf.items(), key=lambda item: item[1], reverse=True)}

    print("Finish the general keyword analysis.")
    
    # Then we conduct differential analysis for each cluster
    clustered_corpus = load_clustered_corpus(dict_related_corpus)
    dict_cluster_keyword_values = analyse_clustered_keywords(clustered_corpus)
    # # Remove the intersecting parts with the overall keywords from the general keyword set
    # for cluster_id, cluster_keyword_values in dict_cluster_keyword_values.items():
    #     cluster_keywords = set(cluster_keyword_values.keys())
    #     general_keywords -= set(cluster_keywords)
    # Store the results
    with open(os.path.join(OUTPUT_DIR, "cluster_keyword_values.json"), "w") as f:
        json.dump(dict_cluster_keyword_values, f)
    with open(os.path.join(OUTPUT_DIR, "general_keyword_values.json"), "w") as f:
        json.dump(dict_general_keyword_values, f)
        
    print("Finish the cluster keyword analysis.")