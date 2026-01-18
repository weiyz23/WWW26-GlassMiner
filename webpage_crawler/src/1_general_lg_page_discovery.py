# According to the corpus given by the previous step, we now use them to build search queries and crawl the webpages.
# The whole process includes three steps:
# 1. Data Preparation: Load the corpus, and the AS-Rank related information. Process the corpus and AS-Rank data.
# 2. Non-Targeted Crawling: Use the corpus in each cluster to build search queries and crawl the webpages.
# 3. Targeted Crawling: Use the AS-Rank information to build search queries and crawl the webpages.

from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import json
import pickle as pkl
import time
import json
from urllib.parse import quote_plus

from configs import *
from utils import *

def build_search_terms(dict_set_cluster_keywords, set_general_keywords):
    """
    Build the search terms for the search engine.
    Every time we choose one cluster, find all pairs of keywords in the cluster.
    Then we deduplicate all the pairs and using them as the search terms.
    """
    search_terms = set()
    for cluster_id, cluster_keywords in dict_set_cluster_keywords.items():
        # sort the keywords
        list_sorted_keywords = sorted(list(cluster_keywords))
        for i in range(len(list_sorted_keywords)):
            for j in range(i+1, len(list_sorted_keywords)):
                search_terms.add((list_sorted_keywords[i], list_sorted_keywords[j]))
    # Add the general keyword pairs
    list_sorted_general_keywords = sorted(list(set_general_keywords))
    for i in range(len(list_sorted_general_keywords)):
        for j in range(i+1, len(list_sorted_general_keywords)):
            search_terms.add((list_sorted_general_keywords[i], list_sorted_general_keywords[j]))
    return search_terms

def purify_the_corpus(dict_city_by_name):
    """
    Load the general corpus and the clustered corpus.
    Purify the corpus by removing the geolocation related terms.
    """
    dict_general_keyword_corpus = json.load(open(os.path.join(SHARED_DATA_DIR, "general_keyword_values.json"), "r"))
    dict_cluster_keyword_corpus = json.load(open(os.path.join(SHARED_DATA_DIR, "cluster_keyword_values.json"), "r"))
    # First, remove all the terms that are below the threshold
    set_raw_general_keywords = set([key for key, value in dict_general_keyword_corpus.items() if value > GENERAL_WEIGHT_THRESHOLD])
    dict_set_raw_cluster_keywords = {}
    for cluster_id, cluster_info in dict_cluster_keyword_corpus.items():
        dict_set_raw_cluster_keywords[cluster_id] = set([key for key, value in cluster_info.items() if value > CLUSTER_WEIGHT_THRESHOLD])
    
    print(f"Raw general keywords: {len(set_raw_general_keywords)}")
    
    cluster_info = json.load(open(os.path.join(SHARED_DATA_DIR, "hybrid_clusters.json"), "r"))
    
    # Second, remove all the geolocation related terms from both general and clustered corpus
    set_general_keywords = set()
    for keyword in set_raw_general_keywords:
        if keyword not in dict_city_by_name and len(keyword) < TERM_LEN_MAX_THRESHOLD:
            set_general_keywords.add(keyword)
            
    dict_set_cluster_keywords = {}
    for cluster_id, cluster_keywords in dict_set_raw_cluster_keywords.items():
        if len(cluster_info[cluster_id]) < CLUSTER_SIZE_THRESHOLD:
            continue
        set_cluster_keywords = set()
        for keyword in cluster_keywords:
            if keyword not in dict_city_by_name and len(keyword) < TERM_LEN_MAX_THRESHOLD:
                set_cluster_keywords.add(keyword)
        dict_set_cluster_keywords[cluster_id] = set_cluster_keywords
    
    print(f"Purified general keywords: {len(set_general_keywords)}")
    
    # Third, remove all the terms appeared in the clustered corpus from the general corpus
    for cluster_id, cluster_keywords in dict_set_cluster_keywords.items():
        set_general_keywords -= cluster_keywords
    
    return set_general_keywords, dict_set_cluster_keywords

def fetch_one_piece_of_webpages(list_terms, thread_index):
    # Sleep for index * 30 seconds to avoid the anti-crawler detection
    time.sleep(thread_index * 30)
    print('Thread ' + str(thread_index) + ' start')
    
    timer_start = time.time()
    browser = init_browser()
    candidate_urls = set()
    failed_terms = set()
    
    # Log the processed terms
    log_term_file = open(os.path.join(LOGS_DIR, 'log_terms_' + str(thread_index) + '.txt'), 'a')
    log_url_file = open(os.path.join(LOGS_DIR, 'log_urls_' + str(thread_index) + '.txt'), 'a')
    
    # Search for the urls
    count = 0
    for terms in list_terms:
        # Search term: key+looking+glass
        key = quote_plus(f'"{terms[0]}" "{terms[1]}" looking glass')
        tmp_urls = search_for_one_keyword(browser, key)
        
        if len(tmp_urls) == 0:
            print(f"Cannot find any urls for {key}")
            # write the terms to log file
            log_term_file.write(terms[0] + ' ' + terms[1] + '\n')
            # flush the buffer
            log_term_file.flush()
            failed_terms.add(terms)
        else:
            # write the urls to file
            for url in tmp_urls:
                log_url_file.write(url + '\n')
            # flush the buffer
            log_url_file.flush()
            candidate_urls.update(tmp_urls)

        count += 1
        if count % 10 == 0:
            print('Thread {} processed {} terms, {} left.'.format(thread_index, count, len(list_terms) - count))
 
    # close the log files
    log_term_file.close()
    log_url_file.close()
    browser.quit()
    timer_end = time.time()
    print('Thread {} end, time cost: {}, {} urls are collected'.format(thread_index, timer_end - timer_start, len(candidate_urls)))
    return candidate_urls, failed_terms

if __name__ == "__main__":
    dict_city_by_name = pkl.load(open(os.path.join(GEOLOCATION_DIR, "dict_city_by_name.bin"), "rb"))
    set_general_keywords, dict_set_cluster_keywords = purify_the_corpus(dict_city_by_name)
    # Build the search terms for the search engine
    search_terms = build_search_terms(dict_set_cluster_keywords, set_general_keywords)
    total_length = len(search_terms)
    print(f"Cluster search terms: {total_length}")
    
    os.makedirs(LOGS_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    # Split the cluster search terms into pieces
    list_cluster_search_terms = list(search_terms)
    
    # For distributed crawler usage.
    # index = 0
    # NUM_CRAWLER = 6
    # list_cluster_search_terms = list_cluster_search_terms[index*total_length//NUM_CRAWLER:(index+1)*total_length//NUM_CRAWLER]
    
    num_terms = len(list_cluster_search_terms)
    list_term_slices = [list_cluster_search_terms[i*num_terms // NUM_THREADS:(i+1)*num_terms // NUM_THREADS] for i in range(NUM_THREADS)]
    
    # Start searching for the webpages by using the cluster search terms
    # Parallelize the searching process, and use future to capture the results
    futures = []
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        for i in range(NUM_THREADS):
            future = executor.submit(fetch_one_piece_of_webpages, list_term_slices[i], i)
            futures.append(future)
        # Collect the results
        all_urls = set()
        all_failed_terms = set()
        for future in as_completed(futures):
            candidate_urls, failed_terms = future.result()
            all_urls.update(candidate_urls)
            all_failed_terms.update(failed_terms)
        # Save the results
        with open(os.path.join(OUTPUT_DIR, "candidate_urls.bin"), "wb") as f:
            pkl.dump(all_urls, f)
        with open(os.path.join(OUTPUT_DIR, "failed_terms.bin"), "wb") as f:
            pkl.dump(all_failed_terms, f)