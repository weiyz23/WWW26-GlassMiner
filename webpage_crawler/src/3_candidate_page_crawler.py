# Merge the old and new candidate URLs, then crawl all the urls and save the results
# Import necessary libraries
import random
from urllib.parse import urljoin
import requests
import json
import os
import pickle as pkl
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial

# Import the customized content
from configs import *
from utils import *

def pre_deduplicate_by_url(candidate_url_list: list) -> list:
    """
    Deduplicate the LG page list by URL. We treat below URLs as the same:
    1. Two urls with only tailing slash difference: http://example.com/ and http://example.com
    2. Two urls with only different protocol: http://example.com and https://example.com
    3. TODO. more specific deduplication rules, but not necessary now.
    """
    # Load the available LG page list
    available_lg_page_list = json.load(open(os.path.join(SHARED_DATA_DIR, AVAI_FILE), "r"))
    downloaded_lg_url_set = set()
    for lg_info in available_lg_page_list:
        downloaded_lg_url_set.add(lg_info["url"])
    
    unsupported_cnt = 0
    preprocessed_lg_page_dict = {}
    # remove all the tailing slash, split the urls by protocol and domain
    for url in candidate_url_list:
        # other protocol, such as telnet, ssh, etc.
        if not url.startswith("http"):
            unsupported_cnt += 1
            continue
        try:
            proto, domain = url.split("://")
        except:
            continue
        domain = domain[:-1] if domain.endswith("/") else domain
        if domain not in preprocessed_lg_page_dict:
            preprocessed_lg_page_dict[domain] = {
                "proto": proto
            }
        elif proto == "http":
            preprocessed_lg_page_dict[domain]["proto"] = proto
    deduplicated_url_list = []
    for domain, info in preprocessed_lg_page_dict.items():
        url = info["proto"] + "://" + domain
        # if the url is already in the downloaded list, skip it
        if url in downloaded_lg_url_set:
            continue
        # if the url is already in the deduplicated list, skip it
        deduplicated_url_list.append(url)
    return deduplicated_url_list

def post_deduplicate_by_url(available_candidate_list: list) -> list:
    """
    Directly remove the duplicated LG pages by URL.
    """
    unique_url_set = set()
    for url in available_candidate_list:
        unique_url_set.add(url["url"])
    return list(unique_url_set)

def check_availabilty_and_download(lg_url_list: list):
    """
    Concurrently check the availability of LG pages.
    If the target couldn't response, return False.
    If the target can normally response, download the webpage and return True.
    Schedule the download task to avoid being blocked, and give one task at most 3 times.
    """
    succ_cnt = 0
    failed_cnt = 0
    processed_cnt = 0
    available_candidate_list = []
    failed_page_list = []
    
    lg_url_set = set(lg_url_list)
    # Allowing continuous download from the breakpoint
    # check the lg_url_list and remove the already downloaded urls
    downloaded_count = 0
    downloaded_lg_filepath_list = os.listdir(SAVE_DIR)
    filename_to_url_dict = {}
    for url in lg_url_list:
        filename = url_to_filename(url)
        filename_to_url_dict[filename] = url
    downloaded_set = set()
    for filename in downloaded_lg_filepath_list:
        if filename in filename_to_url_dict:
            downloaded_set.add(filename_to_url_dict[filename])
            available_candidate_list.append({
                "url": filename_to_url_dict[filename],
                "filename": filename,
            })
    lg_url_list = list(lg_url_set - downloaded_set)
    downloaded_count = len(downloaded_set)
    print(f"Already downloaded {downloaded_count} LG pages.")
    
    # random shuffle the list to avoid being blocked
    random.shuffle(lg_url_list)

    os.makedirs(SAVE_DIR, exist_ok=True)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        with requests.Session() as session:
            partial_crawl = partial(fetch_one_page, session=session)
            futures = {executor.submit(partial_crawl, lg): lg for lg in lg_url_list}
            
            for future in as_completed(futures):
                result = future.result()
                # update the success and failed count
                processed_cnt += 1
                if result['success']:
                    available_candidate_list.append({
                        "url": result['final_url'],
                        "filename": filename,
                    })
                    succ_cnt += 1
                else:
                    failed_cnt += 1
                    failed_page_list.append({
                        "url": result["original_url"], 
                        "err": str(result["error"]),
                    })
                if processed_cnt % 500 == 0:
                    print("{} processed, {} success, {} failed".format(processed_cnt, succ_cnt, failed_cnt))
    return available_candidate_list, failed_page_list

if __name__ == "__main__":
    os.makedirs(SAVE_DIR, exist_ok=True)
    candidate_list = pkl.load(open(os.path.join(OUTPUT_DIR, "new_candidate_urls.bin"), "rb"))
    print("Now Start deduplication...")
    dedup_candidate_list = pre_deduplicate_by_url(candidate_list)    
    print(f"Get {len(dedup_candidate_list)} candidate pages after deduplication.")
    print("Now Start checking the availability of candidate pages...")
    
    # For parallel download
    # index = 0
    # total_workers = 6
    # dedup_candidate_list = dedup_candidate_list[index::total_workers]
    # print(f"Get {len(dedup_candidate_list)} candidate pages for {index}th worker.")
    
    available_candidate_list, failed_lg_page_list = check_availabilty_and_download(dedup_candidate_list)    
    available_candidate_list = post_deduplicate_by_url(available_candidate_list)
    
    print("Got {} available candidate pages.".format(len(available_candidate_list)))
    with open(os.path.join(OUTPUT_DIR, CANDIDATE_FILE), "w") as f:
        json.dump(available_candidate_list, f, indent=4)