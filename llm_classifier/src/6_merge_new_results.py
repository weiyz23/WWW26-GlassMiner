import hashlib
import json
import shutil
import time
import pandas as pd
import pickle as pkl
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from typing import Callable

from configs import *
from utils import *

def build_new_dataset(label=None):
    """
    Build the dataset for the LLM classifier.
    """
    with open(os.path.join(OUTPUT_DIR, "new_filtered_page_list.json"), "r") as f:
        filtered_page_list = json.load(f)
    set_finished_url = set()
    # Filter out all the finished samples, each line is an url
    with open(os.path.join(OUTPUT_DIR, "tmp_logs.txt"), "r") as f:
        lines = f.readlines()
        for line in lines:
            line = line.strip()
            url, _, _ = line.split("\t")
            set_finished_url.add(url)

    dataset = []
    for page_info in filtered_page_list:
        url = page_info["url"]
        if url in set_finished_url:
            continue
        text_path = os.path.join(PROCS_DIR, page_info["filename"])
        with open(text_path, "r", encoding="utf-8") as f:
            text = f.read()
            text = f"{url}: {text}"
            dataset.append((text, label, url, page_info["filename"]))
    return dataset

def non_batched_classification(dataset: list, method: Callable) -> pd.DataFrame:
    finish_count = 0
    start_time = time.time()
    task_idx = 0
        
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        future_set = set()
        result_log = []
        future_mapping = {}
        # Initial tasks
        for i in range(NUM_THREADS):
            data = dataset[task_idx]
            html_text, label, url, text_path = data
            task_idx += 1
            future = executor.submit(method, html_text)
            future_set.add(future)
            future_mapping[future] = (task_idx, label, url, text_path)
            time.sleep(0.3)
        
        # After one task complete, start another one
        while future_set:
            done, future_set = wait(future_set, return_when=FIRST_COMPLETED, timeout=120)
            
            if len(done) == 0:
                # If more than 2 mins without update, direct shut down the progress.
                break
            
            for future in done:
                result = future.result()
                if result is not None:
                    future_info = future_mapping[future]
                    #  task_idx, label, url, text_path, retry_count
                    result_log.append((result, future_info[1], future_info[2], future_info[3]))
                    
                    with open(os.path.join(OUTPUT_DIR, "tmp_logs.txt"), "a") as f:
                        f.write("{}\t{}\t{}\n".format(future_info[2], future_info[3], result))
                    
                    finish_count += 1
                    if finish_count % 100 == 0:
                        print("Finished {} tasks, time elapsed: {:.2f} seconds".format(finish_count, time.time() - start_time))
                    if task_idx < len(dataset):
                        data = dataset[task_idx]
                        html_text, label, url, text_path = data
                        task_idx += 1
                        future = executor.submit(method, html_text)
                        future_set.add(future)
                        future_mapping[future] = (task_idx, label, url, text_path)
                        time.sleep(0.3)
                else:
                    future_info = future_mapping[future]
                    origin_idx = future_info[0]
                    data = dataset[origin_idx]
                    html_text, label, url, text_path = data
                    future = executor.submit(method, html_text)
                    future_set.add(future)
                    future_mapping[future] = (origin_idx, label, url, text_path)
                    time.sleep(0.3)
        # Change res_mapping to pandas dataframe
        res_df = pd.DataFrame(result_log, columns=["result", "label", "url", "text_path"])
    return res_df

if __name__ == "__main__":
    dataset = build_new_dataset()
    print("Total dataset: ", len(dataset))
    random.shuffle(dataset)

    print("Start testing...")
    res_df = non_batched_classification(dataset, prompted_binary_classification)
    print("Prompted binary classification finished.")

    res_path = os.path.join(OUTPUT_DIR, "classification_result.pkl")
    if os.path.exists(res_path):
        with open(res_path, "rb") as f:
            res_df_old = pkl.load(f)
        res_df = pd.concat([res_df_old, res_df], ignore_index=True)
    # Save the result to file
    with open(res_path, "wb") as f:
        pkl.dump(res_df, f)

    # For each webpage with label 3, extract the result to build UNIQ_FILE
    res_3_df = res_df[res_df["result"] == 3]
    unique_lg_page_list = []
    for i in range(len(res_3_df)):
        url = res_3_df.iloc[i]["url"]
        text_path = res_3_df.iloc[i]["text_path"]
        filename = os.path.basename(text_path)
        # copy the src_path to the dst_path
        src_path = os.path.join(SAVE_DIR, filename)
        dst_path = os.path.join(VERIFIED_DIR, filename)
        try:
            shutil.copy(src_path, dst_path)
        except Exception as e:
            pass
        unique_lg_page_list.append({
            "url": url,
            "filename": filename,
        })
    with open(os.path.join(OUTPUT_DIR, UNIQ_FILE), "w") as f:
        json.dump(unique_lg_page_list, f, indent=2)
        
    # merge with the seed pages and de-duplication
    total_file_path = os.path.join(OUTPUT_DIR, TOTAL_FILE)
    # Load the seed pages and the unique pages, merge them
    original_clusters = json.load(open(os.path.join(SHARED_DATA_DIR, "hybrid_clusters.json"), "r")) 
    # Merge the two lists
    set_url = set()
    for lg_info in unique_lg_page_list:
        url = lg_info["url"]
        set_url.add(url)
    for cluster_id, cluster in original_clusters.items():
        set_url.update(cluster)
    total_lg_page_list = [{"url": url, "filename": url_to_filename(url)} for url in set_url]
    # Write back to file
    with open(total_file_path, "w") as f:
        json.dump(total_lg_page_list, f, indent=2)
    print(f"Total {len(total_lg_page_list)} unique URLs.")
    
    # de-duplication
    hash_to_urls = {}
    page_contents = {}
    # Calculate hash for each page
    count = 0
    for lg_info in total_lg_page_list:
        count += 1
        if count % 500 == 0:
            print(f"Processed {count} pages.")
        try:
            with open(os.path.join(PROCS_DIR, lg_info["filename"]), "r") as f:
                content = f.read()
        except:
            try:
                with open(os.path.join(SAVE_DIR, lg_info["filename"]), "r") as f:
                    html_text = f.read()
            except:
                print("download the file again")
                with requests.Session() as session:
                    fetch_one_page(lg_info["url"], session, 0)
                with open(os.path.join(SAVE_DIR, lg_info["filename"]), "r") as f:
                    html_text = f.read()
            content = collect_text_in_order(html_text)
            if not content:
                print(f"Error: {lg_info['filename']} is empty.")
                continue
            with open(os.path.join(PROCS_DIR, lg_info["filename"]), "w") as f:
                f.write(content)
                
        page_contents[lg_info["url"]] = content
        
        hash_val = hashlib.md5(content.encode()).hexdigest()
        if hash_val not in hash_to_urls:
            hash_to_urls[hash_val] = []
        hash_to_urls[hash_val].append(lg_info["url"])

    # Select shortest non-IP URL for each hash
    unique_urls = {min(urls, key=lambda url: (bool(re.search(r'\b([0-9]{1,3}\.){3}[0-9]{1,3}\b', url)), len(url)))
                for urls in hash_to_urls.values()}
    
    total_lg_page_list = [{"url": url, "filename": url_to_filename(url)} for url in unique_urls]
    print(f"Total {len(total_lg_page_list)} unique URLs after de-duplication.")
    # Write back to file
    with open(total_file_path, "w") as f:
        json.dump(total_lg_page_list, f, indent=2)