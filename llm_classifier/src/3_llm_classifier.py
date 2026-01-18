import json
import time
import shutil
import pandas as pd
import pickle as pkl
import random
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from typing import Callable

from configs import *
from utils import *

def build_dataset():
    """
    Build the dataset for the LLM classifier.
    """
    old_res_log = []
    with open(os.path.join(OUTPUT_DIR, "filtered_page_list.json"), "r") as f:
        filtered_page_list = json.load(f)
    set_finished_url = set()
    # Filter out all the finished samples, each line is an url
    with open(os.path.join(OUTPUT_DIR, "tmp_logs.txt"), "r") as f:
        lines = f.readlines()
        for line in lines:
            line = line.strip()
            url, filename, res = line.split("\t")
            set_finished_url.add(url)
            old_res_log.append((res, url , filename))
    old_res_df = pd.DataFrame(old_res_log, columns=["result", "url", "text_path"])

    dataset = []
    for page_info in filtered_page_list:
        url = page_info["url"]
        if url in set_finished_url:
            continue
        text_path = os.path.join(PROCS_DIR, page_info["filename"])
        with open(text_path, "r", encoding="utf-8") as f:
            text = f.read()
            text = f"{url}: {text}"
            dataset.append((text, url, page_info["filename"]))
    return dataset, old_res_df

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
            html_text, url, text_path = data
            task_idx += 1
            future = executor.submit(method, html_text)
            future_set.add(future)
            future_mapping[future] = (task_idx, url, text_path)
            time.sleep(0.2)
        
        # After one task complete, start another one
        while future_set:
            done, future_set = wait(future_set, return_when=FIRST_COMPLETED, timeout=60)
            
            if len(done) == 0:
                # If more than 2 mins without update, direct shut down the progress.
                break
            
            for future in done:
                result = future.result()
                if result is not None:
                    future_info = future_mapping[future]
                    #  task_idx, url, text_path, retry_count
                    result_log.append((result, future_info[1], future_info[2]))
                    
                    with open(os.path.join(OUTPUT_DIR, "tmp_logs.txt"), "a") as f:
                        f.write("{}\t{}\t{}\n".format(future_info[1], future_info[2], result))
                    
                    finish_count += 1
                    if finish_count % 100 == 0:
                        print("Finished {} tasks, time elapsed: {:.2f} seconds".format(finish_count, time.time() - start_time))
                    if task_idx < len(dataset):
                        data = dataset[task_idx]
                        html_text, url, text_path = data
                        task_idx += 1
                        future = executor.submit(method, html_text)
                        future_set.add(future)
                        future_mapping[future] = (task_idx, url, text_path)
                        time.sleep(0.2)
                else:
                    future_info = future_mapping[future]
                    origin_idx = future_info[0]
                    data = dataset[origin_idx]
                    html_text, url, text_path = data
                    future = executor.submit(method, html_text)
                    future_set.add(future)
                    future_mapping[future] = (origin_idx, url, text_path)
                    time.sleep(0.2)
        # Change res_mapping to pandas dataframe
        res_df = pd.DataFrame(result_log, columns=["result", "url", "text_path"])
    return res_df

if __name__ == "__main__":
    dataset, old_res_df = build_dataset()
    print("Total dataset: ", len(dataset))
    random.shuffle(dataset)

    print("Start testing...")
    res_df = non_batched_classification(dataset, prompted_binary_classification)
    print("Prompted binary classification finished.")
    
    # Merge the old result with the new result
    res_df = pd.concat([res_df, old_res_df], ignore_index=True)

    res_path = os.path.join(OUTPUT_DIR, "classification_result.pkl")
    # Save the result to file
    with open(res_path, "wb") as f:
        pkl.dump(res_df, f)

    os.makedirs(RELATED_DIR, exist_ok=True)
    os.makedirs(VERIFIED_DIR, exist_ok=True)
    # For each webpage with label 2, copy the src page -> RELATED_DIR
    res_2_df = res_df[res_df["result"] == 2]
    related_page_list = []
    for i in range(len(res_2_df)):
        url = res_2_df.iloc[i]["url"]
        text_path = res_2_df.iloc[i]["text_path"]
        filename = os.path.basename(text_path)
        # copy the src_path to the dst_path
        src_path = os.path.join(SAVE_DIR, filename)
        dst_path = os.path.join(RELATED_DIR, filename)
        try:
            shutil.copy(src_path, dst_path)
        except Exception as e:
            pass
        related_page_list.append({
            "url": url,
            "filename": filename,
        })
    with open(os.path.join(OUTPUT_DIR, RELATED_FILE), "w") as f:
        json.dump(related_page_list, f, indent=2)

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