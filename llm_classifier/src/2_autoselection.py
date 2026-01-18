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

feat_logs = {}

def request_llm_and_get_response(payload):
    """
    Request the LLM and get the response.
    """
    retry_count = 0
    res = None
    while res is None:
        try:
            response = requests.request("POST", API_URL, json=payload, headers=API_HEADER)
            response_dict = response.json()
            content = response_dict["choices"][0]["message"]["content"]
            # find four "\d-\d" in the content
            feat_pattern = r"\d-\d"
            res = re.findall(feat_pattern, content)
        except Exception as e:
            res = None
            retry_count += 1
            time.sleep(retry_count)
            if retry_count > 5:
                break
    feat = [0, 0, 0, 0]
    if res:
        # convert the result to a list of integers
        for item in res:
            index, value = item.split("-")
            index = int(index) - 1
            value = int(value)
            feat[index] = value
    return feat        

def interpret_one_page(data):
    """
    Interpret the webpage content using the LLM.
    """
    html_text, sample_prompt = data[0], data[1]
        
    new_base_prompt = {
        "model": "Pro/deepseek-ai/DeepSeek-V3",
        "stream": False,
        "max_tokens": 256,
        "temperature": 0.5,
        "top_p": 0.7,
        "top_k": 50,
        "frequency_penalty": 0.5,
        "n": 1,
        "messages": []
    }
    
    new_base_prompt["messages"].append({
        "content": sample_prompt,
        "role": "system"
    })
    
    instruction_prompt = """Analyze the given webpage content, where hyperlinks are formatted as `[text](link)`. Identify the following features and determine whether it is a Looking Glass service page. Each feature should be marked as 1 if present, and 0 otherwise. A value of 1 indicates a stronger likelihood that the page provides service.
Features:
1. Contains measurement command, "looking glass" or "hyperglass" keywords
2. Above keywords appear outside `[]` (not just links)
3. Not a Whois information page.
4. Is a Looking Glass service page.
Output Format:
1-1
2-1
3-0
4-0
"""

    input_prompt = """Input:
{}""".format(html_text)

    new_base_prompt["messages"].append({
        "content": instruction_prompt,
        "role": "system"
    })
    new_base_prompt["messages"].append({
        "content": sample_prompt,
        "role": "system"
    })
    new_base_prompt["messages"].append({
        "content": input_prompt,
        "role": "user"
    })
    
    result = request_llm_and_get_response(new_base_prompt)
    return result

def criticize_one_page(data):
    """
    Criticize the webpage content using the LLM.
    """
    html_text, original_features, correct_label = data[0], data[1], data[2]
    new_base_prompt = {
        "model": "Pro/deepseek-ai/DeepSeek-V3",
        "stream": False,
        "max_tokens": 256,
        "temperature": 0.5,
        "top_p": 0.7,
        "top_k": 50,
        "frequency_penalty": 0.5,
        "n": 1,
        "messages": []
    }
    system_prompt = """Task:
You are given a webpage's content, a list of extracted features (in the form of 1/0), each 1 indicates stronger evidence for an Looking Glass page, and the correct label indicating whether the page is a LG service. Your task is to:
1. Analyze whether the provided features are consistent with the content, and identify which features may have been misjudged.
2. Suggest corrected featurees based on the content, and output them in the same index-value format.
Features:
1. Contains commands, "looking glass" or "hyperglass" keywords
2. Above keywords appear outside `[]` (not just links)
3. Not a Whois information page.
4. Is a Looking Glass service page.
Input:
Webpage content:
{}
Original extracted features:
1-{}
2-{}
3-{}
4-{}
Correct label: 4-{}
Output the same index-value format only. No more explanations.""".format(
        html_text,
        original_features[0],
        original_features[1],
        original_features[2],
        original_features[3],
        correct_label
    )
    new_base_prompt["messages"].append({
        "content": system_prompt,
        "role": "user"
    })
    
    
    result = request_llm_and_get_response(new_base_prompt)
    return result

def non_batched_classification(dataset: list, method: Callable) -> pd.DataFrame:
    finish_count = 0
    start_time = time.time()
    task_idx = 0
    result_list = [[] for _ in range(len(dataset))]
    
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        future_set = set()
        future_mapping = {}
        # Initial tasks
        for i in range(NUM_THREADS):
            data = dataset[task_idx]
            task_idx += 1
            future = executor.submit(method, data)
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
                    task_idx = future_info[0]
                    result_list[task_idx - 1] = result
                    finish_count += 1
                    if finish_count % 50 == 0:
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
    return result_list

def format_feature(sample: list) -> str:
    """
    Format the features to a string.
    """
    feature_str = ""
    if sample["feature"] is None:
        return "... 4-{}".format(sample["label"])
    for i, feature in enumerate(sample["feature"]):
        feature_str += "{}-{}\n".format(i + 1, feature)
    return feature_str

def interpreter_model(dataset: list, samples: list) -> pd.DataFrame:
    """
    Interpret the dataset using the interpreter model, then parse the result to get features.
    """
    original_features = []    
    example_prompt = """Examples:
`Sample 1: {} -> {}`
`Sample 2: {} -> {}`
`Sample 3: {} -> {}`
`Sample 4: {} -> {}`""".format(
        samples[0][1]["content"], format_feature(samples[0][1]),
        samples[1][1]["content"], format_feature(samples[1][1]),
        samples[2][1]["content"], format_feature(samples[2][1]),
        samples[3][1]["content"], format_feature(samples[3][1])
    )        
    
    input_dataset = [(example_prompt, sample["content"]) for sample in dataset]
    results = non_batched_classification(input_dataset, interpret_one_page)

    for idx, result in enumerate(results):
        sample = dataset[idx]
        one_feature = {
            "url": sample["url"],
            "text_path": sample["text_path"],
            "label": sample["label"],
            "content": sample["content"],
            "feature": result
        }
        original_features.append(one_feature)
    return orginial_features

def criticize_model(err_samples: list) -> list:
    """
    According to the result of the interpreter model, criticize the dataset.
    Get the refined samples and the corresponding features.
    """
    revised_samples = []
    input_dataset = []
    for index_sample in err_samples:
        index, sample = index_sample[0], index_sample[1]
        input_dataset.append((sample["content"], sample["feature"], sample["label"]))
    result = non_batched_classification(input_dataset, criticize_one_page)
    for idx, result in enumerate(result):
        original_idx = err_samples[idx][0]
        sample = err_samples[idx][1]
        one_feature = {
            "url": sample["url"],
            "text_path": sample["text_path"],
            "label": sample["label"],
            "content": sample["content"],
            "feature": result
        }
        revised_samples.append((original_idx, one_feature))
    return revised_samples

def diff_and_get_new_sample(revised_samples: list, err_samples: list) -> pd.DataFrame:
    """
    Compare the new result with the old result, calculate the difference.
    """
    max_diff = 0
    max_diff_idx = None
    for idx, sample in enumerate(revised_samples):
        original_sample = err_samples[idx][1]
        revised_sample = sample[1]
        label_diff = abs(original_sample["label"] - revised_sample["label"]) * 2
        feat_diff = sum(abs(original_sample["feature"] - revised_sample["feature"]))
        diff = label_diff + feat_diff
        if diff > max_diff:
            max_diff = diff
            max_diff_idx = idx
    # If the max_diff is 0, then return the original sample
    max_diff_sample = revised_samples[max_diff_idx]
    return max_diff_sample

def select_new_few_shots(err_samples: list, few_shot_samples: list, new_sample: tuple) -> pd.DataFrame:
    """
    Select each shot from the few_shot_samples samples, replace it with the new sample.
    Then check the classification result of the interpreter model on those erroneous samples.
    Choose the one replacement that has the best classification result.
    """
    # if the new sample is the same as the old sample, directly replace it
    for index, sample in enumerate(few_shot_samples):
        if sample[0] == new_sample[0]:
            few_shot_samples[index] = new_sample
            return few_shot_samples
    best_index = None
    max_recall = 0
    max_precision = 0
    input_error_samples = [err_sample[1] for err_sample in err_samples]
    for index, sample in enumerate(few_shot_samples):
        # replace the sample with the new sample
        new_few_shot_samples = few_shot_samples.copy()
        new_few_shot_samples[index] = new_sample
        # interpret the dataset again
        new_features = interpreter_model(input_error_samples, new_few_shot_samples)
        precision, recall = calcualte_metrics(new_features)
        if recall > max_recall or (recall == max_recall and precision > max_precision):
            max_recall = recall
            max_precision = precision
            best_index = index
    few_shot_samples[best_index] = new_sample
    return few_shot_samples

def calcualte_metrics(features: list) -> tuple:
    """
    Calculate the precision and recall of the result.
    """
    fp = 0
    fn = 0
    tp = 0
    tn = 0
    for sample in features:
        if sample["result"] == 1 and sample["label"] == 1:
            tp += 1
        elif sample["result"] == 1 and sample["label"] == 0:
            fp += 1
        elif sample["result"] == 0 and sample["label"] == 1:
            fn += 1
        elif sample["result"] == 0 and sample["label"] == 0:
            tn += 1
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    return precision, recall

if __name__ == "__main__":
    # including: [{url, text_path, content, label}]
    with open(os.path.join(OUTPUT_DIR, "representative_dataset.pkl"), "rb") as f:
        rep_dataset = pkl.load(f)
    print("Total dataset: ", len(rep_dataset))
    # Log the iteration, about the acc and the selected few shots
    few_shot_logs = {}
    delta_acc = 0
    max_iter = 20
    cur_iter = 0
    # Randomly select 4 samples from the dataset
    rand_idx_list = random.sample(range(len(rep_dataset)), 4)
    few_shot_samples = [(idx, rep_dataset[idx]) for idx in rand_idx_list]
    while cur_iter < max_iter:
        print("Iteration: ", cur_iter)
        # 1. Iterpret the dataset
        orginial_features = interpreter_model(rep_dataset, few_shot_samples)
        # check the error samples
        err_samples = []
        for idx, sample in enumerate(orginial_features):
            if sample["result"] != sample["label"]:
                err_samples.append((idx, sample))
        precision, recall = calcualte_metrics(orginial_features)
        few_shot_logs[cur_iter] = {
            "few_shot_samples": [sample[0] for sample in few_shot_samples],
            "precision": precision,
            "recall": recall,
        }
        if len(err_samples) == 0:
            print("No error samples, break the loop.")
            break
        # 2. Criticize the dataset
        revised_samples = criticize_model(err_samples)
        # 3. Select the new shot
        new_sample = diff_and_get_new_sample(revised_samples, err_samples)
        # 4. Replace the new sample with the old sample
        few_shot_samples = select_new_few_shots(err_samples, few_shot_samples, new_sample)
        cur_iter += 1
    
    # dump the few shot logs to file
    with open(os.path.join(OUTPUT_DIR, "few_shot_logs.json"), "w") as f:
        json.dump(few_shot_logs, f)
    