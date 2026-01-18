# Using the LGs with traceroute capability to trace to a set of target IPs

import json
import os
import pickle
import time
import random
import requests
from urllib.parse import urlparse, urljoin
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from configs import *
from utils import *

requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

def get_lg_with_trace_capability(lg_list):
    lg_trace_list = []
    for lg in lg_list:
        if lg.get("traceroute", False):
            lg_trace_list.append(lg)
    return lg_trace_list

def is_traceroute_supported(vp_info):
    """Check if a vantage point supports traceroute."""
    if 'command' not in vp_info or 'options' not in vp_info['command']:
        return False
    
    keywords = METHOD_KEYWORDS.get("traceroute", [])
    for option in vp_info['command']['options']:
        value = option.get('value', '').lower()
        for keyword in keywords:
            if keyword in value:
                return True
    return False

def worker(lg_info, lg_idx, host):
    """Worker function to perform a single traceroute with retry logic."""
    # First attempt
    trace_log = trace_to_one_lg(lg_info, host)
    if trace_log:
        return lg_idx, trace_log
    
    # If first attempt failed, wait and retry once more
    time.sleep(random.uniform(1, 3))  # Random delay between 1-3 seconds
    trace_log = trace_to_one_lg(lg_info, host)
    if trace_log:
        return lg_idx, trace_log
    # Both attempts failed
    return None, None

if __name__ == "__main__":
    # Load unique looking glass page list
    uniq_file_path = os.path.join(SHARED_DATA_DIR, UNIQ_FILE)
    with open(uniq_file_path, 'r') as f:
        lg_list = json.load(f)
    random.seed(time.time())

    # Filter for LGs that support traceroute
    supported_lgs = [(i, lg) for i, lg in enumerate(lg_list) if is_traceroute_supported(lg)]
    random.shuffle(supported_lgs)  # Shuffle to distribute load
    # supported_lgs = supported_lgs[100:110]  # Limit to first 10 for testing
    print(f"Found {len(supported_lgs)} LGs supporting traceroute out of {len(lg_list)} total.")

    all_results = {}
    all_results_obj = {}
    failed_logs = []
    
    # Get all unique hosts from CUSTOMER_TO_CDN
    all_hosts = sorted(list(set(host for hosts in CUSTOMER_TO_CDN.keys() for host in hosts)))
    # all_hosts = all_hosts[2:4]  # Limit for testing

    for host in tqdm(all_hosts, desc="Probing hosts"):
        all_results[host] = {}
        all_results_obj[host] = {}
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Create a future for each supported LG
            future_to_lg = {executor.submit(worker, lg_info, lg_idx, host): (lg_idx, lg_info) for lg_idx, lg_info in supported_lgs}
            
            progress_bar = tqdm(as_completed(future_to_lg), total=len(supported_lgs), desc=f"Tracing {host}", leave=False)
            
            for future in progress_bar:
                try:
                    result = future.result()
                    if result:
                        lg_idx, trace = result
                        dict_trace = trace.to_dict()
                        all_results[host][lg_idx] = dict_trace
                        all_results_obj[host][lg_idx] = trace
                except Exception as e:
                    lg_idx, lg_info = future_to_lg[future]
                    url = lg_info.get("url", "N/A")
                    failed_logs.append((host, url, lg_idx, str(e)))

    # save log of failed attempts
    os.makedirs(LOGS_DIR, exist_ok=True)
    log_path = os.path.join(LOGS_DIR, LOG_FILE)
    with open(log_path, 'w') as log_file:
        for entry in failed_logs:
            log_file.write(f"Host: {entry[0]}, URL: {entry[1]}, LG Index: {entry[2]}, Error: {entry[3]}\n")
    print(f"Failed traceroute attempts logged to {log_path}")

    # Save results
    output_path = os.path.join(OUTPUT_DIR, "traceroute_results.json")
    with open(output_path, 'w') as f:
        json.dump(all_results, f, indent=4)
    
    output_path_obj = os.path.join(OUTPUT_DIR, "traceroute_results.bin")
    with open(output_path_obj, 'wb') as f:
        pickle.dump(all_results_obj, f)
    
    print(f"All traceroute results saved to {output_path}")