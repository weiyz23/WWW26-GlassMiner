# For those VPs without unknown IP, we can use the Geo-Hint to find their location
# Schedule the unknown VPS to be geolocated, make them ping to hosted machine
import os
import random
import time
import json
import requests
from functools import partial
import concurrent.futures
import time

from templates import *
from configs import *
from utils import *

requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
requests_get = partial(requests.get, timeout=10, verify=False)
requests_post = partial(requests.post, timeout=10, verify=False)

if __name__ == "__main__":
    unknown_vp_list = json.load(open(os.path.join(OUTPUT_DIR, "unknown_vp_list.json"), "r"))
    # unknown_vp_list = unknown_vp_list[2500:2550]  # for test
    vp_num = len(unknown_vp_list)
    print('Total unknown VPS:', vp_num)
    # generate tasks at random order
    task_params = [(m_idx, lg_idx) for m_idx in range(len(HOSTS)) for lg_idx in range(vp_num)]
    timestamp_list = [[0 for _ in range(2 * vp_num)] for _ in range(len(HOSTS))]
    random.seed(time.time())
    random.shuffle(task_params)
    print(f"Total tasks: {len(task_params)}")

    clients, pids = [], []
    for host in HOSTS:
        if "passwd" in host:
            client = CustomSSHClient(host["public_ip"], host["username"], host.get("port", 22), host["passwd"])
        else:
            client = CustomSSHClient(host["public_ip"], host["username"], host.get("port", 22))
        pid = start_tcpdump(client, host["pcap_path"], host['public_ip'], host['private_ip'])
        print(f"[{host['public_ip']}] tcpdump started with PID {pid}")
        clients.append(client)
        pids.append(pid)
    os.makedirs(OUTPUT_DIR, exist_ok=True)    
    try:
        TASK_NUM = 12
        futures = []
        finish_count = 0
        with concurrent.futures.ProcessPoolExecutor(max_workers=TASK_NUM) as executor:
            for m_idx, lg_idx in task_params:
                futures.append(executor.submit(ping_to_one_lg, m_idx, lg_idx, unknown_vp_list[lg_idx]))
            
            # get the result and write to file
            for future in concurrent.futures.as_completed(futures):
                finish_count += 1
                m_idx, lg_idx, start_time = future.result()
                end_time = time.time()
                ingress_tolerance = 1
                egress_tolerance = max(0, 2 - (end_time - start_time))
                timestamp_list[m_idx][2 * lg_idx] = str(start_time - ingress_tolerance)
                timestamp_list[m_idx][2 * lg_idx + 1] = str(end_time + egress_tolerance)
                if finish_count % 500 == 0:
                    print(f"Finish {finish_count} tasks, {len(futures) - finish_count} tasks left")
                
        for m_idx in range(len(HOSTS)):
            time_list = timestamp_list[m_idx]
            with open(os.path.join(OUTPUT_DIR, f'{m_idx}_send.txt'), 'w') as time_file:
                time_file.writelines('\n'.join(time_list))
        print(f'Finish all probing')
    finally:
        for client, host, pid in zip(clients, HOSTS, pids):
            print(f"[{host['public_ip']}] Stopping tcpdump...")
            stop_tcpdump(client, pid)
            local_path = os.path.join(OUTPUT_DIR, host['local_path'])
            download_pcap(client, host["pcap_path"], local_path)
            client.close()
            print(f"[{host['public_ip']}] Done.")