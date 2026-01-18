# For those VPs without unknown IP, we can use the Geo-Hint to find their location
# Schedule the unknown VPS to be geolocated, make them ping to hosted machine
import os
import random
import time
import json
import requests
from functools import partial
import concurrent.futures
import paramiko
import time
from scp import SCPClient

from templates import *
from configs import *
from utils import *

requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
requests_get = partial(requests.get, timeout=10, verify=False)
requests_post = partial(requests.post, timeout=10, verify=False)

if __name__ == "__main__":
    known_vp_list = json.load(open(os.path.join(OUTPUT_DIR, "known_vp_list.json"), "r"))
    known_vp_list = known_vp_list
    vp_num = len(known_vp_list)
    print('Total known VPS:', vp_num)
    # generate tasks at random order
    task_params = [(m_idx, lg_idx) for m_idx in range(len(HOSTS)) for lg_idx in range(vp_num)]
    random.seed(time.time())
    random.shuffle(task_params)
    print(f"Total tasks: {len(task_params)}")

    clients, pids = [], []
    for host in HOSTS:
        if "passwd" in host:
            client = CustomSSHClient(host["public_ip"], host["username"], host.get("port", 22), host["passwd"])
        else:
            client = CustomSSHClient(host["public_ip"], host["username"], host.get("port", 22), host.get("passwd", None))
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
                futures.append(executor.submit(ping_to_one_lg, m_idx, lg_idx, known_vp_list[lg_idx]))
            
            # get the result and write to file
            for future in concurrent.futures.as_completed(futures):
                finish_count += 1
                m_idx, lg_idx, start_time = future.result()
                if finish_count % 500 == 0:
                    print(f"Finish {finish_count} tasks, {len(futures) - finish_count} tasks left")
        print(f'Finish all probing')
    finally:
        for client, host, pid in zip(clients, HOSTS, pids):
            print(f"[{host['public_ip']}] Stopping tcpdump...")
            stop_tcpdump(client, pid)
            local_path = os.path.join(OUTPUT_DIR, host['local_path'])
            download_pcap(client, host["pcap_path"], local_path)
            client.close()
            print(f"[{host['public_ip']}] Done.")