import os
# import numpy as np
# ====================== Directory & path Configs ====================== #
# Shared data directories (centralized large data files)
SHARED_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "shared_data")
GEOLOCATION_DIR = os.path.join(SHARED_DATA_DIR, "geolocation")
NETWORK_DIR = os.path.join(SHARED_DATA_DIR, "network")
SAVE_DIR = os.path.join(SHARED_DATA_DIR, "downloaded")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "output")
PROCS_DIR = os.path.join(OUTPUT_DIR, "processed")
LOGS_DIR = os.path.join(OUTPUT_DIR, "logs")

TOTAL_FILE = "total_lg_page_list.json"
AVAI_FILE = "available_lg_page_list.json"
FAIL_FILE = "failed_lg_page_list.json"
UNIQ_FILE = "unique_active_vp_info.json"
SIM_FILE = "similar_matrix_{}.bin"
DUP_FILE = "dict_hash_contents.json"

# ====================== Crawler Configs ====================== #

# crawler configs
MAX_RETRY = 2
TIMEOUT = 20
MAX_WORKERS = 24
# A list of headers to avoid being blocked
USER_AGENT_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.81 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.97 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.96 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.81 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.96 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
]
BASE_HEADER = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive"
}
SIMPLE_FILETER_WORDS = {
    "looking glass",
    "lookingglass",
    "hyperglass",
    "traceroute",
    "ping",
    "route",
    "bgp",
}
SIMPLE_FILETER_URLS = {
    "looking-glass",
    "lg",
    "lookingglass",
    "looking",
    "glass",
}
SIMPLE_STOP_WORDS = {
    "not found",
    "forbidden",
    "error",
    "notfound"
}
FILE_NAME_MAX_LENGTH = 200

# ====================== Clustering Configs ====================== #
PTN_CHAR = r'^[^\p{L}\u4e00-\u9fff\u0400-\u04FF]*$'
PTN_IP = r'\b([0-9]{1,3}\.){3}[0-9]{1,3}\b'

SHINGLE_SIZE = 3  # The size of the shingle, important for the Jaccard similarity
IGNORE_THRESHOLD = 4 # The text with characters less than this threshold will be ignored
TEXT_LEN_MAX_THRESHOLD = 50  # The threshold of the text length, remove the text if it's too long
TEXT_LEN_MIN_THRESHOLD = 10  # The threshold of the text length, remove the text if it's too short
CORPUS_THRESHOLD = 0.4  # The threshold of the Jaccard similarity for clustering
STRUC_THRESHOLD = 0.8  # The threshold of the Jaccard similarity for clustering

# Define the info of the self-controlled hosts
HOSTS = [
    {
        "public_ip": "<Your Public IP #1>",
        "private_ip": "<Your Private IP #1>",
        "username": "<Your SSH Username #1>",
        "port": 22,
        # "passwd": "",  # Optional, if using password authentication
        "pcap_path": "/tmp/0_receive.pcap",
        "local_path": "0_receive.pcap",
    },
    {
        "public_ip": "<Your Public IP #2>",
        "private_ip": "<Your Private IP #2>",
        "username": "<Your SSH Username #2>",
        "port": 22,
        # "passwd": "",  # Optional, if using password authentication
        "pcap_path": "/tmp/1_receive.pcap",
        "local_path": "1_receive.pcap",
    },
    {
        "public_ip": "<Your Public IP #3>",
        "private_ip": "<Your Private IP #3>",
        "username": "<Your SSH Username #3>",
        "port": 22,
        # "passwd": "",  # Optional, if using password authentication
        "pcap_path": "/tmp/2_receive.pcap",
        "local_path": "2_receive.pcap",
    },
]