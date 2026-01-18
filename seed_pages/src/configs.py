import os
import numpy as np
# ====================== Directory & path Configs ====================== #
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "output")
# Shared data directories (centralized large data files)
SHARED_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "shared_data")
GEOLOCATION_DIR = os.path.join(SHARED_DATA_DIR, "geolocation")
NETWORK_DIR = os.path.join(SHARED_DATA_DIR, "network")
SAVE_DIR = os.path.join(OUTPUT_DIR, "downloaded")
PROCS_DIR = os.path.join(OUTPUT_DIR, "processed")
VERIFIED_DIR = os.path.join(OUTPUT_DIR, "verified")
UNVERIFIED_DIR = os.path.join(OUTPUT_DIR, "unverified")
UNRELATED_DIR = os.path.join(OUTPUT_DIR, "unrelated")
LOGS_DIR = os.path.join(OUTPUT_DIR, "logs")

AVAI_FILE = "available_lg_page_list.json"
FAIL_FILE = "failed_lg_page_list.json"
UNIQ_FILE = "unique_lg_page_list.json"
SIM_FILE = "similar_matrix_{}.bin"
DUP_FILE = "dict_hash_contents.json"

# ====================== Crawler Configs ====================== #

# crawler configs
MAX_RETRY = 2
TIMEOUT = 15
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
GENERAL_WEIGHT_THRESHOLD = 1e-3  # The threshold of the weight for the useful words
CLUSTER_WEIGHT_THRESHOLD = 1e-2

SHINGLE_LEN_LIST = [1, 2, 3, 4, 5, 6, 7]
# From 0.1 to 0.6 with step 0.01
CLUSTER_THR_LIST = np.linspace(0.2, 0.95, 76)