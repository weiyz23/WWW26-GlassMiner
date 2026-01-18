import os
import regex as re
import numpy as np
# ====================== Directory & path Configs ====================== #
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "output")
# Shared data directories (centralized large data files)
SHARED_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "shared_data")
GEOLOCATION_DIR = os.path.join(SHARED_DATA_DIR, "geolocation")
NETWORK_DIR = os.path.join(SHARED_DATA_DIR, "network")
SAVE_DIR = os.path.join(SHARED_DATA_DIR, "downloaded")
PROCS_DIR = os.path.join(OUTPUT_DIR, "processed")
VERIFIED_DIR = os.path.join(OUTPUT_DIR, "verified")
RELATED_DIR = os.path.join(OUTPUT_DIR, "related")

TOTAL_FILE = "total_lg_page_list.json"
CAND_FILE = "candidate_lg_page_list.json"
UNIQ_FILE = "unique_lg_page_list.json"
RELATED_FILE = "related_page_list.json"

# crawler configs
MAX_RETRY = 2
TIMEOUT = 15
MAX_WORKERS = 24
NUM_THREADS = 8
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
    "trace",
    "mtr"
}
FILE_NAME_MAX_LENGTH = 200
PTN_CHAR = r'^[^\p{L}\u4e00-\u9fff\u0400-\u04FF]*$'
PTN_IP = r'\b([0-9]{1,3}\.){3}[0-9]{1,3}\b'
PTN_KEYWORD = re.compile(r'\b(?:' + '|'.join(SIMPLE_FILETER_WORDS) + r')\b', re.IGNORECASE)
PTN_LINK = r'\[(.+?)\]\((.+?)\)'
URL_FILTER_WORDS = {
    "lookingglass",
    "lg",
    "speedtest",
    "traceroute",
    "trace",
    "ping"
}

MAX_WORKERS = 48
NUM_THREADS = 8
IGNORE_THRESHOLD = 3 # The text with characters less than this threshold will be ignored
TEXT_LEN_MAX_THRESHOLD = 200  # The threshold of the text length, remove the text if it's too long
TEXT_LEN_MIN_THRESHOLD = 10  # The threshold of the text length, remove the text if it's too short