import os
import numpy as np
# ====================== Directory & path Configs ====================== #
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "output")
# Shared data directories (centralized large data files)
SHARED_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "shared_data")
GEOLOCATION_DIR = os.path.join(SHARED_DATA_DIR, "geolocation")
NETWORK_DIR = os.path.join(SHARED_DATA_DIR, "network")
LOGS_DIR = os.path.join(OUTPUT_DIR, "logs")
UNIQ_FILE = "unique_active_vp_info.json"
LOG_FILE = "lg_prober.log"

SUBNET_LEN_LIST = [20, 24, 26, 28, 30]
DEFAULT_SUBNET_LEN = 30
EST_SPEED = 100  # ~100 km/ms , as RTT includes both ways, this is the upper bound
EMP_SPEED = 66.7  # ~66.7 km/ms , empirical speed using 4/9 of light speed 

# crawler configs
MAX_RETRY = 2
TIMEOUT = 120
MAX_WORKERS = 48
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

METHOD_KEYWORDS = {
    "traceroute": [
        "traceroute4",
        "traceroute",
        "trace",
        "tracert",
        "trace-route",
    ],
    "traceroute6": [
        "traceroute6",
        "traceroute",
        "trace",
        "tracert",
        "trace-route",
    ],
}

CUSTOMER_TO_CDN = {
    'bing.com': 'Microsoft',
    'xbox.com': 'Microsoft',
    'yammer.com': 'Microsoft',
    'google.com': 'Google',
    'mozilla.org': 'Google',
    'cloudflare.com': 'Cloudflare',
    'w3.org': 'Cloudflare',
    'claude.ai': 'Cloudflare',
    'akamai.com': 'Akamai',
    'nbcsports.com': 'Akamai',
}