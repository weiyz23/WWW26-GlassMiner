import socket
import ssl
import threading
from urllib.parse import urlparse
import warnings
import geoip2.database
import maxminddb
import time
import random
from bs4 import BeautifulSoup
import requests
import regex as re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.driver_cache import DriverCacheManager
import shutil
import zstandard as zstd
import io

from configs import *
cache_manager = DriverCacheManager()
cache_dir = cache_manager._root_dir
shutil.rmtree(cache_dir, ignore_errors=True)
driver_path = ChromeDriverManager().install()

requests.packages.urllib3.disable_warnings() # type: ignore
context = ssl.create_default_context()
context.set_ciphers('HIGH:!DH:!aNULL')

DOMAIN2IP_CACHE = {}
ASN_READER_1 = geoip2.database.Reader(GEOLITE_ASN)
ASN_READER_2 = maxminddb.open_database(IPINFO_ASN)

CACHE_LOCK = threading.Lock()

def get_asn_from_ip(ip: str):
    """
    Get the ASN from the IP address.
    """
    try:
        response = ASN_READER_2.get(ip)
    except Exception as e:
        response = None
    if response is not None:
        as_str: str = response['asn'] # type: ignore
        return int(as_str[2:])
    else:
        try:
            response = ASN_READER_1.asn(ip)
            return response.autonomous_system_number
        except Exception as e:
            return None    
    
def get_ip_from_url(url):
    """
    Add extra cache to avoid repeated DNS queries.
    """
    domain = urlparse(url).hostname
    if not domain:
        return None
    if domain in DOMAIN2IP_CACHE.keys():
        return DOMAIN2IP_CACHE[domain]
    try:
        # 获取所有IP地址（IPv4和IPv6）
        ip_list = socket.getaddrinfo(domain, None)
        # 提取IP地址（排除IPv6的百分号后缀）
        ips = {ip[4][0].split('%')[0] for ip in ip_list} # type: ignore
        # lock the cache for async
        with CACHE_LOCK:
            for ip in ips:
                DOMAIN2IP_CACHE[domain] = ip
        return list(set(ips))  # 去重
    except Exception as e:
        return None

def extract_asn_from_url(url):
    """
    Extract the ASN from the URL. 
    """
    # Maybe leading / or .
    pattern = r"[/\.]as(\d+)\."
    asn_match = re.search(pattern, url, flags=re.IGNORECASE)
    asn = 0
    if asn_match:
        asn = int(asn_match.group(1))
    return asn

def get_asn_from_url(url):
    """
    Get the ASN from the URL.
    """
    asn_set = set()
    ip_list = get_ip_from_url(url)
    if ip_list is not None:
        for ip in ip_list:
            asn = get_asn_from_ip(ip)
            if asn is not None:
                asn_set.add(asn)
    return asn_set

def init_browser():
    """
    Initialize Chrome browser in headless mode with necessary options.
    Hide from anti-crawler detection
    """
    options = webdriver.ChromeOptions()
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_argument("--incognito")
    options.add_argument("--no-sandbox")
    options.add_argument("--headless")
    options.add_argument("--disable-blink-features")
    options.add_argument("--disable-blink-features=AutomationControlled")
    service = Service(driver_path)
    return webdriver.Chrome(service=service, options=options)

def fetch_one_page(url, session: requests.Session, retry_count=0) -> dict:
    header = BASE_HEADER
    header["User-Agent"] = random.choice(USER_AGENT_LIST)
    try:
        # First send a HEAD request to check content size, discard if > 10 MB
        head_response = session.head(url, timeout=TIMEOUT, headers=header, verify=False, allow_redirects=True)
        content_size = head_response.headers.get('Content-Length', 0)
        if content_size and int(content_size) > 10 * 1024 * 1024:
            return {
                "original_url": url,
                "error": "Content size too large",
                "success": False
            }
        else:
            response = session.get(url, timeout=TIMEOUT, headers=header, verify=False, allow_redirects=True, stream=True)
            # check if the content encoding is zstandard
            if response.headers.get('Content-Encoding') == 'zstd':
                # decompress the content
                dctx = zstd.ZstdDecompressor()
                with dctx.stream_reader(io.BytesIO(response.raw.read())) as reader:
                    decompressed = reader.read()
                    response_text = decompressed.decode("utf-8")
            else:
                response_text = response.text
            final_url = response.url.rstrip('/')
    except Exception as e:
        if retry_count < MAX_RETRY:
            return fetch_one_page(url, session, retry_count + 1)
        return {
            "original_url": url,
            "error": str(e),
            "success": False
        }
    soup = parse_webpages(response_text)
    filename = url_to_filename(final_url)
    filepath = os.path.join(SAVE_DIR, filename)
    with open(filepath, 'w', encoding='utf-8', errors='ignore') as f:
        f.write(str(soup))
    return {
        "original_url": url,
        "final_url": final_url,
        "success": True
    }

def extract_url_from_bing_search(driver: webdriver.Chrome):
    urls = []
    time.sleep(1)
    js = 'window.scrollTo(0, document.body.scrollHeight);'
    driver.execute_script(js)
    tmp_gap = random.randint(20, 40) / 10
    time.sleep(tmp_gap)
    driver.execute_script(js)
    time.sleep(1)
    source_code = driver.page_source
    
    if('There are no results for' not in source_code.replace('\n','')):
        soup = BeautifulSoup(source_code, "html.parser")
        # eg: https://learn.microsoft.com › en-us › advertising › guides -> https://learn.microsoft.com/en-us/advertising/guides
        cite_tags = soup.find_all('cite')
        if len(cite_tags) == 0:
            return None
        for cite in cite_tags:
            raw_text = cite.get_text()
            url = ''
            # Split the text by space, and then join them with '/'
            # If the last character is '…', remove it
            slices = raw_text.split(' ')
            if slices[-1].endswith('…') or slices[-1].endswith('...'):
                slices = slices[:-1]
            for slice in slices:
                if slice == '›':
                    continue
                url += slice + '/'
            if len(url) == 0:
                continue                  
            url = url.rstrip('/')
            # if the url are not start with http, add it
            if not url.startswith('http'):
                url = 'https://' + url
            urls.append(url)
    return urls

def perform_search_with_retry(browser, query_url):
    browser.get(query_url)
    tmp_urls = extract_url_from_bing_search(browser)
    gap_time = random.randint(20, 80) / 10    
    time.sleep(gap_time)

    # rate limit by bing
    limit_count = 0
    while tmp_urls is None:
        limit_count += 1
        if limit_count > 2:
            return []
        browser.get(query_url)
        tmp_urls = extract_url_from_bing_search(browser)
        gap_time = random.randint(50, 100) / 10
    return tmp_urls

def search_for_one_keyword(browser, keyword, num=500):
    # Search term: key+looking+glass
    candidate_urls = set()
    url = BASE_URL.format(keyword, 1)
    browser.get(url)
    ## 获取当前页面中的结果的 URL，记录总数，直到满 500 条或者没有更多结果
    tmp_urls = perform_search_with_retry(browser, url)
    
    time.sleep(1)
    js = 'window.scrollTo(0, document.body.scrollHeight);'
    browser.execute_script(js)
    time.sleep(2)
    
    # If the first page has no results, return None
    if len(tmp_urls) == 0:
        print(f"Cannot find any urls for {keyword}")
        return candidate_urls
    
    collected_count = len(tmp_urls)
    candidate_urls.update(tmp_urls)
    
    while collected_count < num:
        url = BASE_URL.format(keyword, collected_count+1)
        tmp_urls = perform_search_with_retry(browser, url)
        candidate_urls.update(tmp_urls)
        collected_count += max(len(tmp_urls), 1)
    # cut the urls to num
    if len(candidate_urls) > num:
        candidate_urls = set(list(candidate_urls)[:num])
    return candidate_urls

def url_to_filename(url: str) -> str:
    """
    Convert the URL to a filename by replacing the special characters.
    """
    filename = url.split('://')[1]    
    # remove the tailing slash, and only keep the first 40 characters
    if filename.endswith('/'):
        filename = filename[:-1]
    filename = filename.replace('/', '_')
    if len(filename) > FILE_NAME_MAX_LENGTH:
        filename = filename[:FILE_NAME_MAX_LENGTH]
    return filename

def parse_webpages(webpage: str) -> BeautifulSoup | None:
    """
    Adaptive parsing of the webpage content by html parser or lxml parser.
    """
    try:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            soup = BeautifulSoup(webpage, "html.parser")
            # Check for XML parsed as HTML warnings
            if any("XMLParsedAsHTMLWarning" in str(warning.message) for warning in w):
                soup = BeautifulSoup(webpage, "xml")
    except Exception as e:
        print(f"Error parsing webpage: {e}")
        return None
    return soup
