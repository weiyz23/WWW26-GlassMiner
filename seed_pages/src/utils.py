import time
from bs4 import BeautifulSoup, NavigableString
import warnings
import random
import requests
import ssl
import regex as re
from math import log2
from difflib import SequenceMatcher
from niteru.html_parser import parse_html
import zstandard as zstd
import io

from configs import *

requests.packages.urllib3.disable_warnings()
context = ssl.create_default_context()
context.set_ciphers('HIGH:!DH:!aNULL')

TOKINIZER = None

def request_with_random_header(url: str) -> BeautifulSoup:
    """
    Send a request to the target URL with a random User-Agent
    """
    header = BASE_HEADER
    header["User-Agent"] = random.choice(USER_AGENT_LIST)
    response = requests.get(url, headers=header)
    soup = parse_webpages(response.text)
    return soup

def fetch_one_page(url, session: requests.Session, retry_count=0) -> dict:
    try:
        header = BASE_HEADER
        header["User-Agent"] = random.choice(USER_AGENT_LIST)
        # ignore the https insecure warning, and allow the redirect
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

    except Exception as e:
        if retry_count < MAX_RETRY:
            return fetch_one_page(url, session, retry_count + 1)
        return {
            "original_url": url,
            "error": str(e),
            "retries": retry_count,
            "success": False
        }
    # remove tailing slash
    final_url = response.url.rstrip('/')
    return {
        "original_url": url,
        "final_url": final_url,
        "content": response_text,
        "success": True
    }

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

def count_filter_words(contents: str) -> int:
    """
    Check if the webpage is a Looking Glass page by checking the title and body.
    Return the count of appeared filter words.
    """
    # check the content to verify if it's a looking glass page
    appeared_set = set()
    text_lower = contents.lower()
    for word in SIMPLE_FILETER_WORDS:
        if word.lower() in text_lower:
            appeared_set.add(word)
    return len(appeared_set)

def parse_webpages(webpage) -> BeautifulSoup:
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

def is_symbols(token):
    if re.match(PTN_CHAR, token):
        return True
    return False

def tokinize_text(text: str):
    """
    Tokenize the text into words.
    """
    global TOKINIZER
    if TOKINIZER == None:
        from transformers import BertTokenizer
        TOKINIZER = BertTokenizer.from_pretrained("bert-base-multilingual-uncased")
    text = text.lower()
    tokens = TOKINIZER.tokenize(text)
    words = TOKINIZER.convert_tokens_to_string(tokens).split()
    words = filter_out_useless_text(words)
    
    return words

def filter_out_useless_text(list_of_text):
    """
    Filter out the text whose length is longer than the threshold.
    Remove the text with less than 
    """
    new_list = []
    for text in list_of_text:
        if len(text) > TEXT_LEN_MAX_THRESHOLD or len(text) < IGNORE_THRESHOLD:
            continue
        if is_symbols(text):
            continue
        new_list.append(text)
    return new_list

def remove_script_and_style(soup: BeautifulSoup):
    """
    Using BeautifulSoup to remove all the script style tages
    """
    for script in soup.find_all('script'):
        script.decompose()
    for style in soup.find_all('style'):
        style.decompose()
    return soup

def collect_text_in_order(soup: BeautifulSoup):
    """
    Remove all the content within tags from the soup, keep the meta content and text only.
    DO NOT include the text from its children!
    Return the list of texts for given soup.
    """
    list_of_text = []    
    for child in soup.children:
        if isinstance(child, NavigableString):
            # 处理文本节点
            text = child.strip()
            if text:
                list_of_text.append(text)
        elif child.name:
            if child.name == "meta":
                if child.get("content") and child.get("name"):
                    list_of_text.append(child.get("content"))
            # if tag is an input tag, we can extract the "value" attr
            if child.name == "input" and child.get("value"):
                list_of_text.append(child.get("value"))
            # 递归处理子元素
            list_of_text.extend(collect_text_in_order(child))
    return filter_out_useless_text(list_of_text)

# For structure similarity computation.
class StructuralComparator(SequenceMatcher):
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, '_initialized'):
            super().__init__()
            self._initialized = True
    
    def ratio(self) -> float:
        matches = sum(triple[-1] for triple in self.get_matching_blocks())
        modified_len = min(len(self.a), len(self.b)) + log2(max(len(self.a), len(self.b)))
        if modified_len == 0:
            return 0
        return 1.0 * matches / modified_len

def sequence_similarity(html_1: str, html_2: str):
    comparator = StructuralComparator()
    comparator.set_seq1(html_1.tags)
    comparator.set_seq2(html_2.tags)
    return comparator.ratio()

def jaccard_similarity(shingles1, shingles2):
    intersection = shingles1.intersection(shingles2)
    if len(intersection) == 0:
        return 0
    # Must not be empty strings
    len_min = min(len(shingles1), len(shingles2))
    log_len_max = np.log(max(len(shingles1), len(shingles2)))
    # using a symmetric factor to make the similarity symmetric
    return len(intersection) / (log_len_max + len_min)