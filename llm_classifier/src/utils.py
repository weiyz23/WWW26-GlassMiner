import random
import ssl
import time
from bs4 import BeautifulSoup
import regex as re
import warnings
import html2text
import requests
import zstandard as zstd
import io

from configs import *

requests.packages.urllib3.disable_warnings() # type: ignore
context = ssl.create_default_context()
context.set_ciphers('HIGH:!DH:!aNULL')


API_HEADER = {
    "Authorization": "Bearer <Your API Key>",
    "Content-Type": "application/json"
}

API_URL = "https://api.siliconflow.cn/v1/chat/completions"
BASE_PROMPT = {
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

class CustomHTMLParser(html2text.HTML2Text):
    """
    Custom HTML parser to handle specific cases.
    """
    def __init__(self):
        super().__init__()

        self.ignore_emphasis = True
        self.ignore_links = False
        self.single_line_break = True
        self.body_width = 0

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

def contain_filter_words(contents: str) -> bool:
    """
    Check if the webpage contains any filter words.
    """
    text_lower = contents.lower()
    for word in SIMPLE_FILETER_WORDS:
        if word.lower() in text_lower:
            return True
    return False

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

def collect_text_in_order(html_str):
    if len(html_str) > 500000:
        return None
    # Part one: Extract text in input / meta, change them into direct text
    soup = parse_webpages(html_str)
    if soup is None:
        return None
    # Find all input with "placeholder" or "value" attributes
    input_tags = soup.find_all("input")
    for input_tag in input_tags:
        text = ""
        # Check if the input tag has a placeholder or value attribute
        if "placeholder" in input_tag.attrs:
            text = input_tag["placeholder"]    
        elif "value" in input_tag.attrs:
            text = input_tag["value"]
        # replace input tags by [Input]:{text}
        if text:
            input_tag.replace_with(f"[Input]:{text}")
    # fing the meta tags with "content" and "name" attributes
    meta_tags = soup.find_all("meta", attrs={"name": True, "content": True})
    # repalce meta tags with [Meta]{name}:{content}
    meta_text = []
    for meta_tag in meta_tags:
        # Check if the meta tag has a name attribute
        name = meta_tag["name"]
        if "hyperglass" in name or "title" in name or "description" in name:            
            content = meta_tag["content"]
            meta_text.append(f"[Meta]:{name}:{content}")
    meta_text = " ".join(meta_text)
    # Part two: Extract text in the body
    html_str = str(soup)
    handler = CustomHTMLParser()
    text = handler.handle(html_str)
    # Split the text by \n
    lines = text.split("\n")
    # Remove empty lines and lines with too long words
    filtered_lines = []
    for line in lines:
        # Remove leading and trailing spaces
        line = line.strip()
        # Check if the line is empty or contains too long words
        if len(line) > 0 and len(line) < TEXT_LEN_MAX_THRESHOLD:
            filtered_lines.append(line)
    # Join the filtered lines into a single string
    text = " ".join(filtered_lines)
    text = meta_text + " " + text
    text = re.sub(r"\s+", " ", text)
    link_matches = re.findall(PTN_LINK, text)
    # two groups, the first group is the text, the second group is the link
    for match in link_matches:
        # check if the first group contains filter words, if not, remove the link
        if not contain_filter_words(match[0]):
            # replace the original match with the first group
            text = text.replace(f"[{match[0]}]({match[1]})", match[0])
    return text.strip()

def extract_context_around_keywords(content: str) -> str | None:
    """
    If the web page content length is too long, we need to extract the context between the keywords.
    """
    match_strs = re.finditer(PTN_KEYWORD, content)
    list_match_pos = []
    for match_str in match_strs:
        list_match_pos.append(match_str.start())
    # If no match, return None
    if len(list_match_pos) == 0:
        return None
    # 3000 characters, usually less than 1000 words.
    if len(content) > 2000:
        # If there are more than 10 keywords, we only keep the last 10 keywords
        if len(list_match_pos) > 10:
            list_match_pos = list_match_pos[-10:]
        # Find every keyword index in the whole text
        slice_len = 200
        list_slices = [(max(0, i - slice_len), min(i + slice_len, len(content))) for i in list_match_pos]
        # For each slice, expand to both sides to find one blank space
        for i in range(len(list_slices)):
            start, end = list_slices[i]
            # Expand to the left
            while start > 0 and content[start - 1] != " ":
                start -= 1
            # Expand to the right
            while end < len(content) and content[end] != " ":
                end += 1
            list_slices[i] = (start, end)
        merged_slices = []
        for start, end in list_slices:
            # If the merged_slices is empty, add the first slice
            if merged_slices and merged_slices[-1][1] >= start:
                merged_slices[-1][1] = max(merged_slices[-1][1], end)
            else:
                merged_slices.append([start, end])

        # Extract the content around the keywords
        context_list = []
        for start, end in merged_slices:
            context_list.append(content[start:end])
        content = " ".join(context_list)
    return content

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
            # find the first number in the result
            res = re.search(r"\d+", content)
        except Exception as e:
            res = None
            retry_count += 1
            time.sleep(retry_count)
            if retry_count > 5:
                break
    if res:
        pred = int(res.group())
    else:
        pred = None
    return pred

def prompted_binary_classification(html_text):
    """
    通过提示模型进行两次二分类
    """
    new_base_prompt_1 = {
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
    new_base_prompt_1["messages"].append({
        "content": "Categorize webpage content: 1. Unrelated; 2. Looking Glass-related (links to LG or similar network tools). Example A: ```Innovative Technological Solutions ### Network Tools * Network Status * Looking Glass * DNS Lookup * IP Lookup * WhoIs``` contains word Looking Glass, but no related links -> classify as 1. Example B: ```Welcome to the webserver of RLP-NET. The only public service offered so far on this server is a [traceroute](/cgi-bin/tracer.cgi) server.``` contains link to traceroute server -> classify as 2. Output 1 or 2 only.",
        "role": "system"
    })
    
    new_base_prompt_2 = {
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
    
    new_base_prompt_2["messages"].append({
        "content": """Categorize webpage content: 1. Looking Glass related (link to LG or similar webpages) but no direct service; 2. Direct LG service. If it allows commands (traceroute, ping, etc.), selection of parameters (addresses, etc.), it should be class 2. If it is a whois or network information page or only provide links to LG or similar services, it should be class 1. The input starts with its url, and hyperlinks are presented as `[text](link)`.
        Example A: ```[Ping Testi](https://atlantisnet.com.tr/internet-ping-testi/) *  ##### Yardım * [ Atlantis Looking Glass ](https://lg.atlantisnet.com.tr/)``` All commands are links to other pages but no direct service -> class 1; 
        Example B: ```The only public service offered so far on this server is a [traceroute](/cgi-bin/tracer.cgi) server.``` contains command keywords but only links -> class 1; 
        Example C: ```[Meta]:og:title:INS BGP looking glass [Meta]:description:International Network Services Network Looking Glass [Meta]:hyperglass * ## FRA Marseille, MRS1 FM * ## ZAF Durban, DMO ZD``` contains `hyperglass` from template LG webpage. -> class 2.
        Example D: ```www.ip2location.com:...``` or ``` www.peeringdb.com: ...``` are likely information pages -> class 1.
        Output 1 or 2 only.""",
        "role": "system"
    })
    
    new_base_prompt_1["messages"].append({
        "content": html_text,
        "role": "user"
    })
    result = request_llm_and_get_response(new_base_prompt_1)
    if result == 2:
        new_base_prompt_2["messages"].append({
            "content": html_text,
            "role": "user"
        })
        result = request_llm_and_get_response(new_base_prompt_2)
        if result is not None:
            result += 1
    return result

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