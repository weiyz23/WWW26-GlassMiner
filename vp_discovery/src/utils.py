from bs4 import BeautifulSoup
import warnings
import json
import random
import requests
import ssl
import regex as re
from math import log2
import html2text
from difflib import SequenceMatcher
import zstandard as zstd
import io
import pickle as pkl
import geoip2.database
from string import digits
import reverse_geocoder
import ipaddress
import paramiko
import time
from scp import SCPClient
from typing import Tuple
from urllib.parse import urlencode, urljoin, parse_qsl
import shlex
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from configs import *

requests.packages.urllib3.disable_warnings()
context = ssl.create_default_context()
context.set_ciphers('HIGH:!DH:!aNULL')

class CustomHTMLParser(html2text.HTML2Text):
    """
    Custom HTML parser to handle specific cases.
    """
    def __init__(self):
        super().__init__()

        self.ignore_emphasis = True
        self.ignore_links = True
        self.single_line_break = True
        self.body_width = 0

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


class CustomSSHClient(paramiko.SSHClient):
    def __init__(self, hostname: str, username: str, port: int, password: str = None):
        super().__init__()
        self.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.connect(hostname, username=username, port=port, look_for_keys=True)
        self.password = password
        self.port = port
        self.hostname = hostname
        self.username = username
        self.need_password = password is not None
        
        # Enable keepalive to prevent connection timeout
        transport = self.get_transport()
        transport.set_keepalive(10)  # Send keepalive every 30 seconds
        
        # execute a dummy sudo command to cache the sudo permission
        stdin, stdout, stderr = self.exec_command("sudo -n true")
        exit_status = stdout.channel.recv_exit_status()
        self.need_password = exit_status != 0

        if self.need_password and self.password:
            # Validate and cache sudo timestamp with password
            stdin, stdout, stderr = self.exec_command('sudo -S -v')
            stdin.write(self.password + '\n')
            stdin.flush()
            res = stdout.channel.recv_exit_status()
            if res != 0:
                raise Exception("Sudo permission denied or incorrect password")

def start_tcpdump(client: CustomSSHClient, pcap_path: str, public_ip: str = None, private_ip: str = None) -> str:
    # first, need to find the device with the public IP or private IP
    device = None
    cmd = f"ip addr show"
    stdin, stdout, _ = client.exec_command(cmd, get_pty=True)
    exit_status = stdout.channel.recv_exit_status()
    output = stdout.read().decode()
    pattern = re.compile(
        r'^\d+: (\S+):[^\n]*\n(?:[ \t].*\n)*?[ \t]+inet (\d+\.\d+\.\d+\.\d+)/\d+',
        flags=re.MULTILINE
    )
    matches = pattern.findall(output)
    for dev, ip in matches:
        if ip == public_ip or ip == private_ip:
            device = dev
    print(f"[{client.hostname}] Using device: {device} for tcpdump")
    
    # Build command using nohup to ensure persistence, with better error handling
    pcap_q = shlex.quote(pcap_path)
    # Remove the old file first to ensure a clean start
    if client.need_password:
        cmd1 = f"echo {client.password} | sudo -S rm -f {pcap_q}"
        cmd2 = f"echo {client.password} | nohup sudo -S tcpdump -U icmp -i {device} -w {pcap_q} > /dev/null 2>&1 & echo $!"
    else:
        cmd1 = f"sudo rm -f {pcap_q}"
        cmd2 = f"nohup sudo -S tcpdump -U icmp -i {device} -w {pcap_q} > /dev/null 2>&1 & echo $!"
    _, _, _ = client.exec_command(cmd1)
    print(f"[{client.hostname}] Removed old pcap file if existed.")
    stdin, stdout, stderr = client.exec_command(cmd2)
    pid = stdout.read().decode().strip()
    return pid

def stop_tcpdump(client: CustomSSHClient, pid: str):
    try:
        # Check connection and reconnect if necessary
        try:
            client.exec_command("echo test")
        except:
            print(f"Connection to {client.hostname} lost, attempting to reconnect...")
            client.close()
            client.connect(client.hostname, username=client.username, port=client.port, look_for_keys=True)
        
        # First try to kill the process group if setsid was used
        if client.password:
            stdin, stdout, _ = client.exec_command(f"sudo -S kill -TERM -{pid}")
            stdin.write(client.password + '\n')
            stdin.flush()
            # Also try to kill the specific process
            stdin, stdout, _ = client.exec_command(f"sudo -S kill -TERM {pid}")
            stdin.write(client.password + '\n')
            stdin.flush()
        else:
            client.exec_command(f"sudo -n kill -TERM -{pid}")
            client.exec_command(f"sudo -n kill -TERM {pid}")
    except Exception as e:
        print(f"Failed to stop tcpdump on {client.hostname}: {e}")
        raise


def download_pcap(client: CustomSSHClient, remote_path: str, local_path: str):
    # Give tcpdump a moment to flush and ensure the file exists
    time.sleep(1)
    # Check existence on remote first to provide clearer error
    stdin, stdout, stderr = client.exec_command(f"test -f {remote_path} && echo OK || echo MISSING")
    status = stdout.read().decode().strip()
    if status != "OK":
        # Try a short grace period before giving up
        time.sleep(1)
        stdin, stdout, stderr = client.exec_command(f"test -f {remote_path} && echo OK || echo MISSING")
        status = stdout.read().decode().strip()
    if status != "OK":
        raise FileNotFoundError(f"Remote pcap not found: {remote_path}")
    with SCPClient(client.get_transport()) as scp:
        scp.get(remote_path, local_path)

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
    return text.strip()

def sequence_similarity(html_1: str, html_2: str):
    comparator = StructuralComparator()
    comparator.set_seq1(html_1.tags)
    comparator.set_seq2(html_2.tags)
    return comparator.ratio()

def process_params(vp_info, target_ip='8.8.8.8') -> Tuple[str, str]:
    url = vp_info["url"]
    action = vp_info["action"]
    base_url = urljoin(url, action)
    dict_params = {}
    params = dict(vp_info["params"])

    for name, value in params.items():
        dict_params[name] = value
    
    command_value = 'ping' # Default fallback
    if 'options' in vp_info["command"]:
        for opt in vp_info["command"]['options']:
            val = opt.get('value', '').lower()
            placeholder = opt.get('placeholder', '').lower()
            if 'ping' in val or 'ping' in placeholder:
                command_value = opt.get('value')
                break
    dict_params[vp_info["command"]['name']] = command_value
    dict_params[vp_info["input"]['name']] = target_ip
    query = urlencode(dict_params)
    return base_url, query

def ping_to_one_lg(m_idx, lg_idx, vp_info):
    """
    According to the machine index and lg index, ping to the lg and get the result.
    Handles CSRF token if required.
    """
    ping_url, query = process_params(vp_info, HOSTS[m_idx]['public_ip'])
    header = BASE_HEADER.copy()
    header["User-Agent"] = random.choice(USER_AGENT_LIST)
    start_time = time.time()
    
    try:
        session = requests.Session()
        method = vp_info["method"]

        if method == "post":
            if 'csrfToken' in vp_info.get("params", {}):
                # 1. GET the page to obtain session cookie and CSRF token
                get_resp = session.get(vp_info["url"], timeout=TIMEOUT, headers=header, verify=False, allow_redirects=True)
                get_resp.raise_for_status()
                # 2. Extract CSRF token from the response
                m = re.search(r'name="csrfToken" value="([a-f0-9]+)"', get_resp.text)
                if m:
                    csrf_token = m.group(1)
                    params_dict = dict(parse_qsl(query))
                    params_dict['csrfToken'] = csrf_token
                    query = urlencode(params_dict)
            else:
                # Looking House type, need redirect to the action URL first
                get_resp = session.get(vp_info['url'], timeout=TIMEOUT, headers=header, verify=False, allow_redirects=True)
                redirected_url = get_resp.url
                ping_url = ping_url.replace(vp_info['url'], redirected_url)
                print(f"Redirected URL: {redirected_url}")
            header['Content-Type'] = vp_info['content-type']
            if vp_info['content-type'] == 'application/json':
                json_data = dict(parse_qsl(query))
                query = json.dumps(json_data)
            header['Origin'] = vp_info["url"].rstrip('/')
            header['Referer'] = vp_info["url"] if vp_info["url"].endswith('/') else vp_info["url"] + '/'
            response = session.post(ping_url, data=query, timeout=TIMEOUT, headers=header, verify=False, allow_redirects=True, stream=True)
            response.raise_for_status()
        
        elif method == "get":
            ping_url = ping_url + '?' + query
            response = session.get(ping_url, timeout=TIMEOUT, headers=header, verify=False, allow_redirects=True, stream=True)
            response.raise_for_status()

    except Exception as e:
        pass
    finally:
        return m_idx, lg_idx, start_time

# =================== Geolocation Utility =================== #
GEOLITE_READER = geoip2.database.Reader(os.path.join(GEOLOCATION_DIR, "GeoLite.mmdb"))

def split_word(name: str):
    name_split = set()
    # remove digits
    table = str.maketrans('', '', digits)
    name = name.translate(table).lower()

    if name.count(',') > 0 and '(' not in name:
        # For '-', there are two choices, one is to remove it, the other is to replace it with ','
        new_name = name.replace('-', '')
        name_split.update([x.replace(' ', '') for x in new_name.split(',')])
        new_name = name.replace('-', ',')
        name_split.update([x.replace(' ', '') for x in new_name.split(',')])
    # check format 'xx (AS number)'
    elif '(as' in name:
        pass
    else:
        name = name.replace('(', ' ')
        name = name.replace(')', ' ')
        name = name.replace(',', ' ')
        name = name.replace('，', ' ')
        name = name.replace('/', ' ')
        name_split.update(name.replace('-',' ').split())
    return name_split

def check_raw_word(raw_word):
    raw_word = raw_word.lower()

    # check if the word is a city name with space
    for name in dict_hasspace_city:
        if name in raw_word:
            return dict_hasspace_city[name]

    set_oneword = split_word(raw_word)
    candidates = list()
    for word in set_oneword:
        # check if the word is a city name
        if word in dict_iata_code:
            if dict_iata_code[word][1] in set_oneword:
                candidates.append(dict_iata_code[word])
            if 'ixp' in set_oneword:
                candidates.append(dict_iata_code[word])

        # check if the word is a city name
        if word in dict_city_by_name:
            candidates.append(dict_city_by_name[word])
   
    # If there are two candidates, check if they are the same city
    if len(candidates) == 2:
        _, _, admin_0, city_0, _  = candidates[0]
        _, _, admin_1, city_1, _  = candidates[1]
        if city_0 == admin_1:
            return candidates[1]
        if city_1 == admin_0:
            return candidates[0]    
    # if there are more than two candidates, check if they are the same city
    if len(candidates) > 0:
        coor_candidates = [x[0] for x in candidates]
        for cdx_1 in range(0, len(coor_candidates)):
            for cdx_2 in range(cdx_1 + 1, len(coor_candidates)):
                if abs(coor_candidates[cdx_1][0] - coor_candidates[cdx_2][0]) > 0.5 or \
                    abs(coor_candidates[cdx_1][1] - coor_candidates[cdx_2][1]) > 0.5:
                    return None
        return candidates[0]
    elif len(candidates) == 0:
        return None

def normalize_geolocation(coord):
    """
    Using reverse_geocoder to normalize the geolocation.
    """
    try:
        result = reverse_geocoder.search(coord, mode=1)
    except Exception as e:
        print(f"Error normalizing geolocation {coord}: {e}")
        return None
    if result:
        # get the country code and city
        country_code = result[0]['cc']
        city = result[0]['name']
        latitude = result[0]['lat']
        longitude = result[0]['lon']
        return {
            "country_code": country_code,
            "city": city,
            "latitude": latitude,
            "longitude": longitude
        }
    

def geolocate_ip(ip_addr):
    """
    Geolocate the IP address using an external API.
    Need country code, city only, 
    """
    try:
        response = GEOLITE_READER.city(ip_addr)
    except:
        return None
    raw_lat = response.location.latitude
    raw_lon = response.location.longitude
    raw_coord = (raw_lat, raw_lon)
    location = None
    if raw_coord:
        location = normalize_geolocation(raw_coord)
    return location

dict_city_by_name = pkl.load(open(os.path.join(GEOLOCATION_DIR, "dict_city_by_name.bin"), "rb"))
dict_hasspace_city = pkl.load(open(os.path.join(GEOLOCATION_DIR, "dict_hasspace_city.bin"), "rb"))
dict_iata_code = pkl.load(open(os.path.join(GEOLOCATION_DIR, "dict_iata_code.bin"), "rb"))
dict_city_alter_name = pkl.load(open(os.path.join(GEOLOCATION_DIR, "dict_city_alter_name.bin"), "rb"))
def geolocate_hint(hint):
    """
    Geolocate the hint using an external API.
    """
    geo_info = check_raw_word(hint)
    location = None
    if geo_info:
        coord = geo_info[0]
        location = normalize_geolocation(coord)
    return location

def geolocate_one_vp(vp_info):
    """
    Geolocate one VP by IP or Geo-Hint.
    """
    location = {}
    ip_addr = vp_info["ip_addr"]
    is_hint = False
    if ip_addr:
        # Geolocate by IP
        location = geolocate_ip(ip_addr)
    else:
        # Geolocate by Geo-Hint
        hint = vp_info["hint"]
        if hint:
            location = geolocate_hint(hint)
            if location:
                is_hint = True
    return location, is_hint

BOGON_NETWORKS = [
    "0.0.0.0/8",
    "10.0.0.0/8",
    "100.64.0.0/10",
    "127.0.0.0/8",
    "169.254.0.0/16",
    "172.16.0.0/12",
    "192.0.0.0/24",
    "192.0.2.0/24",
    "192.168.0.0/16",
    "198.18.0.0/15",
    "198.51.100.0/24",
    "203.0.113.0/24",
    "224.0.0.0/4",
    "240.0.0.0/4",
    "255.255.255.255/32",
]
BOGON_NETS = [ipaddress.ip_network(net) for net in BOGON_NETWORKS]
def is_bogon(ip_str):
    try:
        ip_obj = ipaddress.ip_address(ip_str)
    except ValueError:
        return False
    return any(ip_obj in net for net in BOGON_NETS)

# =================== Hyperglass Parser Utility =================== #

# Initialize Chrome driver path globally
try:
    driver_path = ChromeDriverManager().install()
except Exception as e:
    print(f"Error installing Chrome driver: {e}")
    driver_path = "chromedriver"  # Fallback

def location_text_to_value(text):
    """
    Convert location placeholder text to value format
    Rules: lowercase, spaces to underscores, keep hyphens, remove other non-latin characters
    Example: "SÃO PAULO" -> "so_paulo", "London-UK" -> "london-uk"
    """
    if not text:
        return ""
    
    # Convert to lowercase
    value = text.lower()
    
    # Replace spaces with underscores
    value = value.replace(' ', '_')
    
    # Remove non-latin characters (keep only a-z, 0-9, underscore, hyphen)
    value = re.sub(r'[^a-z0-9_-]', '', value)
    
    # Remove multiple consecutive underscores
    value = re.sub(r'_+', '_', value)
    # Remove leading/trailing underscores
    value = value.strip('_')
    
    return value

def extract_query_type_value(browser):
    """
    Extract query type value from hidden input after selection
    Looks for hidden inputs with names containing 'query' and 'type'
    """
    try:
        # Common hidden input selectors for query type
        selectors = [
            'input[name*="query"][name*="type"][type="hidden"]',
            'input[name="query_type"][type="hidden"]',
            'input[name="QueryType"][type="hidden"]',
            'input[name="queryType"][type="hidden"]'
        ]
        
        for selector in selectors:
            elements = browser.find_elements(By.CSS_SELECTOR, selector)
            for element in elements:
                value = element.get_attribute('value')
                if value:
                    return value
        return None
    except:
        return None

class HyperglassParser:
    """
    Parser for Hyperglass pages with dynamic CSS selectors
    Handles both React Select dropdowns and Chakra UI list components
    """
    
    def __init__(self):
        """
        Initialize the parser with a Chrome browser
        
        Args:
            headless (bool): Whether to run browser in headless mode
        """
        self.browser = None
        
    def init_browser(self):
        """Initialize Chrome browser with optimized settings for dynamic content"""
        if self.browser:
            return
            
        options = webdriver.ChromeOptions()        
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_argument("--incognito")
        options.add_argument("--no-sandbox")
        options.add_argument("--headless")
        options.add_argument("--disable-blink-features")
        options.add_argument("--disable-blink-features=AutomationControlled")
        
        service = Service(driver_path)
        self.browser = webdriver.Chrome(service=service, options=options)
        # Hide webdriver property
        self.browser.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
    def close_browser(self):
        """Close the browser"""
        if self.browser:
            self.browser.quit()
            self.browser = None
            
    def parse_nextjs_data(self, url):
        """
        Parse Hyperglass Next.js data directly from script tag
        """
        try:
            self.init_browser()
            self.browser.get(url)
            
            try:
                script = self.browser.find_element(By.ID, "__NEXT_DATA__")
            except NoSuchElementException:
                return None

            data = json.loads(script.get_attribute('innerHTML'))
            config = data.get('props', {}).get('appProps', {}).get('config', {})
            
            # Extract Query Types
            query_types = []
            queries_list = config.get('queries', {}).get('list', [])
            
            if not queries_list:
                return None
            
            for q in queries_list:
                if q.get('enable'):
                    query_types.append({
                        'value': q.get('name'),
                        'placeholder': q.get('display_name')
                    })
            
            # Extract Locations and VRFs
            vps = []
            networks = config.get('networks', [])
            for net in networks:
                for loc in net.get('locations', []):
                    # Find default VRF
                    vrfs = loc.get('vrfs', [])
                    default_vrf = next((v['_id'] for v in vrfs if v.get('default')), None)
                    if not default_vrf and vrfs:
                        default_vrf = vrfs[0]['_id']
                    
                    params = {
                        'query_location': loc.get('_id')
                    }
                    if default_vrf:
                        params['query_vrf'] = default_vrf

                    vps.append({
                        'params': params,
                        'command': {
                            'name': 'query_type',
                            'options': query_types
                        },
                        'input': {
                            'name': 'query_target',
                            'placeholder': ''
                        },
                        'hint': loc.get('name')
                    })

            return {
                'url': url,
                'type': 'react_select', # Next.js version is typically the react one
                'vps': vps,
                'success': True,
                'error': None
            }
            
        except Exception as e:
            return None

    def _open_and_get_menu(self, control_element):
        """
        Helper to open a dropdown menu and return the menu element.
        Simplified logic: If a menu is already visible, use it. Otherwise click and wait.
        """
        menus = self.browser.find_elements(By.CSS_SELECTOR, "div[class*='-menu'][id*='react-select'][id*='listbox']")
        for m in menus:
            if m.is_displayed():
                return m
        
        ActionChains(self.browser).move_to_element(control_element).click().perform()
        
        def wait_for_menu(d):
            candidates = d.find_elements(By.CSS_SELECTOR, "div[class*='-menu'][id*='react-select'][id*='listbox']")
            for c in candidates:
                if c.is_displayed():
                    return c
            return False

        return WebDriverWait(self.browser, 5).until(wait_for_menu)

    def detect_hyperglass_type(self, url):
        """
        Detect which type of Hyperglass page this is
        """
        max_retries = 2
        
        for attempt in range(max_retries):
            try:
                self.init_browser()
                self.browser.get(url)
                
                # Wait for page to load
                WebDriverWait(self.browser, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                time.sleep(1)
                                
                # Check for Chakra UI list style (Type 2)
                # Look for li elements with class starting with chakra-wrap
                chakra_list_elements = self.browser.find_elements(
                    By.CSS_SELECTOR, "li[class*='chakra-wrap']"
                )
                
                if chakra_list_elements:
                    return 'chakra_list'
                    
                return 'react_select'
                
            except TimeoutException as e:
                if self.browser:
                    self.close_browser()
                    
                if attempt == max_retries - 1:
                    return 'react_select'
                    
                time.sleep(1)
                
            except Exception as e:
                if self.browser:
                    self.close_browser()
                    
                if attempt == max_retries - 1:
                    return 'react_select'
                    
                time.sleep(1)
            
    def parse_react_select_options(self, url):
        """
        Parse React Select style Hyperglass page (Type 1)
        Tries Next.js data extraction first, falls back to interactive parsing.
        """
        try:            
            # Extract from Next.js JSON data
            json_result = self.parse_nextjs_data(url)
            if json_result:
                return json_result
        except Exception:
            pass
            
        return self._parse_react_select_interactive(url)

    def _parse_react_select_interactive(self, url):
        """
        Fallback interactive parser for React Select
        """
        try:
            self.init_browser()
            self.browser.get(url)
            
            WebDriverWait(self.browser, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(1)
            
            locations = []
            target_placeholder = ''
            
            controls = self.browser.find_elements(By.CSS_SELECTOR, "input[id*='react-select']")
            if not controls:
                return {'url': url, 'type': 'react_select', 'success': False, 'error': "No react-select controls found"}
                
            loc_control = controls[0]
            
            # 1. Extract Locations
            # Open menu to count options
            menu = self._open_and_get_menu(loc_control)
            options = menu.find_elements(By.CSS_SELECTOR, "div[class*='-option']")
            num_locs = len(options)
                        
            for i in range(num_locs):
                try:
                    # Re-find controls
                    controls = self.browser.find_elements(By.CSS_SELECTOR, "input[id*='react-select']")
                    if not controls:
                        continue
                    loc_control = controls[0]
                    
                    # Open menu
                    menu = self._open_and_get_menu(loc_control)
                    options = menu.find_elements(By.CSS_SELECTOR, "div[class*='-option']")
                    
                    if i >= len(options):
                        break   
                    opt = options[i]
                    name = opt.text.strip()
                    
                    # Scroll into view and click
                    self.browser.execute_script("arguments[0].scrollIntoView({block: 'center'});", opt)
                    time.sleep(0.2)
                    ActionChains(self.browser).move_to_element(opt).click().perform()
                    time.sleep(0.5)
                    
                    # Get value
                    try:
                        val_input = self.browser.find_element(By.CSS_SELECTOR, "input[name='queryLocation'][type='hidden']")
                        value = val_input.get_attribute('value')
                    except:
                        value = location_text_to_value(name)
                        

                    controls = self.browser.find_elements(By.CSS_SELECTOR, "input[id*='react-select']")
                    current_query_types = []
                    query_control = controls[1]
                    
                    # Open query menu
                    q_menu = self._open_and_get_menu(query_control)
                    q_options = q_menu.find_elements(By.CSS_SELECTOR, "div[class*='-option']")
                    num_qs = len(q_options)
                        
                    for j in range(num_qs):
                        try:
                            controls = self.browser.find_elements(By.CSS_SELECTOR, "input[id*='react-select']")
                            query_control = controls[1]
                            
                            q_menu = self._open_and_get_menu(query_control)
                            q_options = q_menu.find_elements(By.CSS_SELECTOR, "div[class*='-option']")
                            
                            if j >= len(q_options):
                                break
                                
                            q_opt = q_options[j]
                            q_text = q_opt.text.strip()
                            
                            # Scroll into view and click
                            self.browser.execute_script("arguments[0].scrollIntoView({block: 'center'});", q_opt)
                            time.sleep(0.2)
                            ActionChains(self.browser).move_to_element(q_opt).click().perform()
                            time.sleep(0.5)
                            
                            try:
                                q_val_input = self.browser.find_element(By.CSS_SELECTOR, "input[name='queryType'][type='hidden']")
                                q_value = q_val_input.get_attribute('value')
                            except:
                                q_value = q_text.lower().replace(' ', '_')
                                
                            current_query_types.append({'placeholder': q_text, 'value': q_value})
                            
                        except Exception as e:
                            print(f"Error extracting query type {j}: {e}")
                            try:
                                input_elem = query_control.find_element(By.CSS_SELECTOR, "input[aria-expanded]")
                                if input_elem.get_attribute("aria-expanded") == "true":
                                    self.browser.find_element(By.TAG_NAME, "body").click()
                                    time.sleep(0.5)
                            except:
                                pass

                    locations.append({'name': name, 'value': value, 'query_types': current_query_types})


                    # Try to find clear button globally
                    clear_btns = self.browser.find_elements(By.CSS_SELECTOR, "div[role='button']")
                    clear_btns[0].click()
                    time.sleep(0.5)
                except Exception as e:
                    print(f"Error processing location {i}: {e}")
                    try:
                        input_elem = loc_control.find_element(By.CSS_SELECTOR, "input[aria-expanded]")
                        if input_elem.get_attribute("aria-expanded") == "true":
                            self.browser.find_element(By.TAG_NAME, "body").click()
                            time.sleep(0.5)
                    except:
                        pass
            
            # 3. Target Input
            try:
                target_input = self.browser.find_element(
                    By.CSS_SELECTOR, "input[placeholder*='target' i], input[placeholder*='host' i], input[placeholder*='ip' i]"
                )
                target_placeholder = target_input.get_attribute('placeholder') or 'Target'
            except:
                pass
                
            # Build VPs
            vps = []
            for loc in locations:
                vps.append({
                    'params': {
                        'queryLocation': loc['value']
                    },
                    'command': {
                        'name': 'queryType',
                        'options': loc['query_types']
                    },
                    'input': {
                        'name': 'queryTarget',
                        'placeholder': target_placeholder,
                        'is_list': True
                    },
                    'hint': loc['name']
                })
                
            return {
                'url': url,
                'type': 'react_select',
                'vps': vps,
                'success': True,
                'error': None
            }
            
        except Exception as e:
            return {
                'url': url,
                'type': 'react_select',
                'success': False,
                'error': str(e)
            } 
            
    def parse_chakra_list_options(self, url):
        """
        Parse Chakra UI list style Hyperglass page (Type 2)
        """
        try:
            self.init_browser()
            self.browser.get(url)
            
            WebDriverWait(self.browser, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(2)
            
            locations = []
            query_types = []
            target_placeholder = ''
            
            # 1. Extract Locations (without clicking)
            location_items = self.browser.find_elements(
                By.CSS_SELECTOR, "li[class*='chakra-wrap']"
            )
            
            if not location_items:
                location_items = self.browser.find_elements(
                    By.CSS_SELECTOR, "li[class*='chakra'], li[class*='css-']"
                )
            
            for i, item in enumerate(location_items):
                try:
                    # Extract text (prefer headings)
                    location_text = ""
                    try:
                        title_element = item.find_element(By.CSS_SELECTOR, "h1, h2, h3, h4, h5, h6")
                        location_text = title_element.text.strip()
                    except NoSuchElementException:
                        pass
                    
                    if not location_text:
                        # Fallback to item text but exclude img roles
                        location_text = item.text.strip()
                        
                    if not location_text:
                        continue
                        
                    location_value = location_text_to_value(location_text)
                    
                    location_data = {
                        'value': location_value,
                        'name': location_text,
                        'placeholder': location_text
                    }
                    locations.append(location_data)
                    
                except Exception as e:
                    continue

            # 2. Extract Query Types (Shared)
            # Need click one location to reveal query types

            if location_items:
                ActionChains(self.browser).move_to_element(location_items[0]).click().perform()
                time.sleep(1)
            
            try:
                # Find the query type control
                query_input = None
                try:
                    # Try to find input that follows a placeholder div (React-Select structure)
                    query_input = self.browser.find_element(By.XPATH, "//div[contains(@class, 'placeholder')]/following-sibling::div//input")
                except NoSuchElementException:
                    # Fallback: try specific aria-label
                    try:
                        query_input = self.browser.find_element(By.CSS_SELECTOR, "input[aria-label='Query Type']")
                    except NoSuchElementException:
                        pass
                
                if query_input:
                    # Find the parent control div
                    query_control = query_input.find_element(By.XPATH, "./ancestor::div[contains(@class, 'control')]")
                    
                    # Use helper to open and find menu
                    menu = self._open_and_get_menu(query_control)
                    
                    # Get count of options first
                    options = menu.find_elements(By.CSS_SELECTOR, "div[class*='option']")
                    num_options = len(options)
                    
                    # Close menu to reset state before iteration
                    try:
                        self.browser.find_element(By.TAG_NAME, "body").click()
                        time.sleep(0.5)
                    except:
                        pass
                        
                    # Iterate through options to get real values
                    for j in range(num_options):
                        try:
                            # Re-find control to avoid StaleElementReferenceException
                            # The DOM might have updated after selecting an option
                            current_query_input = None
                            try:
                                current_query_input = self.browser.find_element(By.XPATH, "//div[contains(@class, 'placeholder')]/following-sibling::div//input")
                            except NoSuchElementException:
                                try:
                                    current_query_input = self.browser.find_element(By.CSS_SELECTOR, "input[aria-label='Query Type']")
                                except NoSuchElementException:
                                    pass
                            
                            if not current_query_input:
                                break
                                
                            current_query_control = current_query_input.find_element(By.XPATH, "./ancestor::div[contains(@class, 'control')]")

                            # Use helper to open and find menu
                            menu = self._open_and_get_menu(current_query_control)
                            
                            # Re-find options
                            options = menu.find_elements(By.CSS_SELECTOR, "div[class*='option']")
                            if j >= len(options):
                                break
                                
                            option = options[j]
                            option_text = option.text.strip()
                            
                            if option_text:
                                # Scroll into view and click
                                self.browser.execute_script("arguments[0].scrollIntoView({block: 'center'});", option)
                                time.sleep(0.2)
                                ActionChains(self.browser).move_to_element(option).click().perform()
                                time.sleep(0.5) # Wait for React state update
                                
                                # Extract real value from hidden input
                                actual_value = extract_query_type_value(self.browser)
                                
                                # Fallback
                                if not actual_value:
                                    actual_value = option_text.lower().replace(' ', '_')
                                
                                query_types.append({
                                    'value': actual_value,
                                    'placeholder': option_text
                                })
                        except Exception as e:
                            # Try to reset
                            try:
                                self.browser.find_element(By.TAG_NAME, "body").click()
                            except:
                                pass
                            continue
                            
            except Exception as e:
                pass

            # Find target input
            try:
                target_input = self.browser.find_element(
                    By.CSS_SELECTOR, "input[placeholder*='target' i], input[placeholder*='host' i], input[placeholder*='ip' i]"
                )
                target_placeholder = target_input.get_attribute('placeholder') or 'Target'
            except NoSuchElementException:
                pass
            
            # Build VPs
            vps = []
            for loc in locations:
                vps.append({
                    'params': {
                        'queryLocation': loc['value']
                    },
                    'command': {
                        'name': 'queryType',
                        'options': query_types
                    },
                    'input': {
                        'name': 'queryTarget',
                        'placeholder': target_placeholder,
                        'is_list': True
                    },
                    'hint': loc['name']
                })

            return {
                'url': url,
                'type': 'chakra_list',
                'vps': vps,
                'success': True,
                'error': None
            }
            
        except Exception as e:
            return {
                'url': url,
                'type': 'chakra_list',
                'success': False,
                'error': str(e)
            }

    def parse_hyperglass_page(self, url):
        """
        Main method to parse any Hyperglass page
        Auto-detects the type and uses appropriate parser
        """
        try:
            page_type = self.detect_hyperglass_type(url)
            
            if page_type == 'react_select':
                return self.parse_react_select_options(url)
            elif page_type == 'chakra_list':
                return self.parse_chakra_list_options(url)
            else:
                return {
                    'url': url,
                    'type': 'unknown',
                    'success': False,
                    'error': 'Not a recognized Hyperglass page'
                }
        except Exception as e:
            return {
                'url': url,
                'success': False,
                'error': str(e)
            }
        finally:
            self.close_browser()

# Convenience functions
def parse_hyperglass_url(url):
    parser = HyperglassParser()
    try:
        return parser.parse_hyperglass_page(url)
    finally:
        parser.close_browser()