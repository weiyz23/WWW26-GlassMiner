# this is a template file for the LGs to parse the probing results
# Now we only parse the `Traceroute` and `BGP` results
import os
import json
import random
import re
import charset_normalizer
from urllib.parse import urlparse, urljoin, urlencode, parse_qsl
import ipaddress
import requests
import pycountry_convert as pc

from configs import *

# ================ Measurement Helper ================ #

def process_params(vp_info, target_ip='8.8.8.8', command='traceroute'):
    url = vp_info["url"]
    action = vp_info["action"]
    base_url = urljoin(url, action)
    dict_params = {}
    params = dict(vp_info["params"])

    keywords = METHOD_KEYWORDS.get(command, [])
    command_found = False
    for option in vp_info['command']['options']:
        value = option['value'].lower()
        for keyword in keywords:
            if keyword in value:
                dict_params[vp_info['command']['name']] = value
                command_found = True
                break
        if command_found:
            break
    if not command_found:
        return None, None
    
    for name, value in params.items():
        dict_params[name] = value
    dict_params[vp_info["input"]['name']] = target_ip
    
    query = urlencode(dict_params)
    return base_url, query

def trace_to_one_lg(vp_info, target_host):
    """
    Traceroute to the target_host using the LG specified in vp_info.
    The target_host can be an IP or a domain name.
    Returns the response TracerouteLog object or None if failed.
    """
    trace_url, query = process_params(vp_info, target_host, command="traceroute")
    header = BASE_HEADER.copy()
    header["User-Agent"] = random.choice(USER_AGENT_LIST)
    
    trace_log = None
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
                trace_url.replace(vp_info['url'], redirected_url)
            header['Content-Type'] = vp_info['content-type']
            if vp_info['content-type'] == 'application/json':
                json_data = dict(parse_qsl(query))
                query = json.dumps(json_data)
            header['Origin'] = vp_info["url"].rstrip('/')
            header['Referer'] = vp_info["url"] if vp_info["url"].endswith('/') else vp_info["url"] + '/'
            response = session.post(trace_url, data=query, timeout=TIMEOUT, headers=header, verify=False, allow_redirects=True, stream=True)
            response.raise_for_status()

        elif method == "get":
            trace_url = trace_url + '?' + query
            response = session.get(trace_url, timeout=TIMEOUT, headers=header, verify=False, allow_redirects=True, stream=True)
            response.raise_for_status()
            
        # Parse the response
        content = b""
        for chunk in response.iter_content(chunk_size=8192):
            content += chunk
        detected = charset_normalizer.from_bytes(content).best()
        encoding = detected.encoding if detected else 'utf-8'
        response_text = content.decode(encoding, errors='ignore')
        trace_log = parse_traceroute(response_text, vp_info['ip_addr'], target_host)
        
        # New: if the trace_log is None, save the response_text for debugging
        # if trace_log is None:
        #     debug_file = f"debug_trace_{vp_info['ip_addr']}_{target_host}.txt"
        #     with open(debug_file, 'w', encoding='utf-8') as f:
        #         f.write(response_text)
        
    except Exception as e:
        pass
    finally:
        return trace_log

# ================ Traceroute Parser ================ #
class TraceResult:
    def __init__(self, ip, hop_index, rtt, hostname=None):
        self.ip = ip
        self.hop_index = hop_index
        self.rtt = rtt
        self.hostname = hostname

    def to_dict(self):
        return self.__dict__

class TraceLog:
    def __init__(self, target_host):
        self.target_host = target_host
        self.src_ip = None
        self.dest_ip = None
        self.is_successful = False
        # Key: IP address (str), Value: TraceResult object
        self.results: dict[str, TraceResult] = {}

    def add_or_update_trace(self, ip, hop_index, rtt, hostname=None):
    
        if hostname == ip:
            hostname = None
    
        if ip in self.results:
            # IP already exists, update RTT if the new one is smaller
            if rtt < self.results[ip].rtt:
                self.results[ip].rtt = rtt
        else:
            # New IP, add it
            self.results[ip] = TraceResult(ip, hop_index, rtt, hostname)
    
    def to_dict(self):
        return {
            "target_host": self.target_host,
            "dest_ip": self.dest_ip,
            "is_successful": self.is_successful,
            "results": {ip: result.to_dict() for ip, result in self.results.items()}
        }
    
    def is_empty(self):
        return len(self.results) == 0

# Pattern to find parts: host (ip), rtt with unit, ip
# This pattern is designed to capture individual IP/host and RTT entries.
part_pattern = re.compile(
    r"([\w\.\-\_]+)\s+\(([\d\.]+\.\d+)\)"       # G1, G2: Hostname and IP
    r"|(\d+\.?\d*)\s*(ms|msec|s)"               # G3, G4: Latency and unit (PRIORITY HIGHER)
    r"|([\d\.]+\.\d+)"                          # G5: Just an IP (PRIORITY LOWER)
    r"|(\!N)",                                  # G6: Unreachable mark (PRIORITY LOWEST)
)

def is_valid_ip(str_ip):
    try:
        ipaddress.ip_address(str_ip)
        return True
    except ValueError:
        return False

# They parse the response of traceroute from LG and then return a TracerouteLog object
def preprocess_trace_response(response: str) -> str:
    """
    Cleans the raw traceroute response by removing HTML tags and special characters.
    """
    
    try:
        data = json.loads(response)
        response = data.get("Template", "")
        if not response:
            response = data.get("result", "")
    except json.JSONDecodeError:
        pass

    # For some LGs, the traceroute output is inside a <pre> tag
    pre_match = re.search(r'<pre>(.*?)</pre>', response, re.DOTALL | re.IGNORECASE)
    if pre_match:
        response = pre_match.group(1)
        
    # Replace <br> tags with newlines
    response = re.sub(r'<br\s*/?>', '\n', response, flags=re.IGNORECASE)
    # Replace &nbsp; with a space
    response = response.replace('&nbsp;', ' ')
    # Remove any other HTML tags
    response = re.sub(r'<.*?>', '', response)
    
    start_match = re.search(r"traceroute to .*?,", response, re.IGNORECASE)
    if start_match:
        response = response[start_match.start():]
    return response

def parse_traceroute(response: str, ip_addr: str, target_host: str) -> TraceLog | None:
    """
    Parse the traceroute response and return a TracerouteLog object.
    The final log contains unique IPs, each with its minimum hop and minimum RTT.
    Handles cases where a single hop spans multiple lines.
    """
    trace_log = TraceLog(target_host)
    
    response = preprocess_trace_response(response)
    
    # if there's "trace.* to <hostname> (<IP)>" in the response, we can directly extract the target_host and ip_addr
    header_match = re.search(r'trace.* to ([\w\.\-]+) \(([\d\.]+)\)', response, re.IGNORECASE)
    
    if header_match:
        trace_log.dest_ip = header_match.group(2)
    
    lines = response.split('\n')
    current_hop_index = 0
    
    for line in lines:
        line = line.strip()
        if not line:
            continue

        hop_match = re.match(r'^\s*(\d+)\s+(.*)', line)
        
        rest_of_line = ""
        if hop_match:
            current_hop_index = int(hop_match.group(1))
            rest_of_line = hop_match.group(2).strip()
        else:
            # If there's no hop number, use the whole line and the previous hop index.
            # This handles multi-line entries for a single hop.
            if current_hop_index == 0:
                continue # Skip lines before the first hop
            rest_of_line = line

        # Remove all '*' characters to ignore timeouts
        rest_of_line = rest_of_line.replace('*', '')
        # Remove all bracketed content, e.g., [AS12345] or [*]
        rest_of_line = re.sub(r'\s*\[.*?\]', '', rest_of_line)        
        
        parts = part_pattern.finditer(rest_of_line)
        last_host = None
        last_ip = None
        potential_dest_ip = None
        for part_match in parts:
            # host (ip)
            if part_match.group(1) and part_match.group(2):
                last_host = part_match.group(1)
                last_ip = part_match.group(2)
            # rtt
            elif part_match.group(3) and part_match.group(4):
                if not last_ip:
                    continue
                
                rtt_val = float(part_match.group(3))
                unit = part_match.group(4).lower()

                # Convert RTT to ms
                if unit == 's':
                    rtt_in_ms = rtt_val * 1000
                else: # ms, msec
                    rtt_in_ms = rtt_val
                
                # If there is invalid IP, must stop!
                if is_valid_ip(last_ip):
                    trace_log.add_or_update_trace(last_ip, current_hop_index, rtt_in_ms, last_host)
                    potential_dest_ip = last_ip
                else:
                    return None
                
                # Reset after processing a full entry
                last_host = None
                last_ip = None
            # ip
            elif part_match.group(5):
                last_host = None
                last_ip = part_match.group(5)
            
            # Unreachable mark
            if part_match.group(6):
                potential_dest_ip = None

    if trace_log.is_empty():
        return None

    # if no dest_ip is found, use the potential_dest_ip
    if not trace_log.dest_ip and potential_dest_ip:
        trace_log.dest_ip = potential_dest_ip

    if trace_log.dest_ip and trace_log.dest_ip in trace_log.results:
        trace_log.is_successful = True

    if ip_addr:
        trace_log.src_ip = ip_addr

    return trace_log

# ================ Geolocation Utility =================== #
import geoip2.database
import reverse_geocoder
import pickle
import pytricia
from string import digits

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
        name_split = [x.strip('.') for x in name_split if len(x) > 1]
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
    # if there are more than two candidates, return the city with the largest population
    if len(candidates) > 0:
        candidates = sorted(candidates, key=lambda x: x[4], reverse=True)
        return candidates[0]
    elif len(candidates) == 0:
        return None

def haversine_distance(_lat1, _lon1, _lat2, _lon2):
    """Calculate the great circle distance between two points on the earth (specified in decimal degrees)."""
    from math import radians, sin, cos, sqrt, atan2
    lat1, lon1, lat2, lon2 = float(_lat1), float(_lon1), float(_lat2), float(_lon2)
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    r = 6371  # Radius of earth in kilometers
    return c * r

def get_continent(country_code):
    """Get continent from country code."""
    try:
        return pc.country_alpha2_to_continent_code(country_code)
    except (KeyError, TypeError):
        return "Unknown"

def find_one_site_nearby(location, replica_list):
    nearest_site = None
    min_dist = float('inf')
    for replica in replica_list:
        dist = haversine_distance(location['latitude'], location['longitude'], replica['latitude'], replica['longitude'])
        if dist < min_dist:
            nearest_site = replica
            min_dist = dist
    return nearest_site

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
    Geolocate the IP address using GeoLite.
    """
    try:
        response = GEOLITE_READER.city(ip_addr)
    except:
        return None
    raw_lat = response.location.latitude
    raw_lon = response.location.longitude
    if raw_lat is None:
        return None
    raw_coord = (raw_lat, raw_lon)
    return normalize_geolocation(raw_coord)

dict_city_by_name = pickle.load(open(os.path.join(GEOLOCATION_DIR, "dict_city_by_name.bin"), "rb"))
dict_hasspace_city = pickle.load(open(os.path.join(GEOLOCATION_DIR, "dict_hasspace_city.bin"), "rb"))
dict_iata_code = pickle.load(open(os.path.join(GEOLOCATION_DIR, "dict_iata_code.bin"), "rb"))
dict_city_alter_name = pickle.load(open(os.path.join(GEOLOCATION_DIR, "dict_city_alter_name.bin"), "rb"))

def geolocate_hint(hint):
    """
    Geolocate the hint using dictionaries.
    """
    geo_info = check_raw_word(hint)
    location = None
    if geo_info:
        coord = geo_info[0]
        location = normalize_geolocation(coord)
    return location

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