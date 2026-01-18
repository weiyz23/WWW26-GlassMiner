# Here logs all the templates for those webpages can be automatically generated.
# For any VP template, we should get all following fields for them:
# 1. url: the url of the LG page (TODO)
# 2. method: the method of API, including GET or POST (TODO)
# 3. action: the action of the API, including the prefix of the URL and its params.
# 3. params: the params of the VP, including the fields and its options.
#    For those need to select, we keep all the options. For those with fixed values, we only keep the value.
# 4. command: the supported commands of the VP, including the name of query, and all its options.
# 5. input: the required inputs of the VP, including the name of query, and the placeholder.
# ===============================================
# 6. hint: the location hint of the VP (if no IP address is found)
# 7. ip_addr: the ipv4 address of the VP


from configs import *
from utils import *

IPV4_PTN_1 = re.compile(r'ipv4.*?([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)', flags=re.IGNORECASE)
IPV4_PTN_2 = re.compile(r'([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)', flags=re.IGNORECASE)
LOC_PTN = re.compile(r'location.*?:(.*)', flags=re.IGNORECASE)

def extract_one_select(select_soup: BeautifulSoup):
    """
    Extract infomation of one select elements from the soup.
    Find its name, and all its options placeholders and values.
    """
    select_name = select_soup.get('name')
    if not select_name:
        return None
    # Find all the options in this select element
    options = select_soup.find_all('option')
    if not options:
        return None
    option_items = []
    for option in options:
        # Get the value and placeholder of this option
        option_value = option.get('value')
        if not option_value:
            continue
        option_placeholder = option.get_text().strip()
        option_items.append({
            'value': option_value,
            'placeholder': option_placeholder
        })
    select_item = {
        'name': select_name,
        'options': option_items
    }
    return select_item

def extract_one_input(input_soup: BeautifulSoup):
    """
    Extract infomation of one input elements from the soup.
    Find its name, and all its options placeholders and values.
    """
    input_name = input_soup.get('name')
    if not input_name:
        return None
    # Get the value and placeholder of this option
    input_placeholder = input_soup.get('value')
    if not input_placeholder:
        input_placeholder = input_soup.get('placeholder')
        if not input_placeholder:
            input_placeholder = ''
    return {
        'name': input_name,
        'placeholder': input_placeholder
    }

def try_find_action(soup: BeautifulSoup):
    action_form = soup.find('form', {'action': True})
    if action_form:
        action = action_form.get('action')
        if action and ('/' in action or '.' in action):
            return action
    return ''

def parse_template_1(soup: BeautifulSoup, url: str = None) -> list:
    """
    Parse the template #1, which is used by LookingGlass open-source project.
    Typical API format: ajax.php?cmd={}&host={}.
    """
    # Find the form in the soup, find all the select and input elements
    vp_list = []
    vp_info = {}
    # find a select with name 'cmd'
    select_item = soup.find('select', {'name': 'cmd'})
    if not select_item:
        return None
    # find a select with name 'host'
    input_item = soup.find('input', {'name': 'host'})
    if not input_item:
        return None
    select_params = extract_one_select(select_item)
    if select_params:
        vp_info['command'] = select_params
    vp_info['action'] = 'ajax.php'
    # check the input fields of the form
    input_params = extract_one_input(input_item)
    if input_params:
        vp_info['input'] = input_params
    vp_info["params"] = {}
    vp_info['method'] = 'get'
    vp_info['content-type'] = ''
    # Find the ip address after the first "ipv4"
    ip_addr = ""
    ipv4 = soup.find(string=lambda text: text and IPV4_PTN_1.search(text))
    if ipv4:
        ipv4_match = IPV4_PTN_1.search(ipv4)
        if ipv4_match:
            ip_addr = ipv4_match.group(1)
    vp_info['ip_addr'] = ip_addr
    # Find the hint after the first "location"
    hint = ""
    location = soup.find(string=lambda text: text and LOC_PTN.search(text))
    if location:
        raw_hint = location.strip().split(':')[-1].strip()
        if len(raw_hint) > 0:
            hint = raw_hint
        # the hint is in the next sibling tag
        else:
            next_tag = location.find_next()
            if next_tag:
                hint = next_tag.get_text().strip()
    vp_info['hint'] = hint
    vp_list.append(vp_info)
    # Only contains one VP per page.
    return vp_list

def parse_template_2(soup: BeautifulSoup, url: str = None) -> list:
    """
    Parse the template #2, which is used by looking.house project.
    Typical API format: network?url={}&item={}&network={}&input={}.
    """
    # Find all the buttons with "SingleNetwork" in the 'onclick' attribute
    buttons = soup.find_all('button', {'onclick': re.compile(r'SingleNetwork')})
    if not buttons:
        return None
    vp_list = []
    vp_info = {}
    params = {}
    # For each button, check its onclick attribute, find all pieces between the single quotes
    supported_commands = []
    for button in buttons:
        onclick = button.get('onclick')
        if not onclick:
            continue
        # Find all pieces between the single quotes
        pieces = re.findall(r"'(.*?)'", onclick)
        if len(pieces) < 2:
            continue
        # Get the text in the button
        button_text = button.get_text().strip()
        # The first piece is the command, the second piece is the host
        url, item, network = pieces[0], pieces[1], pieces[2]
        params["url"] = url
        params["item"] = item
        command_info = {
            'value': network,
            'placeholder': button_text
        }
        supported_commands.append(command_info)
    # Check if we have supported commands
    if supported_commands:
        # Add the supported commands to the params
        vp_info['command'] = {
            'name': 'network',
            'options': supported_commands
        }
        vp_info['input'] = {
            'name': 'input',
            'placeholder': ''
        }
        vp_info['params'] = params
        vp_info['action'] = 'https://looking.house/action/looking-glass/network' if "looking.house" in url else 'action/looking-glass/network'
        vp_info['method'] = 'post'
        vp_info['content-type'] = 'application/x-www-form-urlencoded'
        # find ipv4 address
        ip_addr = ""
        ipv4 = soup.find(string=lambda text: text and IPV4_PTN_2.search(text))
        if ipv4:
            ipv4_match = IPV4_PTN_2.search(ipv4)
            if ipv4_match:
                ip_addr = ipv4_match.group(1)
        vp_info['ip_addr'] = ip_addr
        hint = ""
        # the location is in a <div class="h4"> tag without any other attributes
        h4_divs = soup.find_all('div', class_='h4')
        pure_h4 = [d for d in h4_divs if d.get('class') == ['h4'] and d.find_next_sibling('span')]
        if len(pure_h4) >= 2:
            country_tag = pure_h4[0]
            city_tag = pure_h4[1]
        else:
            country_tag = None
            city_tag = None
        if country_tag and city_tag:
            country_span = country_tag.find_next_sibling('span')
            city_span = city_tag.find_next_sibling('span')
            if country_span and city_span:
                country_text = country_span.get_text().strip()
                city_text = city_span.get_text().strip()
                if len(country_text) > 0 and len(city_text) > 0:
                    hint = f"{city_text}, {country_text}"
                elif len(country_text) > 0:
                    hint = country_text
                elif len(city_text) > 0:
                    hint = city_text
        vp_info['hint'] = hint
        vp_list.append(vp_info)
    # Only contains one VP per page.
    return vp_list

def parse_template_3(soup: BeautifulSoup, url: str = None) -> list:
    """
    Parse the template #3, which is used by hybula Looking Glass.
    Typical API format: ?csrfToken={}&targetHost={}&backendMethod={}&submitForm={}.
    Note: This template needs query to extract and get the csrfToken.
    """
    # csrfToken, targetHost, backendMethod, submitForm
    # Find a input with name 'csrfToken'
    csrf_token = soup.find('input', {'name': 'csrfToken'})
    # Find a input with name 'targetHost'
    target_host = soup.find('input', {'name': 'targetHost'})
    # Find a select with name 'backendMethod'
    backend_method = soup.find('select', {'name': 'backendMethod'})
    # Find a button with name 'submitForm'
    submit_form = soup.find('button', {'name': 'submitForm'})
    vp_list = []
    if csrf_token and target_host and backend_method and submit_form:
        vp_info = {}
        params = {
            "csrfToken": "",
            "submitForm": "",
        }
        input_item = {
            'name': "targetHost",
            'placeholder': ""
        }
        checkTerms = soup.find('input', {'name': 'checkTerms'})
        if checkTerms:
            params['checkTerms'] = "on"
        supported_commands = extract_one_select(backend_method)
        vp_info['action'] = try_find_action(soup)
        if supported_commands:
            vp_info['command'] = supported_commands
            vp_info['input'] = input_item
            vp_info['params'] = params
            vp_info['method'] = 'post'
            vp_info['content-type'] = 'application/x-www-form-urlencoded'
            # find ipv4 address, in the value attribute of an input
            ip_addr = ""
            # Find the first label with word "IPv4": in the text
            ipv4_label = soup.find('label', string=lambda text: text and "IPv4" in text)
            if ipv4_label:
                ipv4_tag = ipv4_label.find_next('input', {'type': 'text'})        
                if ipv4_tag:
                    ip_addr = ipv4_tag.get('value')
            vp_info['ip_addr'] = ip_addr
            hint_select = soup.find('select', class_='form-select')
            if hint_select:
                selected_option = hint_select.find('option', selected=True)
                if selected_option:
                    hint = selected_option.get_text().strip()
                else:
                    hint = ""
            else:
                hint = ""
            vp_info['hint'] = hint
            vp_list.append(vp_info)
    return vp_list

def parse_template_4(soup: BeautifulSoup, url: str = None) -> list:
    """
    Parse the template #4, which is used by Looking Glass project.
    Typical API format: ?routers={}&query={}&parameter={}&dontlook=.
    """
    # Find the form in the soup, find all the select and input elements
    vp_list = []
    # find a select with name 'routers'
    vp_item = soup.find('select', {'name': 'routers'})
    if not vp_item:
        return None
    # find a select with name 'host'
    command_item = soup.find('select', {'name': 'query'})
    if not command_item:
        return None
    # check if thier are a "dontlook" input
    dontlook_item = soup.find('input', {'name': 'dontlook'})
    is_dontlook = False
    if dontlook_item:
        is_dontlook = True
    supported_commands = extract_one_select(command_item)
    vp_params = extract_one_select(vp_item)
    action = try_find_action(soup)
    if vp_params and supported_commands:
        vp_options = vp_params['options']
        for vp_option in vp_options:
            vp_info = {}
            vp_hint = vp_option["placeholder"]
            vp_value = vp_option["value"]
            vp_info['params'] = {
                'routers': vp_value,
            }
            if is_dontlook:
                vp_info['params']['dontlook'] = ''
            vp_info['command'] = supported_commands
            vp_info['input'] = {
                'name': 'parameter',
                'placeholder': ""
            }
            vp_info['method'] = 'get'
            vp_info['content-type'] = ''
            vp_info['action'] = action
            vp_info['ip_addr'] = ""
            vp_info['hint'] = vp_hint
            vp_list.append(vp_info)
    # Only contains one VP per page.
    return vp_list

def parse_template_5(soup: BeautifulSoup, url: str = None) -> list:
    """
    Parse the template #5, which is used by HSDN PHP LookingGlass open-source project.
    Typical API format: ?command=bgp&protocol=ipv4&query=8.8.8.8&router=GBLX+%7C+Level+3
    """
    vp_list = []
    # Find all the input with name "command"
    command_items = soup.find_all('input', {'type': 'radio'})
    if not command_items:
        return None
    supported_commands = []
    # Find all the input with name "protocol"
    for command_item in command_items:
        command_value = command_item.get('value')
        command_name = command_item.get('name')
        # find next tag with type <label>
        label_item = command_item.find_next('label')
        placeholder = label_item.get_text().strip() if label_item else command_value
        supported_commands.append({
            'value': command_value,
            'placeholder': placeholder
        })
    # Find all the vps
    vp_item = soup.find('select', {'name': 'router'})
    if not vp_item:
        return None
    vp_params = extract_one_select(vp_item)
    if not vp_params:
        return None
    action = try_find_action(soup)
    for vp_option in vp_params['options']:
        vp_info = {}
        vp_info['method'] = 'get'
        vp_info['content-type'] = ''
        vp_hint = vp_option["placeholder"]
        vp_value = vp_option["value "]
        vp_info['params'] = {
            'router': vp_value,
            'protocol': 'ipv4',
        }
        vp_info['command'] = {
            'name': command_name,
            'options': supported_commands
        }
        vp_info['input'] = {
            'name': 'query',
            'placeholder': ""
        }
        vp_info['action'] = action
        vp_info['ip_addr'] = ""
        vp_info['hint'] = vp_hint
        vp_list.append(vp_info)
    return vp_list

def parse_template_6(soup: BeautifulSoup, url: str = None) -> list:
    """
    Parse the template #6, which is used by OPEN BGPD open-source project.
    Typical API format: cgi-bin/bgplg?cmd=traceroute&req=8.8.8.8.
    Note: some webpages already have /cgi-bin/bgplg as the prefix.
    """
    # Find the form in the soup, find all the select and input elements
    vp_list = []
    vp_info = {}
    # find a select with name 'cmd'
    select_item = soup.find('select', {'name': 'cmd'})
    if not select_item:
        return None
    # find a select with name 'host'
    input_item = soup.find('input', {'name': 'req'})
    if not input_item:
        return None
    select_params = extract_one_select(select_item)
    vp_info['command'] = select_params
    action = try_find_action(soup)
    vp_info['action'] = action
    # check the input fields of the form
    input_params = extract_one_input(input_item)
    vp_info['input'] = input_params
    vp_info['method'] = 'get'
    vp_info['content-type'] = ''
    vp_info["params"] = {}
    vp_info['ip_addr'] = ""
    vp_info['hint'] = ""
    vp_list.append(vp_info)
    # Only contains one VP per page.
    return vp_list

def parse_template_minimum(soup: BeautifulSoup, url: str = None) -> list:
    """
    Parse the template #7, which is used by generic Looking Glass project.
    """
    # Find the form in the soup, find all the select and input elements
    vp_list = []
    vp_info = {}
    # find a select with name 'cmd'
    select_item = soup.find('select', {'name': 'test'})
    if not select_item:
        return None
    # find a select with name 'host'
    input_item = soup.find('input', {'name': 'destination'})
    if not input_item:
        return None
    select_params = extract_one_select(select_item)
    vp_info['command'] = select_params
    # check the input fields of the form
    action = try_find_action(soup)
    vp_info['action'] = action
    input_params = extract_one_input(input_item)
    vp_info['input'] = input_params
    vp_info['method'] = 'get'
    vp_info['content-type'] = ''
    vp_info["params"] = {}
    vp_info['ip_addr'] = ""
    vp_info['hint'] = ""
    vp_list.append(vp_info)
    # Only contains one VP per page.
    return vp_list

def parse_template_hyperglass(soup: BeautifulSoup, url: str = None) -> list:
    """
    Parse Hyperglass (React/Chakra UI) based pages.
    Uses Selenium parser if static detection matches.
    """
    if not url:
        return None
        
    # Static detection: Check full HTML source for 'hyperglass' (case-insensitive)
    # Hyperglass might be in script tags, comments, or attributes not visible in get_text()
    if not re.search(r'hyperglass', str(soup), re.IGNORECASE):
        return None

    # Use the Selenium parser
    try:
        hg_data = parse_hyperglass_url(url)
    except Exception as e:
        print(f"Hyperglass parser failed for {url}: {e}")
        return None
        
    if not hg_data or not hg_data.get('success'):
        return None

    # Convert to VP list
    vp_list = []
    vps_data = hg_data.get('vps', [])
    
    for vp_data in vps_data:
        vp_info = {}
        vp_info['url'] = url
        vp_info['method'] = 'post'
        vp_info['content-type'] = 'application/json'
        vp_info['action'] = 'api/query/'        
        vp_info['params'] = vp_data.get('params', {})
        vp_info['command'] = vp_data.get('command', {})
        vp_info['input'] = vp_data.get('input', {})
        vp_info['hint'] = vp_data.get('hint')
        vp_info['ip_addr'] = ""
        
        vp_list.append(vp_info)
        
    return vp_list

template_hook_list = [
    parse_template_1,
    parse_template_2,
    parse_template_3,
    parse_template_4,
    parse_template_5,
    parse_template_6,
    parse_template_minimum,
    parse_template_hyperglass,
]

def parse_one_template(soup: BeautifulSoup, url: str):
    """
    Parse one template from the soup.
    """
    vp_list_with_url = []
    for template_hook in template_hook_list:
        try:
            vp_list = template_hook(soup, url)
        except TypeError:
            # Fallback for hooks that don't accept url yet
            vp_list = template_hook(soup)
            
        if vp_list:
            for vp_info in vp_list:
                vp_info['url'] = url
                vp_list_with_url.append(vp_info)
            return vp_list_with_url
    return vp_list_with_url

