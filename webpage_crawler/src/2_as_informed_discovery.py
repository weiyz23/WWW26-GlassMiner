from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
import pickle as pkl
import json
import regex as re
import tld
from urllib.parse import quote_plus

from configs import *
from utils import *

# Build the mapping from secondary domain to ASN
def build_asn_domain_mapping(dict_as_info):
    """
    Build the mapping from secondary domain to ASN.
    If the PeeringDB contains the ASN information, we directly get the secondary domain from the network page.
    Otherwise, we use the organization name to generate possible secondary domains.
    """
    list_peeringdb = json.load(open(os.path.join(NETWORK_DIR, "peeringdb_net.json"), "r"))["data"]
    # tranverse the dict_peeringdb, and get the "ASN" and "website" url
    dict_asn_domain_mapping = {"fld": {}, "domain": {}, "orgname": {}}
    for as_info in list_peeringdb:
        asn = as_info["asn"]
        website = as_info["website"]
        if len(website) == 0:
            continue
        # extract the secondary domain from the website url
        domain_info = tld.get_tld(website, as_object=True, fail_silently=True)
        if domain_info is None:
            continue
        fld = domain_info.fld # type: ignore
        # if the domain is not in the dict_asn_domain_mapping, add it
        if fld not in dict_asn_domain_mapping["fld"]:
            dict_asn_domain_mapping["fld"][fld] = set()
        dict_asn_domain_mapping["fld"][fld].add(asn)
    print(f"Total {len(dict_asn_domain_mapping['fld'])} unique secondary domains found.")
    # Now we need to generate the secondary domains from the organization name
    for asn, info in dict_as_info.items():
        org_info = info["organization"]
        if not org_info or len(org_info["orgName"]) == 0:
            continue
        orgname = org_info["orgName"].strip().lower()
        # split the orgname by space and special characters using re
        orgname = re.sub(r"[^a-zA-Z0-9]", " ", orgname)
        orgname_list = re.split(r"\s+", orgname)
        # add each into the dict_asn_domain_mapping
        for orgname in orgname_list:
            if len(orgname) < 3:
                continue
            # if the orgname is not in the dict_asn_domain_mapping, add it
            if orgname not in dict_asn_domain_mapping["orgname"]:
                dict_asn_domain_mapping["orgname"][orgname] = set()
            dict_asn_domain_mapping["orgname"][orgname].add(asn)
    print(f"Total {len(dict_asn_domain_mapping['orgname'])} unique organization names found.")
    return dict_asn_domain_mapping

def get_general_asn_info(dict_asn_domain_mapping):
    with open(os.path.join(OUTPUT_DIR, "candidate_urls.bin"), "rb") as f:
        candidate_urls = pkl.load(f)
    set_asn_logs = set()
    count = 0
    
    failed_candidate_urls = set()
    for url in candidate_urls:
        # Step 1: Directly match the ASN from the URL
        asn = extract_asn_from_url(url)
        if asn != 0:
            set_asn_logs.add(asn)
            continue
        # Step 2: Try to match with the secondary domain
        domain_info = tld.get_tld(url, as_object=True, fail_silently=True)
        if domain_info is not None:
            fld = domain_info.fld # type: ignore
            domain = domain_info.domain # type: ignore
            if fld in dict_asn_domain_mapping["fld"]:
                asn_set = dict_asn_domain_mapping["fld"][fld]
                set_asn_logs.update(asn_set)
                continue
        # Step 3: Try to match with the org name
        if domain in dict_asn_domain_mapping["orgname"]:
            asn_set = dict_asn_domain_mapping["orgname"][domain]
            if len(asn_set) == 1:
                set_asn_logs.update(asn_set)
                continue
        failed_candidate_urls.add(url)
    
    print(f"Total {len(failed_candidate_urls)} URLs failed to match the ASN.")

    # Parallelize the process of getting ASN info
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {executor.submit(get_asn_from_url, url): url for url in failed_candidate_urls}
        count = 0
        
        for future in as_completed(future_to_url):
            asn_set = future.result()
            set_asn_logs.update(asn_set)
            count += 1
            if count % 2000 == 0:
                print(f"{count} URLs have been processed.")

    return set_asn_logs

def search_for_one_asn_slice(dict_as_info_slice, index=1):
    """
    Search for one small slice of ASNs
    """
    print(f"Searching for task index {index}...")
    browser = init_browser()    
    time.sleep(3 * (index % 10))
    candidate_urls = set()
    for asn, info in dict_as_info_slice.items():
        try:
            orgname = info["organization"]["orgName"]
            if len(orgname) == 0:
                keyword = quote_plus(f'AS{asn} looking glass')
            else:
                keyword = quote_plus(f'AS{asn} {orgname} looking glass')
            tmp_urls = search_for_one_keyword(browser, keyword, num=5)
        # Sometimes the driver may be blocked by the search engine, just discard the result
        except:
            tmp_urls = []
        if len(tmp_urls) == 0:
            print(f"Cannot find any urls for AS{asn}")
            continue
        else:
            candidate_urls.update(tmp_urls)
    browser.quit()
    return candidate_urls

def generate_one_asn_slice(dict_queue_asn_rank: dict, slice_size=20):
    """
    Generate the task for one slice of ASNs.
    Remove the selected ASNs from the dict_asn_rank.
    """
    # Split the dict_asn_rank into slices
    dict_as_slice = {}
    for asn, info in dict_queue_asn_rank.items():
        if len(dict_as_slice) >= slice_size:
            break
        dict_as_slice[asn] = info
    for asn in dict_as_slice.keys():
        if asn in dict_queue_asn_rank:
            del dict_queue_asn_rank[asn]
    return dict_as_slice

if __name__ == "__main__":
    dict_as_info = {}
    with open(os.path.join(NETWORK_DIR, "as_info.json"), "r") as f:
        dict_as_info = json.load(f)
    # Build the mapping from secondary domain to ASN
    dict_asn_domain_mapping = build_asn_domain_mapping(dict_as_info)
    # Get the ASN information from the crawled URLs
    set_asn_logs = None
    try:
        with open(os.path.join(OUTPUT_DIR, "asn_log.bin"), "rb") as f:
            set_asn_logs = pkl.load(f)            
    except: 
        set_asn_logs = get_general_asn_info(dict_asn_domain_mapping)
        # Save the ASN information
        with open(os.path.join(OUTPUT_DIR, "asn_log.bin"), "wb") as f:
            pkl.dump(set_asn_logs, f)
            
    print(f"Total {len(set_asn_logs)} ASNs have been logged.")
    
    # Build the priority list by rank
    # sort the dict_asn_rank by rank
    dict_as_info = {k: v for k, v in sorted(dict_as_info.items(), key=lambda item: item[1]['rank'], reverse=True)}
    dict_queue_asn_rank = {}
    for asn, info in dict_as_info.items():
        if asn not in set_asn_logs:
            dict_queue_asn_rank[asn] = dict_as_info[asn]
    
    # For parallelization, assume we have 6 machines, select every 6 * x + index ASNs
    # index = 0
    # list_dict_asn_rank = list(dict_queue_asn_rank.items())
    # new_dict_queue_asn_rank = {}
    # for i in range(index, len(dict_queue_asn_rank), 6):
    #    new_dict_queue_asn_rank[list_dict_asn_rank[i][0]] = list_dict_asn_rank[i][1]
    # dict_queue_asn_rank = new_dict_queue_asn_rank
    
    print(f"Total {len(dict_queue_asn_rank)} ASNs to be searched.")

    # Split the results into pieces, with each piece containing 20 ASNs
    # Parallelize the process of getting results, allowing no more than NUM_THREADS threads running at the same time
    os.makedirs(TMP_DIR, exist_ok=True)
    new_candidate_urls = set()
    with ThreadPoolExecutor(NUM_THREADS) as executor:
        index = 0
        futures = set()
        initial_slices = []
        finish_count = 0
        
        # Continue from the breakpoint by checking the log files to find the last continuous index
        existing_index = 0
        existing_urls = set()
        existing_asn = set()
        current_path = os.path.join(TMP_DIR, "asn_crawler_log_{}.bin".format(existing_index))
        while os.path.exists(current_path):
            with open(current_path, "rb") as f:
                tmp_res = pkl.load(f)
                existing_urls.update(tmp_res["url"])
                existing_asn.update(tmp_res["asn"])
            useless_slice = generate_one_asn_slice(dict_queue_asn_rank, slice_size=10)
            if len(useless_slice) == 0:
                break
            existing_index += 1
            current_path = os.path.join(TMP_DIR, "asn_crawler_log_{}.bin".format(existing_index))
        if existing_index > 0:
            # Remove the existing ASNs from the dict_queue_asn_rank
            for asn in existing_asn:
                if asn in dict_queue_asn_rank:
                    del dict_queue_asn_rank[asn]
            new_candidate_urls.update(existing_urls)
            index = existing_index
        
        for i in range(NUM_THREADS):
            # Generate the task for one slice of ASNs
            as_info_slice = generate_one_asn_slice(dict_queue_asn_rank, slice_size=10)
            if len(as_info_slice) == 0:
                break
            # Submit the task to the executor
            futures.add(executor.submit(search_for_one_asn_slice, as_info_slice, index))
            index += 1
        
        while futures:
            # wait for the first completed task
            done, futures = wait(futures, return_when=FIRST_COMPLETED)
            
            # process the completed tasks
            for future in done:
                finish_count += 1
                if finish_count % 10 == 0:
                    print(f"Task {finish_count} has been finished.")
                candidate_url = future.result()
                new_candidate_urls.update(candidate_url)
                # get the ASN from the candidate URLs
                candidate_asn = set()
                for url in candidate_url:
                    asn = extract_asn_from_url(url)
                    if asn != 0:
                        candidate_asn.add(asn)
                    else:
                        asn = get_asn_from_url(url)
                        if len(asn) == 0:
                            continue
                        candidate_asn.update(asn)
                
                for asn in candidate_asn:
                    if asn in dict_queue_asn_rank:
                        del dict_queue_asn_rank[asn]
            
                # log the results after each task
                tmp_res = {"url": candidate_url, "asn": candidate_asn}
                with open(os.path.join(TMP_DIR, "asn_crawler_log_{}.bin".format(finish_count)), "wb") as f:
                    pkl.dump(tmp_res, f)
                
                # dynamically generate the task for one slice of ASNs
                while len(futures) < NUM_THREADS:
                    as_info_slice = generate_one_asn_slice(dict_queue_asn_rank, slice_size=10)
                    if len(as_info_slice) != 0:
                        futures.add(executor.submit(search_for_one_asn_slice, as_info_slice, index))
                        index += 1
                    else:
                        break
    
    # Save the results
    with open(os.path.join(OUTPUT_DIR, "new_candidate_urls.bin"), "wb") as f:
        pkl.dump(new_candidate_urls, f)