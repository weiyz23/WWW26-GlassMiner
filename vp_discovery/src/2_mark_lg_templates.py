# According to the clustered result, analyze the LG webpages and discover their VPs.

from functools import partial
import json
from niteru.html_parser import parse_html

from configs import *
from utils import *
from templates import *

def analyse_template_by_cluster(clusters):
    """
    Analyse the templates of the webpages in each cluster.
    """
    total_vp_list = []
    total_process_count = 0
    for cluster_id, cluster in clusters.items():
        if cluster_id == "structure_cluster_0":
            print(f"Skip the bgp.he.net cluster.")
            continue
        print(f"Cluster {cluster_id}:")
        for url in cluster:
            total_process_count += 1
            if total_process_count % 100 == 0:
                print(f"Processing {total_process_count} urls, {len(total_vp_list)} VPs found.")
            file_name = url_to_filename(url)
            file_path = os.path.join(SAVE_DIR, file_name)
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    html_text = f.read()
                    soup = parse_webpages(html_text)
                    vp_list = parse_one_template(soup, url)
                    # update to the total_vp_dict
                    total_vp_list.extend(vp_list)
    return total_vp_list

if __name__ == "__main__":
    with open(os.path.join(OUTPUT_DIR, "final_clusters.json"), "r") as f:
        clusters = json.load(f)
    raw_total_vp_list = analyse_template_by_cluster(clusters)
    # dump the raw total vp list
    with open(os.path.join(OUTPUT_DIR, "raw_total_vp_list.json"), "w") as f:
        json.dump(raw_total_vp_list, f, indent=2)
    # load the raw total vp list
    # raw_total_vp_list = json.load(open(os.path.join(OUTPUT_DIR, "raw_total_vp_list.json"), "r"))
    known_vp_list = []      # with IP and geolocation
    unknown_vp_list = []    # without IP
    print(f"Total {len(raw_total_vp_list)} VPs found.")
    # Geolocate the VPS by IP or Hint.
    geo_by_hint_count = 0
    for vp_info in raw_total_vp_list:
        location, is_hint = geolocate_one_vp(vp_info)
        if is_hint:
            geo_by_hint_count += 1
        if vp_info["ip_addr"] and (not is_bogon(vp_info["ip_addr"])):
            vp_info["location"] = location
            known_vp_list.append(vp_info)
        else:
            unknown_vp_list.append(vp_info)
    print(f"Total {geo_by_hint_count} VPs geolocated by hint.")
    # write to files
    with open(os.path.join(OUTPUT_DIR, "known_vp_list.json"), "w") as f:
        json.dump(known_vp_list, f, indent=4)
    with open(os.path.join(OUTPUT_DIR, "unknown_vp_list.json"), "w") as f:
        json.dump(unknown_vp_list, f, indent=4)