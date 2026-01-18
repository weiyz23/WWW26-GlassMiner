# Clustering the collected LG webpages, then analyse thier templates.

# Post-processing of the seed pages to cluster them based on the similarity of the content.
# We just check if the seed pages have the keyword "Looking Glass" in the title or body.

# Import the required libraries
import json
import numpy as np
import pickle as pkl
import time
from disjoint_set import DisjointSet
from niteru.html_parser import parse_html

# Import the customized content
from configs import *
from utils import *

def calculate_structure_similarity(verified_lg_info):
    """
    Calculate the similarity between each pair of webpages.
    """
    start_time = time.time()
    print("Calculating the structure similarity between each pair of webpages...")
    pair_count = 0
    mat_sim = np.zeros((len(verified_lg_info), len(verified_lg_info)))
    html_1 = None
    html_2 = None
    for i in range(len(verified_lg_info)):
        html_1 = verified_lg_info[i]["content"]
        for j in range(i+1, len(verified_lg_info)):
            html_2 = verified_lg_info[j]["content"]
            pair_count += 1
            mat_sim[i][j] = sequence_similarity(html_1, html_2)
            if pair_count % 100000 == 0:
                print(f"{pair_count} pairs of webpages have been calculated, time elapsed: {time.time() - start_time:.2f}s")
    return mat_sim

def cluster_webpages_by_similarity(verified_lg_info, mat_sim, threshold, abs_threshold):
    """
    For each webpage i, find its most similar webpage j (j > i) that has similarity > threshold,
    and merge them into the same cluster.
    """                
    print(f"Clustering webpages with threshold {threshold}...")
    clusters = DisjointSet({i : i for i in range(len(verified_lg_info))})
    
    # For each webpage i, find its most similar webpage j
    for i in range(len(verified_lg_info)):
        max_sim = 0
        max_j = -1
        # Find the most similar webpage j among remaining webpages
        for j in range(i+1, len(verified_lg_info)):
            # For each webpage with similarity > abs_threshold, directly merge them
            if mat_sim[i][j] > abs_threshold:
                clusters.union(i, j)
            if mat_sim[i][j] > max_sim:
                max_sim = mat_sim[i][j]
                max_j = j
        # If found a similar enough webpage, merge them
        if max_sim > threshold and max_j != -1:
            clusters.union(i, max_j)
    
    cluster_dict = {}
    url2cluster = {}
    # Update the two dictionaries
    for idx, cluster in clusters.itersets(with_canonical_elements=True):
        cluster_dict[idx] = []
        for i in cluster:
            url = verified_lg_info[i]["url"]
            url2cluster[url] = idx
            cluster_dict[idx].append(url)            
    # sort by the number of webpages in the cluster
    cluster_dict = {k: v for k, v in sorted(cluster_dict.items(), key=lambda item: len(item[1]), reverse=True)}
    return cluster_dict, url2cluster

if __name__ == "__main__":
    total_lg_page_list = json.load(open(os.path.join(SHARED_DATA_DIR, TOTAL_FILE), "r"))
    print(f"Total {len(total_lg_page_list)} unique URLs.")
    
    final_clusters = {}
    try:
        final_clusters = json.load(open(os.path.join(OUTPUT_DIR, "final_clusters.json"), "r"))
    except:
        
        verified_lg_info = None
        try:
            verified_lg_info = pkl.load(open(os.path.join(OUTPUT_DIR, "verified_lg_info.bin"), "rb"))
        except:
            verified_lg_info = []
            
            count = 0
            for lg_info in total_lg_page_list:
                url = lg_info["url"]
                filename = lg_info["filename"]
                filepath = os.path.join(SAVE_DIR, filename)
                if not os.path.exists(filepath):
                    continue
                # Extract the content from the seed pages
                seed_content = None
                with open(filepath, "r") as f:
                    seed_content = f.read()
                    if len(seed_content) < TEXT_LEN_MIN_THRESHOLD:
                        continue
                parsed_html = parse_html(seed_content)
                if len(parsed_html.tags) == 0:
                    print(f"Error parsing {url}, skip this page.")
                    continue
                verified_lg_info.append({
                    "url": url,
                    "filename": filename,
                    "content": parsed_html,
                })
                count += 1
                if count % 1000 == 0:
                    print(f"{count} pages have been processed.")
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            pkl.dump(verified_lg_info, open(os.path.join(OUTPUT_DIR, "verified_lg_info.bin"), "wb"))

        os.makedirs(LOGS_DIR, exist_ok=True)
        print("\n=== Structure similarity ===")
        try:
            mat_sim_structure = pkl.load(open(os.path.join(LOGS_DIR, SIM_FILE.format(0)), "rb"))
        except:
            mat_sim_structure = calculate_structure_similarity(verified_lg_info)
            pkl.dump(mat_sim_structure, open(os.path.join(LOGS_DIR, SIM_FILE.format(0)), "wb"))
        
        clusters, url2cluster = cluster_webpages_by_similarity(
            verified_lg_info, mat_sim_structure, 
            threshold=STRUC_THRESHOLD, abs_threshold=STRUC_THRESHOLD + 0.1
        )
        
        # Sort clusters by size
        sorted_clusters = {}
        cluster_sizes = [(cluster_id, len(urls)) for cluster_id, urls in clusters.items()]
        cluster_sizes.sort(key=lambda x: x[1], reverse=True)
        for new_id, (old_id, _) in enumerate(cluster_sizes):
            sorted_clusters[f"structure_cluster_{new_id}"] = clusters[old_id]
        # Save structure clustering results
        with open(os.path.join(OUTPUT_DIR, "final_clusters.json"), "w") as f:
            json.dump(sorted_clusters, f, indent=2)
        print(f"{len(clusters)} clusters found for all the webpages.")