# For all crwaled web pages, check if the wen page content contains at least 2 keywords from the list of keywords.

import json
import os
import pickle as pkl

from configs import *
from utils import *
        
if __name__ == "__main__":
    # Load the candidate page list (Now using available page list as the candidate page list)
    # TMP: Now check if it is existing in the SAVE_DIR
    candidate_page_list = json.load(open(os.path.join(SHARED_DATA_DIR, CAND_FILE), "r")) 
    print("Total candidate pages: ", len(candidate_page_list))
    # Check all the candidate pages, filter out the web pages that are not looking glass pages.
    os.makedirs(PROCS_DIR, exist_ok=True)
    filtered_page_list = []
    count = 0
    processed_count = 0      # For breakpoint resume
    for lg_info in candidate_page_list:
        # Check if the webpage contains any filter words.
        dst_filepath = os.path.join(PROCS_DIR, lg_info["filename"])
        if not os.path.exists(dst_filepath) and count > processed_count:
            src_filepath = os.path.join(SAVE_DIR, lg_info["filename"])
            html_str = open(src_filepath, "r", encoding="utf-8").read()
            cleaned_str = collect_text_in_order(html_str)
            if cleaned_str is not None:
                context_content = extract_context_around_keywords(cleaned_str)
                if context_content:
                    # save the content to the PROCS_DIR
                    filename = lg_info["filename"]
                    filepath = os.path.join(PROCS_DIR, filename)
                    with open(filepath, "w", encoding="utf-8") as f:
                        f.write(context_content)
                    filtered_page_list.append(lg_info)
        elif os.path.exists(dst_filepath):
            filtered_page_list.append(lg_info)
        count += 1
        if count % 1000 == 0:
            print("{} processed, {} filtered".format(count, len(filtered_page_list)))
    print("Total filtered pages: ", len(filtered_page_list))
    # Save the filtered page list to file
    with open(os.path.join(OUTPUT_DIR, "filtered_page_list.json"), "w", encoding="utf-8") as f:
        json.dump(filtered_page_list, f, indent=4)