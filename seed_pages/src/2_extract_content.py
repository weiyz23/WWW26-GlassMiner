import json
import hashlib
import sys
import regex as re
import os
from configs import *
from utils import *

def load_pages(filename):
    """Load and return seed pages from JSON file."""
    with open(os.path.join(OUTPUT_DIR, filename), "r") as f:
        pages = json.load(f)
    print(f"Total {len(pages)} seed pages loaded.")
    return pages

def clear_directories():
    """Clear contents of verified, unverified, and unrelated directories."""
    for dir_path in [VERIFIED_DIR, UNVERIFIED_DIR, UNRELATED_DIR]:
        os.makedirs(dir_path, exist_ok=True)
        for filename in os.listdir(dir_path):
            os.remove(os.path.join(dir_path, filename))

def get_unique_urls(pages):
    """Calculate hash values and return unique URLs."""
    hash_to_urls = {}
    page_contents = {}
    
    # Calculate hash for each page
    for page in pages:
        try:
            with open(os.path.join(PROCS_DIR, page["filename"]), "r") as f:
                content = f.read()
        except:
            with open(os.path.join(SAVE_DIR, page["filename"]), "r") as f:
                html_text = f.read()
            soup = parse_webpages(html_text)
            if soup is None:
                continue
            cleaned_soup = remove_script_and_style(soup)
            content = collect_text_in_order(cleaned_soup)
            content = "\n".join(content)
            with open(os.path.join(PROCS_DIR, page["filename"]), "w") as f:
                f.write(content)
        page_contents[page["url"]] = content
        
        hash_val = hashlib.md5(content.encode()).hexdigest()
        if hash_val not in hash_to_urls:
            hash_to_urls[hash_val] = []
        hash_to_urls[hash_val].append(page["url"])
    
    # Select shortest non-IP URL for each hash
    unique_urls = {min(urls, key=lambda url: (bool(re.search(r'\b([0-9]{1,3}\.){3}[0-9]{1,3}\b', url)), len(url)))
                  for urls in hash_to_urls.values()}
    
    return unique_urls, page_contents

def process_pages(pages, unique_urls, page_contents):
    """Process pages and sort them into verified/unverified/unrelated."""
    verified_pages = []
    stats = {"verified": 0, "unverified": 0, "unrelated": 0}
    
    for page in pages:
        if page["url"] not in unique_urls:
            continue
            
        content = page_contents[page["url"]]
        result = count_filter_words(content)
        filename = page["filename"]
        
        if result > 2:
            stats["verified"] += 1
            verified_pages.append(page)
            dest_dir = VERIFIED_DIR
        elif result == 2:
            stats["unverified"] += 1
            dest_dir = UNVERIFIED_DIR
        else:
            stats["unrelated"] += 1
            dest_dir = UNRELATED_DIR
            
        os.rename(os.path.join(PROCS_DIR, filename), 
                 os.path.join(dest_dir, filename))
    
    return verified_pages, stats

def process_manual_verification(verified_pages, available_pages):
    """Process manually verified pages from unverified directory."""
    filename_to_info = {info["filename"]: info for info in available_pages}
    newly_verified = []
    
    for filename in os.listdir(UNVERIFIED_DIR):
        if filename in filename_to_info:
            newly_verified.append(filename_to_info[filename])
            os.rename(os.path.join(UNVERIFIED_DIR, filename),
                     os.path.join(VERIFIED_DIR, filename))
    
    verified_pages.extend(newly_verified)
    return verified_pages, len(newly_verified)

def save_results(verified_pages):
    """Save verified pages to JSON file."""
    with open(os.path.join(OUTPUT_DIR, UNIQ_FILE), "w") as f:
        json.dump(verified_pages, f)

def main():
    if len(sys.argv) != 2:
        print("Usage: python ./2_extract_content.py <mode>")
        print("Please select the mode for extraction.")
        print("1-Initial running to simply classify the pages.")
        print("2-Final running to merge pages after manual check.")
        exit(1)
    else:
       mode = int(sys.argv[1])
    available_pages = load_pages(AVAI_FILE)
    
    if mode == 1:
        clear_directories()
        unique_urls, page_contents = get_unique_urls(available_pages)
        verified_pages, stats = process_pages(available_pages, unique_urls, page_contents)
        
        print("\n======== Processing Results ========")
        print(f"Verified pages: {stats['verified']}")
        print(f"Unverified pages: {stats['unverified']}")
        print(f"Unrelated pages: {stats['unrelated']}")
        print(f"Unique pages: {len(unique_urls)}")
        print(f"Duplicates: {len(available_pages) - len(unique_urls)}")
        print("\nPlease verify unverified pages manually and run with mode=2")
        save_results(verified_pages)
        
    elif mode == 2:
        verified_pages = load_pages(UNIQ_FILE)
        verified_pages, newly_verified_count = process_manual_verification(verified_pages, available_pages)
        save_results(verified_pages)
        
        print(f"Added {newly_verified_count} manually verified pages")
        print(f"Total verified pages: {len(verified_pages)}")
if __name__ == "__main__":
    main()
