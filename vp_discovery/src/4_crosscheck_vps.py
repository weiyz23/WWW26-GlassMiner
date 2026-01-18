# Accroding to the result in 3_discover_vps.py and all the tcpdump files, 
# cross check to find the ip of unknown VPs
from collections import defaultdict
import dpkt
import socket
import sys
import os
import pickle
import operator
import ast
import json

from configs import *
from utils import *


def get_valid_ip(src, dst, idx):
    if (src == HOSTS[idx]['public_ip']) or (src == HOSTS[idx]['private_ip']):
        return dst
    else:
        return src
    
def get_the_responsive_vps(dict_intersection_candidates, dict_threshold_candidates, set_confirmed_ip):
    new_lg_dict = {}
    for lg_idx in dict_intersection_candidates:
        ip_addr = None
        intersection_candidates = dict_intersection_candidates[lg_idx] - set_confirmed_ip
        threshold_candidates = dict_threshold_candidates[lg_idx] - set_confirmed_ip
        if len(intersection_candidates) == 1:
            ip_addr = intersection_candidates.pop()
        elif len(threshold_candidates) == 1:
            ip_addr = threshold_candidates.pop()
        if ip_addr:
            set_confirmed_ip.add(ip_addr)
            new_lg_dict[lg_idx] = ip_addr
    print('----------------------')
    print(f'{len(new_lg_list)} new VPs have been found.')
    return new_lg_dict, set_confirmed_ip


def binary_search_timestamp(timestamps, target_time):
    left, right = 0, len(timestamps) - 1
    while left <= right:
        mid = (left + right) // 2
        if timestamps[mid][0] <= target_time:
            left = mid + 1
        else:
            right = mid - 1
    return left

if __name__ == "__main__":
    unknown_vp_list = json.load(open(os.path.join(OUTPUT_DIR, "unknown_vp_list.json"), "r"))
    unknown_vp_list = unknown_vp_list
    # start_time & end_time timestamp list
    time_lists = []
    vp_num = len(unknown_vp_list)
    # read the start_time and end_time from the tcpdump file
    for m_idx in range(0, len(HOSTS)):
        time_list = []
        time_file_path = os.path.join(OUTPUT_DIR, f'{m_idx}_send.txt')
        with open(time_file_path, 'r') as srcfile:
            # 2 rows in a pair, start and end
            lines = srcfile.readlines()
            for idx in range(len(unknown_vp_list)):
                start_time = float(lines[2 * idx])
                end_time = float(lines[2 * idx + 1])
                time_list.append((start_time, end_time))
        time_lists.append(time_list)

    print("Start reading pcap files...")

    # log the timestamp of the icmp packet
    icmp_timestamp_list = [[] for _ in range(len(HOSTS))]
    read_count = 0
    for m_idx in range(0, len(HOSTS)):
        with open(os.path.join(OUTPUT_DIR, HOSTS[m_idx]['local_path']), 'rb') as fr:
            pcap = dpkt.pcap.Reader(fr)
            for timestamp, buffer in pcap:
                ethernet = dpkt.ethernet.Ethernet(buffer)
                if not isinstance(ethernet.data, dpkt.ip.IP):
                    continue
                ip = ethernet.data
                if not isinstance(ip.data, dpkt.icmp.ICMP):
                    continue

                icmp = ip.data
                src_ip = socket.inet_ntoa(ip.src)
                dst_ip = socket.inet_ntoa(ip.dst)
                this_ip = get_valid_ip(src_ip, dst_ip, m_idx)
                # mark the timestamp of the icmp packet
                icmp_timestamp_list[m_idx].append((timestamp, this_ip))
                read_count += 1
                if read_count % 100000 == 0:
                    print(f"{read_count} packets have been read.")
            # sort by timestamp, ascending
            icmp_timestamp_list[m_idx].sort(key=operator.itemgetter(0))
    print("Finished reading pcap files.")
    
    # intersection the icmp timestamp with time_list duration 
    intersect_dict = defaultdict(lambda: defaultdict(set))
    for m_idx in range(0, len(HOSTS)):
        for lg_idx in range(vp_num):
            start_time, end_time = time_lists[m_idx][lg_idx]
            cur_idx = 0
            cur_idx = binary_search_timestamp(icmp_timestamp_list[m_idx], start_time)
                
            for timestamp, this_ip in icmp_timestamp_list[m_idx][cur_idx:]:
                if timestamp < end_time:
                    intersect_dict[m_idx][lg_idx].add(this_ip)
                else:
                    break

    new_lg_list = []
    threshold = 2 * len(HOSTS) / 3
    processed_count = 0
    dict_intersection_candidates = {}
    dict_threshold_candidates = {}
    dict_total_ip_count = {}
    for lg_idx in range(vp_num):
        intersection_candidates = set()
        threshold_candidates = set()
        show_up_count = {}
        for m_idx in range(len(HOSTS)):
            for ip in intersect_dict[m_idx][lg_idx]:
                if ip not in show_up_count:
                    show_up_count[ip] = 1
                else:
                    show_up_count[ip] += 1
                    if show_up_count[ip] >= threshold:
                        threshold_candidates.add(ip)
                    if show_up_count == len(HOSTS):
                        intersection_candidates.add(ip)
                if ip not in dict_total_ip_count:
                    dict_total_ip_count[ip] = 1
                else:
                    dict_total_ip_count[ip] += 1
        dict_intersection_candidates[lg_idx] = intersection_candidates
        dict_threshold_candidates[lg_idx] = threshold_candidates    
    
    set_confirmed_ip = set()
    # Remove the background noise by check the number of ip count
    # That is, no less than max(2, 0.4 * vp_num) of the total number
    for ip, count in dict_total_ip_count.items():
        if count >= 0.3 * vp_num and count >= 2:
            set_confirmed_ip.add(ip)
    print("background noise ip:", set_confirmed_ip)
    
    new_lg_dict, set_confirmed_ip = get_the_responsive_vps(dict_intersection_candidates, dict_threshold_candidates, set_confirmed_ip)
    
    while len(new_lg_dict) > 0:
        for lg_idx in new_lg_dict:
            ip_addr = new_lg_dict[lg_idx]
            vp_info = unknown_vp_list[lg_idx]
            vp_info['ip_addr'] = ip_addr
            geolocation = geolocate_one_vp(vp_info)
            vp_info['location'] = geolocation[0]
            new_lg_list.append(vp_info)
        new_lg_dict, set_confirmed_ip = get_the_responsive_vps(dict_intersection_candidates, dict_threshold_candidates, set_confirmed_ip)
                    
    # write to files
    print('----------------------')
    print('Unique LG list:', len(new_lg_list))
    with open(os.path.join(OUTPUT_DIR, "active_unknown_vp_list.json"), "w") as f:
        json.dump(new_lg_list, f, indent=4)