# Accroding to the result in 3_discover_vps.py and all the tcpdump files, 
# cross check to find the ip of unknown VPs
import dpkt
import socket
import sys
import os
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
    print(f'{len(new_lg_dict)} new VPs have been found.')
    return new_lg_dict, set_confirmed_ip

if __name__ == "__main__":
    known_vp_list = json.load(open(os.path.join(OUTPUT_DIR, "known_vp_list.json"), "r"))
    known_vp_list = known_vp_list
    # start_time & end_time timestamp list
    time_lists = []
    vp_num = len(known_vp_list)
    # read the start_time and end_time from the tcpdump file
    print("Start reading pcap files...")

    # log the timestamp of the icmp packet
    read_count = 0
    bad_count = 0
    shown_ip_set = set()
    for m_idx in range(0, len(HOSTS)):
        with open(os.path.join(OUTPUT_DIR, HOSTS[m_idx]['local_path']), 'rb') as fr:
            pcap = dpkt.pcap.Reader(fr)
            for timestamp, buffer in pcap:
                try:
                    ethernet = dpkt.ethernet.Ethernet(buffer)
                except:
                    bad_count += 1
                    continue
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
                shown_ip_set.add(this_ip)
                read_count += 1
                if read_count % 100000 == 0:
                    print(f"{read_count} packets have been read.")
    print("Bad packets:", bad_count)
    print("Finished reading pcap files.")
    # intersection the icmp timestamp with time_list duration 
    active_known_vp_list = []
    for vp_info in known_vp_list:
        ip_addr = vp_info['ip_addr']
        if ip_addr in shown_ip_set:
            active_known_vp_list.append(vp_info)
    print("Validated IP list:", len(active_known_vp_list))
   
    # load the old vp_list
    active_unknown_vp_list = json.load(open(os.path.join(OUTPUT_DIR, "active_unknown_vp_list.json"), "r"))
    dict_new_ip_to_lg = {}
    raw_total_vp_list = []
    for vp_info in active_known_vp_list:
        raw_total_vp_list.append(vp_info)
        dict_new_ip_to_lg[vp_info['ip_addr']] = vp_info
    for vp_info in active_unknown_vp_list:
        raw_total_vp_list.append(vp_info)
        dict_new_ip_to_lg[vp_info['ip_addr']] = vp_info
    
    dict_ip_to_lg = {}
    for lg_info in raw_total_vp_list:
        ip_addr = lg_info['ip_addr']
        if ip_addr not in dict_ip_to_lg:
            dict_ip_to_lg[ip_addr] = [lg_info]
        else:
            dict_ip_to_lg[ip_addr].append(lg_info)
    unique_lg_list = []
    for ip_addr, lg_info_list in dict_ip_to_lg.items():
        if len(lg_info_list) == 1:
            unique_lg_list.append(lg_info_list[0])
        elif ip_addr in dict_new_ip_to_lg:
            unique_lg_list.append(dict_new_ip_to_lg[ip_addr])
        else:
            unique_lg_list.append(lg_info_list[0])
    # write to files
    print('----------------------')
    print('Unique LG list:', len(unique_lg_list))
    with open(os.path.join(OUTPUT_DIR, UNIQ_FILE), "w") as f:
        json.dump(unique_lg_list, f, indent=4)