import json
import pickle
import os
import ipaddress
from collections import defaultdict

from utils import *
from configs import *

def get_subnets(ip):
    """Get subnets at /16, /20, /24 prefixes, returned as a dict."""
    subnets = {}
    for prefix in SUBNET_LEN_LIST:
        try:
            network = ipaddress.ip_network(f"{ip}/{prefix}", strict=False)
            subnet_str = str(network.network_address) + f"/{prefix}"
            subnets[prefix] = subnet_str
        except ValueError:
            pass
    return subnets

def find_the_end_hops(tracelog: TraceLog) -> list:
    """
    Return the TraceResult with the [max_hop - 1, max_hop] index.
    """
    max_hop = max(result.hop_index for result in tracelog.results.values())
    second_max_hop = max((result.hop_index for result in tracelog.results.values() if result.hop_index < max_hop), default=0)
    # end_hops = [result for result in tracelog.results.values() if result.hop_index == max_hop]
    end_hops = []
    if second_max_hop > 0:
        cand_end_hops = [result for result in tracelog.results.values() if result.hop_index == second_max_hop]
        end_hops.extend(cand_end_hops)
    return end_hops

def choose_most_frequent_location(locations):
    """Choose the most frequent country and return the most frequent location from a list of locations."""
    if not locations:
        return None
    if len(locations) == 1:
        return locations[0]
    country_count = defaultdict(int)
    location_map = defaultdict(list)
    for loc in locations:
        country = loc.get("country")
        if country:
            country_count[country] += 1
            location_map[country].append(loc)
    if not country_count:
        return None
    most_frequent_country = max(country_count, key=country_count.get)
    # Return the first location in that country (could be improved)
    return location_map[most_frequent_country][0]

def purify_logs(trace_logs):
    """Purify logs by discarding those with invalid IP addresses or hop count > 64."""
    purified = {}
    for lg_idx, log in trace_logs.items():
        if not log.is_successful:
            continue
        # Check hop count > 64
        max_hop = max(result.hop_index for result in log.results.values()) if log.results else 0
        if max_hop > 64:
            continue
        # Check for invalid IPs
        invalid = False
        for result in log.results.values():
            if not is_valid_ip(result.ip):
                invalid = True
                break
        if invalid:
            continue
        purified[lg_idx] = log
    return purified

def analyze_hostnames(trace_logs):
    """Log all hostnames found in the traceroute results."""
    
    hostname_map = defaultdict(lambda: defaultdict(set))
    dst_redirection = defaultdict(set)
    for lg_idx, log in trace_logs.items():
        for result in log.results.values():
            if result.hostname and not is_bogon(result.ip):
                hostname_map[result.ip][result.hostname].add(lg_idx)
        if log.dest_ip:
            dest_result = log.results.get(log.dest_ip)
            if dest_result and dest_result.hostname:
                dst_redirection[log.dest_ip].add(dest_result.hostname)        
    return hostname_map, dst_redirection

def analyze_anycast(trace_logs_dict):
    """Analyze anycast by examining subnets at the end of paths with RTT differences."""
    dest_ip_groups = defaultdict(list)
    for lg_idx, log in trace_logs_dict.items():
        if log.dest_ip:
            dest_ip_groups[log.dest_ip].append((lg_idx, log))
    
    anycast_analysis = {}
    for dest_ip, lg_log_pairs in dest_ip_groups.items():
        candidates = []
        for lg_idx, log in lg_log_pairs:
            dest_result = log.results.get(log.dest_ip)
            if not dest_result:
                continue
            end_hops = find_the_end_hops(log)
            set_subnet = {}
            for result in end_hops:
                if result.ip != dest_ip:
                    subnets = get_subnets(result.ip)
                    for p, s in subnets.items():
                        if p not in set_subnet:
                            set_subnet[p] = set()
                        set_subnet[p].add(s)
                                    
            candidates.append({
                'lg_idx': lg_idx,
                'subnets': set_subnet
            })
        
        # Group candidates into sites based on common subnets by prefix length, separately for each prefix
        prefix_groups = {}
        for prefix in SUBNET_LEN_LIST:
            site_groups = []
            for cand in candidates:
                matched = False
                for site in site_groups:
                    if prefix in site['subnets'] and prefix in cand['subnets']:
                        common = site['subnets'][prefix] & cand['subnets'][prefix]
                        if common:
                            site['candidates'].append(cand)
                            matched = True
                            break
                if not matched:
                    new_subnets = {prefix: cand['subnets'].get(prefix, set())}
                    site_groups.append({'subnets': new_subnets, 'candidates': [cand]})
            prefix_groups[prefix] = site_groups
        
        anycast_analysis[dest_ip] = prefix_groups
    
    return anycast_analysis



def verify_distance(lat1, lon1, lat2, lon2, rtt):
    """Verify if the distance between two points is plausible given the RTT."""
    dist = haversine_distance(lat1, lon1, lat2, lon2)
    max_dist = rtt * EST_SPEED
    return dist <= max_dist

def geolocate_site(site, dest_ip, purified_logs, lg_info_list, replica_list):
    """Geolocate a single anycast site."""
    lg_indices = site['lg_indices']

    # a. Select all LGs with dest_rtt <= 3ms, and choose the nearest replica site
    nearest_lg_list = []
    for lg_idx in lg_indices:
        log = purified_logs.get(lg_idx)
        if not log: continue
        dest_result = log.results.get(dest_ip)
        if not dest_result: continue
        if dest_result.rtt <= 3:
            lg_info = lg_info_list[lg_idx]
            lg_loc = lg_info.get("location")
            if not lg_loc:
                continue
            nearest_lg_list.append((lg_idx, lg_loc))
    
    # directly return if only one LG within 3ms
    if len(nearest_lg_list) == 1:
        lg_idx, lg_loc = nearest_lg_list[0]
        # Find the nearest replica site
        min_dist = float('inf')
        nearest_site = find_one_site_nearby(lg_loc, replica_list)
        if nearest_site:
            return {
                'latitude': nearest_site['latitude'],
                'longitude': nearest_site['longitude'],
                'country_code': nearest_site.get('country_code', ''),
                'city': nearest_site.get('city', ''),
                'radius': 0,
                'num_candidates': 1,
                'candidates': [nearest_site]
            }
    # clustering the LGs within 100km, then choose the largest cluster
    elif len(nearest_lg_list) > 1:
        clusters = []
        for lg_idx, lg_loc in nearest_lg_list:
            matched = False
            for cluster in clusters:
                for member in cluster:
                    dist = haversine_distance(lg_loc['latitude'], lg_loc['longitude'], member['location']['latitude'], member['location']['longitude'])
                    if dist <= 100:
                        cluster.append({'lg_idx': lg_idx, 'location': lg_loc})
                        matched = True
                        break
                if matched:
                    break
            if not matched:
                clusters.append([{'lg_idx': lg_idx, 'location': lg_loc}])
        # Find the largest cluster and the lg with smallest rtt
        largest_cluster = max(clusters, key=len)
        if len(largest_cluster) > 0:
            nearest_lg = None
            min_rtt = float('inf')
            for member in largest_cluster:
                lg_idx = member['lg_idx']
                log = purified_logs.get(lg_idx)
                if not log: continue
                dest_result = log.results.get(dest_ip)
                if not dest_result: continue
                if dest_result.rtt < min_rtt:
                    nearest_lg = member
                    min_rtt = dest_result.rtt
            if nearest_lg:
                nearest_site = find_one_site_nearby(nearest_lg['location'], replica_list)
                if nearest_site:
                    return {
                        'latitude': nearest_site['latitude'],
                        'longitude': nearest_site['longitude'],
                        'country_code': nearest_site.get('country_code', ''),
                        'city': nearest_site.get('city', ''),
                        'radius': 0,
                        'num_candidates': len(largest_cluster),
                        'candidates': [nearest_site]
                    }

    # b. Use mid-hops to triangulate the location
    all_candidate_sites = []
    for lg_idx in lg_indices:
        log = purified_logs.get(lg_idx)
        if not log: continue
        dest_result = log.results.get(dest_ip)
        if not dest_result: continue

        lg_info = lg_info_list[lg_idx]
        lg_loc = lg_info.get("location")
        if not lg_loc:
            print(f"LG {lg_idx} has no location info, skipping.")
            continue

        # Find verified intermediate hops
        mid_hop_list = find_verified_mid_hops(log, dest_ip, site.get('subnets', []), lg_loc)
        # Find candidate replica sites for this LG
        candidate_sites = find_candidate_sites_for_lg(replica_list, lg_info, dest_result, mid_hop_list)
        all_candidate_sites.extend(candidate_sites)

    if not all_candidate_sites:
        print(f"No candidate sites found for any LG tracing to {dest_ip}")
        return None

    # Select the location with the highest count
    location_counts = defaultdict(int)
    for s in all_candidate_sites:
        location_counts[s['index']] += 1
    
    max_index = max(location_counts, key=location_counts.get)
    selected_site = replica_list[max_index]
    
    return {
        'latitude': selected_site['latitude'],
        'longitude': selected_site['longitude'],
        'country_code': selected_site.get('country_code', ''),
        'city': selected_site.get('city', ''),
        'radius': 0,
        'num_candidates': location_counts[max_index],
        'candidates': [selected_site]
    }

def find_verified_mid_hops(log, dest_ip, subnets, lg_location):
    """Find verified intermediate hops from a traceroute log."""
    verified_mid_hops = []
    sorted_hops = sorted(log.results.values(), key=lambda r: r.hop_index, reverse=True)
    dst_rtt = log.results.get(dest_ip).rtt

    for result in sorted_hops:
        if result.ip == dest_ip:
            continue

        mid_loc = None
        source = ""
        if result.hostname:
            mid_loc = geolocate_hint(result.hostname)
        if mid_loc:
            source = 'hostname'
        else:
            in_subnets = any(ipaddress.ip_address(result.ip) in ipaddress.ip_network(subnet) for subnet in subnets)
            if not in_subnets:
                mid_loc = geolocate_ip(result.ip)
                source = "ip"
        
        if mid_loc:
            if verify_distance(lg_location['latitude'], lg_location['longitude'], mid_loc['latitude'], mid_loc['longitude'], result.rtt):
                rtt_diff = max(abs(dst_rtt - result.rtt), 0.3)
                verified_mid_hops.append({
                    "ip": result.ip,
                    "rtt": result.rtt,
                    "rtt_diff": rtt_diff,
                    "location": mid_loc,
                    "source": source
                })
    return verified_mid_hops

def find_candidate_sites_for_lg(replica_list, lg_info, dst_res, verified_mid_hops):
    """Find candidate replica sites verified by an LG and its mid-hops."""
    candidate_sites = []
    lg_verified_sites = []
    lg_lat, lg_lon = lg_info['location']['latitude'], lg_info['location']['longitude']

    for replica_info in replica_list:
        site_lat, site_lon = replica_info['latitude'], replica_info['longitude']
        
        if verify_distance(lg_lat, lg_lon, site_lat, site_lon, dst_res.rtt):
            lg_verified_sites.append(replica_info)

    # Further verify with mid-hops
    for replica_info in lg_verified_sites:
        valid = True
        site_lat, site_lon = replica_info['latitude'], replica_info['longitude']
        for mid_hop in verified_mid_hops:
            mid_loc = mid_hop['location']
            mid_rtt_diff = mid_hop['rtt_diff']
            if not verify_distance(mid_loc['latitude'], mid_loc['longitude'], site_lat, site_lon, mid_rtt_diff):
                valid = False
                break
        if valid:
            candidate_sites.append(replica_info)
    
    if len(candidate_sites) == 0:
        # Find the top-3 replica using the valid mid hosts with lowest rtt_diff
        # The metric is abs(distance between replica and mid-hop - rtt_diff * EMP_SPEED)
        anchor_mid_hop = min(verified_mid_hops, key=lambda x: x['rtt_diff'], default=None)
        if anchor_mid_hop:
            mid_loc = anchor_mid_hop['location']
            mid_rtt_diff = anchor_mid_hop['rtt_diff']
            scored_replicas = []
            for idx, replica in enumerate(lg_verified_sites):
                dist = haversine_distance(mid_loc['latitude'], mid_loc['longitude'], replica['latitude'], replica['longitude'])
                score = abs(dist - mid_rtt_diff * EMP_SPEED)
                scored_replicas.append((score, idx, replica))
            scored_replicas.sort()
            top_replicas = scored_replicas[:3]
            candidate_sites.extend([replica for _, _, replica in top_replicas])
            
    return candidate_sites

def analyze_host(host, trace_logs, replica_info_list, lg_info_list):
    """Run the full analysis pipeline for a single host."""
    cdn_provider = CUSTOMER_TO_CDN.get(host, "Unknown")
    replica_list = replica_info_list.get(cdn_provider, [])
    if not replica_list:
        print(f"No replica info for {host}, skipping analysis.")
        return

    print(f"Analyzing {host}...")
    purified_logs = purify_logs(trace_logs)
    
    # 1. DNS redirection analysis
    dict_hostnames, dst_redirection = analyze_hostnames(purified_logs)
            
    # 2. Anycast analysis
    anycast_analysis = analyze_anycast(purified_logs)

    # Collect anycast candidates with multiple sites
    anycast_candidates = defaultdict(list)
    for dest_ip, prefix_groups in anycast_analysis.items():
        if len(prefix_groups[DEFAULT_SUBNET_LEN]) > 1:
            for i, site in enumerate(prefix_groups[DEFAULT_SUBNET_LEN]):
                lg_indices = [cand['lg_idx'] for cand in site['candidates']]
                subnets = list(site['subnets'].get(DEFAULT_SUBNET_LEN, set()))
                lg_details = []
                lg_rtts = {}
                for lg_idx in lg_indices:
                    lg_info = lg_info_list[lg_idx]
                    lg_details.append({
                        'lg_idx': lg_idx,
                        'src_ip': lg_info.get('ip_addr', ''),
                        'url': lg_info.get('url', '')
                    })
                    # Get RTT from purified logs
                    log = purified_logs.get(lg_idx)
                    if log:
                        dest_result = log.results.get(dest_ip)
                        if dest_result:
                            lg_rtts[lg_idx] = dest_result.rtt
                anycast_candidates[dest_ip].append({
                    'site_index': i + 1,
                    'lg_indices': lg_indices,
                    'subnets': subnets,
                    'lg_details': lg_details,
                    'lg_rtts': lg_rtts
                })
    
    # Save to JSON file for this host
    output_path = os.path.join(OUTPUT_DIR, f"anycast_candidates_{host}.json")
    with open(output_path, 'w') as f:
        json.dump(anycast_candidates, f, indent=4)

    # 3. Geolocation analysis
    successful_sites = []
    unique_locations = set()
    failed_candidates = []
    located_sites_subnets = defaultdict(list)
    for dest_ip, sites in anycast_candidates.items():
        for site in sites:
            final_location = geolocate_site(site, dest_ip, purified_logs, lg_info_list, replica_list)
            
            if final_location:
                site['location'] = final_location  # Add the geolocation result to the site
                successful_sites.append(site)
                unique_locations.add((final_location['latitude'], final_location['longitude']))
                
                # Bind the located site to its subnets
                site_key = f"{final_location['city']}_{final_location['country_code']}"
                if len(site['subnets']) > 0:                
                    located_sites_subnets[site_key].extend(site['subnets'])
            else:
                failed_candidates.append({'dest_ip': dest_ip, 'site': site})

    # Build mapping from lg_idx to its reached site location
    lg_to_site_loc = {}
    for site in successful_sites:
        for lg_idx in site['lg_indices']:
            lg_to_site_loc[lg_idx] = site['location']

    # Analyze each LG that successfully reached the destination
    lg_site_analysis = {}
    for lg_idx, log in purified_logs.items():
        lg_info = lg_info_list[lg_idx]
        lg_loc = lg_info.get("location")
        if not lg_loc:
            continue
        lg_country = lg_loc.get("country_code", "")
        lg_continent = get_continent(lg_country)

        # Reached site (if any)
        reached_site = lg_to_site_loc.get(lg_idx, None)
        
        # RTT to destination
        dest_result = log.results.get(log.dest_ip)
        rtt_to_dest = dest_result.rtt if dest_result else None
        
        # Top 3 closest replicas to the LG
        distances = []
        country_distances = []
        continent_distances = []
        for replica in replica_list:
            dist = haversine_distance(lg_loc['latitude'], lg_loc['longitude'], replica['latitude'], replica['longitude'])
            distances.append((dist, replica))
            country_code = replica.get('country_code')
            if country_code == lg_country:
                country_distances.append((dist, replica))
            if get_continent(country_code) == lg_continent:
                continent_distances.append((dist, replica))
        distances.sort()
        country_distances.sort()
        continent_distances.sort()
        top3_closest_replicas = {
            'overall': [{
                'location': replica,
                'distance': dist
            } for dist, replica in distances[:3]],
            'country': [{
                'location': replica,
                'distance': dist
            } for dist, replica in country_distances[:3]],
            'continent': [{
                'location': replica,
                'distance': dist
            } for dist, replica in continent_distances[:3]]
        }
        
        lg_site_analysis[lg_idx] = {
            'reached_site': reached_site,
            'rtt_to_dest': rtt_to_dest,
            'top3_closest_replicas': top3_closest_replicas
        }

    # Log results
    analysis_data = {
        'anycast_candidates': anycast_candidates,
        'successful_sites': successful_sites,
        'num_unique_locations': len(unique_locations),
        'located_sites_subnets': located_sites_subnets,
        'lg_site_analysis': lg_site_analysis
    }
    output_path = os.path.join(OUTPUT_DIR, f"geolocation_analysis_{host}.json")
    with open(output_path, 'w') as f:
        json.dump(analysis_data, f, indent=4)
    print(f"For {host}, {len(successful_sites)} sites located successfully, {len(failed_candidates)} failed candidates, {len(unique_locations)} unique locations confirmed.")

if __name__ == "__main__":
    # Load results
    tracelog_path = os.path.join(OUTPUT_DIR, "traceroute_results.bin")    
    with open(tracelog_path, 'rb') as f:
        all_results = pickle.load(f)

    # load the replica info
    cdn_info_path = os.path.join(GEOLOCATION_DIR, "cdn_locations.json")
    with open(cdn_info_path, 'r') as f:
        replica_info_list = json.load(f)
        
    # load the LG info
    lg_info_path = os.path.join(SHARED_DATA_DIR, UNIQ_FILE)
    with open(lg_info_path, 'r') as f:
        lg_info_list = json.load(f)

    for host, trace_logs in all_results.items():
        analyze_host(host, trace_logs, replica_info_list, lg_info_list)