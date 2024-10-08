import json
import os
from collections import defaultdict
import re
from datetime import datetime, timedelta

def load_json_file(file_path):
    print(f"Loading file: {file_path}")
    with open(file_path, 'r') as file:
        data = json.load(file)
    print(f"File loaded successfully. Type of data: {type(data)}")
    return data

def get_ilm_policies(diagnostic_dir):
    ilm_path = os.path.join(diagnostic_dir, 'commercial', 'ilm_policies.json')
    return load_json_file(ilm_path)

def get_ilm_explain(diagnostic_dir):
    explain_path = os.path.join(diagnostic_dir, 'commercial', 'ilm_explain.json')
    return load_json_file(explain_path)

def get_indices(diagnostic_dir):
    indices_path = os.path.join(diagnostic_dir, 'cat', 'cat_indices.txt')
    indices = {}
    with open(indices_path, 'r') as file:
        next(file)
        for line in file:
            parts = line.split()
            if len(parts) >= 9:
                index_name = parts[2]
                doc_count = int(parts[6])
                store_size = parts[8]
                indices[index_name] = {
                    'doc_count': doc_count,
                    'store_size': store_size
                }
    return indices

def parse_size(size_str):
    units = {'b': 1, 'kb': 1024, 'mb': 1024**2, 'gb': 1024**3, 'tb': 1024**4}
    try:
        if size_str[-2:].lower() in units:
            number = float(size_str[:-2])
            unit = size_str[-2:].lower()
        elif size_str[-1:].lower() in ['b', 'k', 'm', 'g', 't']:
            number = float(size_str[:-1])
            unit = size_str[-1:].lower() + 'b'
        else:
            raise ValueError(f"Unknown size format: {size_str}")
        
        return int(number * units[unit])
    except (ValueError, KeyError) as e:
        print(f"Error parsing size: {size_str}")
        print(f"Error details: {str(e)}")
        return 0

def estimate_daily_ingestion(indices):
    daily_ingestion = defaultdict(int)
    date_pattern = re.compile(r'\d{4}\.\d{2}\.\d{2}')
    
    for index, info in indices.items():
        match = date_pattern.search(index)
        if match:
            date = match.group()
            size_bytes = parse_size(info['store_size'])
            daily_ingestion[date] += size_bytes
    
    if not daily_ingestion:
        return "Unable to estimate daily ingestion rate. No date-based indices found."
    
    total_ingestion = sum(daily_ingestion.values())
    num_days = len(daily_ingestion)
    avg_daily_ingestion = total_ingestion / num_days
    
    # Find the most recent 30 days
    sorted_dates = sorted(daily_ingestion.keys(), reverse=True)[:30]
    recent_30_days_ingestion = sum(daily_ingestion[date] for date in sorted_dates)
    avg_recent_30_days = recent_30_days_ingestion / len(sorted_dates)
    
    return f"""Estimated Daily Ingestion Rate:
Average over all days: {format_size(avg_daily_ingestion)}/day
Average over last 30 days: {format_size(avg_recent_30_days)}/day
Total data ingested: {format_size(total_ingestion)}
Number of days analyzed: {num_days}"""

def generate_ilm_report(diagnostic_dir):
    policies = get_ilm_policies(diagnostic_dir)
    ilm_explain = get_ilm_explain(diagnostic_dir)
    indices = get_indices(diagnostic_dir)

    policy_usage = defaultdict(list)
    total_usage = 0

    if not isinstance(ilm_explain, dict) or 'indices' not in ilm_explain:
        print("Error: ilm_explain does not have the expected structure")
        return "Error: Unable to process ILM explain data"

    for index, info in ilm_explain['indices'].items():
        if isinstance(info, dict) and info.get('managed', False):
            policy_name = info.get('policy')
            if policy_name:
                policy_usage[policy_name].append(index)

    # Calculate total usage
    for index, info in indices.items():
        size_bytes = parse_size(info['store_size'])
        total_usage += size_bytes

    # Generate report
    report = "ILM Policy and Index Usage Report\n"
    report += "================================\n\n"

    report += "Daily Ingestion Rate Estimation:\n"
    report += "--------------------------------\n"
    report += estimate_daily_ingestion(indices) + "\n\n"

    report += "ILM Policies in Use:\n"
    report += "--------------------\n"
    for policy, indices_list in policy_usage.items():
        report += f"Policy: {policy}\n"
        report += f"Number of indices using this policy: {len(indices_list)}\n"
        report += "Indices:\n"
        policy_total_size = 0
        policy_total_docs = 0
        for index in indices_list:
            if index in indices:
                size_bytes = parse_size(indices[index]['store_size'])
                policy_total_size += size_bytes
                policy_total_docs += indices[index]['doc_count']
                report += f"  - {index} (Size: {indices[index]['store_size']}, Docs: {indices[index]['doc_count']})\n"
            else:
                report += f"  - {index} (Size: Unknown, Docs: Unknown)\n"
        report += f"Total size for this policy: {format_size(policy_total_size)}\n"
        report += f"Total documents for this policy: {policy_total_docs}\n"
        report += "\n"

    report += "Total Index Usage:\n"
    report += "------------------\n"
    report += f"Total size of all indices: {format_size(total_usage)}\n\n"

    report += "Policy Details:\n"
    report += "---------------\n"
    if not isinstance(policies, dict):
        print("Error: policies is not a dictionary")
        return "Error: Unable to process policy data"

    for policy_name, policy_details in policies.items():
        report += f"Policy: {policy_name}\n"
        if isinstance(policy_details, dict) and 'policy' in policy_details and 'phases' in policy_details['policy']:
            phases = policy_details['policy']['phases'].keys()
            report += f"Phases: {', '.join(phases)}\n"
        report += "\n"

    return report

def format_size(size_in_bytes):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_in_bytes < 1024.0:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024.0
    return f"{size_in_bytes:.2f} PB"

def main():
    diagnostic_dir = input("Enter the path to the Elasticsearch cluster diagnostic directory: ")
    
    try:
        report = generate_ilm_report(diagnostic_dir)
        print(report)
        
        with open('ilm_report.txt', 'w') as f:
            f.write(report)
        print("Report has been saved to ilm_report.txt")
    
    except FileNotFoundError as e:
        print(f"Error: Required file not found. {str(e)}")
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format in one of the diagnostic files. {str(e)}")
    except KeyError as e:
        print(f"Error: Missing expected data in the diagnostic files. {str(e)}")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
