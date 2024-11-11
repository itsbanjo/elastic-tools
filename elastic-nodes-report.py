import json
import pandas as pd
from typing import Dict, List
import os

def bytes_to_gb(bytes_value: int) -> float:
    """Convert bytes to GB"""
    return round(bytes_value / (1024 * 1024 * 1024), 2)

def parse_cat_nodes(file_path: str) -> Dict[str, Dict]:
    """Parse cat_nodes.txt and return a dictionary of node metrics"""
    node_metrics = {}
    try:
        with open(file_path, 'r') as f:
            lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            headers = lines[0].split()
            
            for line in lines[1:]:
                values = line.split()
                if len(values) >= len(headers):
                    node_name = values[0].strip()
                    metrics = {
                        'heap_percent': float(values[headers.index('hp')]),
                        'disk_used_percent': float(values[headers.index('dup')]),
                        'load_1m': float(values[headers.index('load_1m')]),
                        'load_5m': float(values[headers.index('load_5m')]),
                        'load_15m': float(values[headers.index('load_15m')])
                    }
                    node_metrics[node_name] = metrics
    except Exception as e:
        print(f"Warning: Error parsing cat_nodes.txt: {str(e)}")
    
    return node_metrics

def create_role_matrix(nodes_data: Dict) -> pd.DataFrame:
    """Create a matrix of node roles"""
    all_roles = [
        'coordinating', 'data', 'data_content', 'data_hot', 'data_warm', 
        'data_cold', 'data_frozen', 'ingest', 'ml', 
        'remote_cluster_client', 'transform', 'master'
    ]
    
    role_data = []
    for node_id, node in nodes_data.get('nodes', {}).items():
        node_name = node.get('name', 'Unknown')
        node_roles = node.get('roles', [])
        
        role_dict = {'Node Name': node_name}
        
        # Mark coordinating nodes (empty roles list means coordinating only)
        role_dict['coordinating'] = 'X' if not node_roles else ''
        
        # Mark data role (if any data-related role exists)
        data_roles = ['data', 'data_content', 'data_hot', 'data_warm', 'data_cold', 'data_frozen']
        has_data_role = any(role in node_roles for role in data_roles)
        role_dict['data'] = 'X' if has_data_role else ''
        
        # Mark other roles
        for role in all_roles[2:]:  # Skip 'coordinating' and 'data'
            role_dict[role] = 'X' if role in node_roles else ''
            
        role_data.append(role_dict)
    
    return pd.DataFrame(role_data)

def generate_nodes_report(json_data: Dict, cat_metrics: Dict) -> pd.DataFrame:
    """Generate a report of node specifications"""
    cluster_name = json_data.get('cluster_name', 'Unknown')
    nodes_data = []
    
    for node_id, node in json_data.get('nodes', {}).items():
        node_name = node.get('name', 'Unknown')
        total_memory = calculate_memory_usage(node)
        cat_node_metrics = cat_metrics.get(node_name, {})
        
        node_info = {
            'Cluster Name': cluster_name,
            'Node Name': node_name,
            'Roles': get_node_roles(node.get('roles', [])),
            'CPUs': node.get('os', {}).get('allocated_processors', 0),
            'Total Memory (GB)': total_memory,
            'Memory Usage (%)': cat_node_metrics.get('heap_percent', 0),
            'Disk Usage (%)': cat_node_metrics.get('disk_used_percent', 0),
            'Load (1m)': cat_node_metrics.get('load_1m', 0),
            'Load (5m)': cat_node_metrics.get('load_5m', 0),
            'Load (15m)': cat_node_metrics.get('load_15m', 0)
        }
        nodes_data.append(node_info)
    
    return pd.DataFrame(nodes_data)

def calculate_memory_usage(node_data: Dict) -> float:
    """Calculate total memory in GB"""
    try:
        heap_max = node_data['jvm']['mem']['heap_max_in_bytes']
        return bytes_to_gb(heap_max)
    except (KeyError, TypeError):
        return 0

def get_node_roles(roles: List) -> str:
    """Convert roles list to readable string"""
    if not roles:
        return "coordinating"
    return ", ".join(sorted(roles))

def main(nodes_json_path: str, cat_nodes_path: str):
    """Main function to read JSON and generate report"""
    try:
        # Read nodes.json
        with open(nodes_json_path, 'r') as f:
            json_data = json.load(f)
        
        # Parse cat_nodes.txt
        cat_metrics = parse_cat_nodes(cat_nodes_path)
        
        # Generate main metrics report
        df_metrics = generate_nodes_report(json_data, cat_metrics)
        
        # Generate role matrix
        df_roles = create_role_matrix(json_data)
        
        # Display the reports
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        
        print("\nElasticsearch Nodes Metrics Report")
        print("=" * 150)
        print(df_metrics.to_string(index=False))
        
        print("\nNode Roles Matrix")
        print("=" * 150)
        print(df_roles.to_string(index=False))
        
        # Save to CSV
        df_metrics.to_csv('elasticsearch_nodes_metrics.csv', index=False)
        df_roles.to_csv('elasticsearch_nodes_roles.csv', index=False)
        print("\nReports saved to 'elasticsearch_nodes_metrics.csv' and 'elasticsearch_nodes_roles.csv'")
        
    except FileNotFoundError as e:
        print(f"Error: File not found - {str(e)}")
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in '{nodes_json_path}'")
    except Exception as e:
        print(f"Error: An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    nodes_json_path = "nodes.json"
    cat_nodes_path = os.path.join("cat", "cat_nodes.txt")
    main(nodes_json_path, cat_nodes_path)
