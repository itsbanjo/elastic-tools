#!/usr/bin/env python3
"""
Elasticsearch Index Data Processor

This script combines data from four Elasticsearch JSON files:
- indices_stats.json: Contains detailed statistics for each index
- indices.json: Contains shard information and node mappings
- nodes.json: Contains node attributes and configuration
- ilm_explain.json: Contains ILM policy information for indices

It creates a pandas DataFrame with the combined information and outputs it
in the requested format.
"""

import json
import pandas as pd
import sys
from pathlib import Path

def process_elasticsearch_data(indices_stats_file, indices_file, nodes_file, ilm_explain_file):
    """
    Process Elasticsearch JSON files and combine the data

    Args:
        indices_stats_file (str): Path to indices_stats.json
        indices_file (str): Path to indices.json
        nodes_file (str): Path to nodes.json
        ilm_explain_file (str): Path to ilm_explain.json

    Returns:
        pandas.DataFrame: Combined data from all files
    """
    try:
        # Load the JSON files
        with open(indices_stats_file, 'r') as f:
            indices_stats = json.load(f)

        with open(indices_file, 'r') as f:
            indices = json.load(f)

        with open(nodes_file, 'r') as f:
            nodes = json.load(f)
            
        with open(ilm_explain_file, 'r') as f:
            ilm_explain = json.load(f)

        # Create an empty list to store our combined data
        combined_data = []

        # Process each shard entry in indices.json
        for shard in indices:
            # Extract basic information
            if shard['index'].startswith('.'):
                continue
            else:
                index_name = shard['index']
            node_id = shard['id']
            node_name = shard['node']
            shard_id = shard['shard']
            is_primary = shard['prirep'] == 'p'
            data_type = "primary" if is_primary else "replica"
            docs_count = shard['docs']

            # Get node information from nodes.json
            node_info = nodes['nodes'].get(node_id, {})
            node_attrs = node_info.get('attributes', {})
            node_type = node_attrs.get('data', 'unknown')
            instance_configuration = node_attrs.get('instance_configuration', 'unknown')

            # Get index size from indices_stats.json
            size = 'unknown'
            if index_name in indices_stats['indices']:
                index_stats = indices_stats['indices'][index_name]

                if is_primary:
                    # For primary shards, get size from primaries section
                    size = index_stats.get('primaries', {}).get('store', {}).get('size_in_bytes', 'unknown')
                else:
                    # For replica shards, find in the shards section
                    shards_list = index_stats.get('shards', {}).get(shard_id, [])
                    for s in shards_list:
                        if s.get('routing', {}).get('primary', True) == False:
                            size = s.get('store', {}).get('size_in_bytes', 'unknown')
                            break
            
            # Get ILM information from ilm_explain.json
            ilm_policy = 'unknown'
            ilm_age = 'unknown'
            ilm_phase = 'unknown'
            
            if index_name in ilm_explain.get('indices', {}):
                ilm_info = ilm_explain['indices'][index_name]
                ilm_policy = ilm_info.get('phase_execution', {}).get('policy', 'unknown')
                ilm_age = ilm_info.get('age', 'unknown')
                ilm_phase = ilm_info.get('phase', 'unknown')

            # Add the row to our combined data
            combined_data.append({
                'index': index_name,
                'data': data_type,
                'node': node_name,
                'node_type': node_type,
                'instance_configuration': instance_configuration,
                'size': size,
                'docs_count': docs_count,
                'ilm_policy': ilm_policy,
                'age': ilm_age,
                'ilm_phase': ilm_phase
            })

        # Create the DataFrame
        return pd.DataFrame(combined_data)

    except FileNotFoundError as e:
        print(f"Error: Could not find file - {e}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON - {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

def main():
    """Main function to process the data and display results"""

    # Define file paths - update these to your actual file locations
    current_dir = Path.cwd()
    indices_stats_file = current_dir / "indices_stats.json_fixed"
    indices_file = current_dir / "indices.json"
    nodes_file = current_dir / "nodes.json"
    ilm_explain_file = current_dir / "commercial" / "ilm_explain.json"

    # Process the data
    df = process_elasticsearch_data(indices_stats_file, indices_file, nodes_file, ilm_explain_file)

    # Print header first with new ILM fields
    print("index, data, node, node_type, instance_configuration, size, docs_count, ilm_policy, age, ilm_phase")

    # Print each row
    for _, row in df.iterrows():
        print(f"{row['index']}, {row['data']}, {row['node']}, "
              f"{row['node_type']}, {row['instance_configuration']}, "
              f"{row['size']}, {row['docs_count']}, {row['ilm_policy']}, "
              f"{row['age']}, {row['ilm_phase']}")

    # Export the DataFrame to a CSV file
    csv_file = current_dir / "elasticsearch_data.csv"
    df.to_csv(csv_file, index=False)
    print(f"\nData exported to: {csv_file}")

if __name__ == "__main__":
    main()
