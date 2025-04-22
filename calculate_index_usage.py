#!/usr/bin/env python3

import json
import pandas as pd
import sys
from pathlib import Path


def bytes_to_gb(bytes_value):
    return bytes_value / (1024 ** 3)  # 1024^3 = 1,073,741,824

def process_elastic_pricing( pricing_file ):
    pricing_data = []

    try:
        with open( pricing_file, 'r') as f:
            pricing = pd.read_csv( f )
            #print(pricing.loc[pricing['product'] == 'aws.es.datawarm.i3en'])
            #print(pricing.query("product == 'aws.es.datawarm.i3en'"))
            #print(pricing.query("product == 'aws.data.highio.i3' and region_code == 'ap-southeast-2'"))
            #print(pricing.loc[(pricing['product'] == 'aws.es.datawarm.i3en') & (pricing['region_code'] == 'us-east-1')].values if 'product' in pricing.columns and 'region_code' in pricing.columns else pd.DataFrame()

    except FileNotFoundError as e:
        print(f"Error: Could not find file - {e}")
        sys.exit(1)
    except pd.errors.EmptyDataError as e:
        print(f"Error: The CSV file is empty - {e}")
        sys.exit(1)
    except pd.errors.ParserError as e:
        print(f"Error: Could not parse the CSV file - {e}")
        sys.exit(1)
    except UnicodeDecodeError as e:
        print(f"Error: File encoding issues - {e}")
        sys.exit(1)
    except PermissionError as e:
        print(f"Error: No permission to access the file - {e}")
        sys.exit(1)
    except MemoryError as e:
        print(f"Error: Not enough memory to load the file - {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

    return pricing

def process_elasticsearch_nodes_data( nodes_stats_file ):

    node_data = []
    try:
        with open(nodes_stats_file, 'r') as f:
            nodes_stat = json.load(f)
    
        nodes = nodes_stat.get("nodes", {})         


        for node_id, node_info in nodes.items():
            node_name = node_info.get("name", "No host information")
            node_roles = node_info.get("roles")
            node_mem = bytes_to_gb(node_info.get('os', {}).get('mem', {}).get('total_in_bytes'))
            node_diskspace = bytes_to_gb(node_info.get('fs', {}).get('total', {}).get('total_in_bytes'))
            node_total_data_size = bytes_to_gb(node_info.get('indices', {}).get('store', {}).get('total_data_set_size_in_bytes'))
            node_freespace = node_diskspace - node_total_data_size
            region = node_info.get('attributes', {}).get('region')
            product = node_info.get('attributes',{}).get("instance_configuration", "No instance configuration")
            availability_zone = node_info.get('attributes',{}).get('availability_zone', "No availability zone")
            node_type = node_info.get('attributes',{}).get('data', 'No node type available')

            node_data.append({
                'id': node_id,
                'node': node_name,
                'roles': node_roles,
                'region_code': region,
                'node_type': node_type,
                'product': product,
                'memory (GB)': node_mem,
                "disk space (GB)": round(node_diskspace,2),
                "data store size (GB)": round(node_total_data_size,2),
                "free space (GB)": round(node_freespace,2)
            })

    except FileNotFoundError as e:
        print(f"Error: Could not find file - {e}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON - {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

    return pd.DataFrame(node_data)


def process_elasticsearch_index_data(indices_stats_file, indices_file, nodes_file, ilm_explain_file):

    try:
        with open(indices_file, 'r') as f:
            indices_data = json.load(f)
        

        indices_df = pd.DataFrame(indices_data)
        
        # Filter out system indices (those starting with '.')
        indices_df = indices_df[~indices_df['index'].str.startswith('.')]
        
        # Add a data_type column based on prirep value
        indices_df['data_type'] = indices_df['prirep'].apply(lambda x: 'primary' if x == 'p' else 'replica')
        

        with open(nodes_file, 'r') as f:
            nodes_data = json.load(f)
        
        # Create a list to store node information
        nodes_list = []
        
        for node_id, node_info in nodes_data['nodes'].items():
            node_attrs = node_info.get('attributes', {})
            nodes_list.append({
                'id': node_id,
                'node_type': node_attrs.get('data', 'unknown'),
                'instance_configuration': node_attrs.get('instance_configuration', 'unknown')
            })
        
        # Create DataFrame from the nodes list
        nodes_df = pd.DataFrame(nodes_list)
        
        # Process indices_stats.json - extract size information
        with open(indices_stats_file, 'r') as f:
            indices_stats_data = json.load(f)
        
        # Create lists to store primary and replica size information
        primary_sizes = []
        replica_sizes = []
        
        for index_name, index_stats in indices_stats_data['indices'].items():
            if not index_name.startswith('.'):  # Skip system indices
                # Get primary size
                primary_size = index_stats.get('primaries', {}).get('store', {}).get('size_in_bytes', 'unknown')
                primary_sizes.append({
                    'index': index_name,
                    'data_type': 'primary',
                    'size': primary_size
                })
                
                # Get replica sizes from shards
                shards = index_stats.get('shards', {})
                for shard_id, shard_list in shards.items():
                    for shard in shard_list:
                        if shard.get('routing', {}).get('primary', True) == False:
                            replica_size = shard.get('store', {}).get('size_in_bytes', 'unknown')
                            replica_sizes.append({
                                'index': index_name,
                                'shard': shard_id,
                                'data_type': 'replica',
                                'size': replica_size
                            })
        
        # Create DataFrames for sizes
        primary_sizes_df = pd.DataFrame(primary_sizes)
        replica_sizes_df = pd.DataFrame(replica_sizes)
        sizes_df = pd.concat([primary_sizes_df, replica_sizes_df])

        # Convert size to numeric format
        sizes_df['size_gb'] = sizes_df['size'].apply(
            lambda x: bytes_to_gb(x) if isinstance(x, (int, float)) else x
        )
        
        # Calculate original data size (before indexing) using the 1.5 factor
        # Only apply to primary shards - replicas are copies and don't represent original ingest
        sizes_df['raw_data_size'] = sizes_df.apply(
            lambda row: row['size'] / 1.5 if row['data_type'] == 'primary' and isinstance(row['size'], (int, float)) else'NA replica',
            axis=1
        )
        
        sizes_df['raw_data_size_gb'] = sizes_df['raw_data_size'].apply(
            lambda x: bytes_to_gb(x) if isinstance(x, (int, float)) else x
        )
        
        # Process ilm_explain.json - extract ILM information
        with open(ilm_explain_file, 'r') as f:
            ilm_data = json.load(f)
        
        ilm_list = []
        
        for index_name, index_ilm in ilm_data.get('indices', {}).items():
            if not index_name.startswith('.'):  # Skip system indices
                ilm_list.append({
                    'index': index_name,
                    'ilm_policy': index_ilm.get('phase_execution', {}).get('policy', 'unknown'),
                    'age': index_ilm.get('age', 'unknown'),
                    'ilm_phase': index_ilm.get('phase', 'unknown')
                })
        
        ilm_df = pd.DataFrame(ilm_list)
        
        # Join all the DataFrames
        
        # First, join indices_df with nodes_df on node ID
        result_df = indices_df.merge(
            nodes_df,
            left_on='id',  # Node ID in indices.json
            right_on='id',  # Node ID in nodes.json
            how='left'
        )
        
        # Join with sizes information
        # For this join, we need to use both index name and data_type
        result_df = result_df.merge(
            sizes_df,
            left_on=['index', 'data_type'],
            right_on=['index', 'data_type'],
            how='left'
        )
        
        # Finally, join with ILM information on index name
        result_df = result_df.merge(
            ilm_df,
            on='index',  # Join on index name
            how='left'
        )
        
        # 6. Clean up and select only the columns we need
        final_df = result_df[[
            'id',
            'index',
            'data_type',
            'node',
            'node_type',
            'instance_configuration',
            'size',
            'size_gb',
            'raw_data_size',       # Added column for original data size in bytes
            'raw_data_size_gb',    # Added column for original data size in GB
            'docs',  # This corresponds to docs_count in the original code
            'ilm_policy',
            'age',
            'ilm_phase'
        ]].rename(columns={'docs': 'docs_count'})

        # Round size values for better readability
        if 'size_gb' in final_df.columns:
            final_df['size_gb'] = final_df['size_gb'].apply(
                lambda x: round(x, 2) if isinstance(x, (int, float)) else x
            )
        
        if 'original_size_gb' in final_df.columns:
            final_df['original_size_gb'] = final_df['original_size_gb'].apply(
                lambda x: round(x, 2) if isinstance(x, (int, float)) else x
            )

        return final_df
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def calculate_pricing(processed_nodes_stat_data, processed_pricing_data):

    # Merge the node data with pricing data using region_code (e.g. ap-southeast-2) and product sku ( e.g. gcp.apm.n2.68x32x45.2 )
    combined_df = processed_nodes_stat_data.merge(processed_pricing_data, on=['region_code', 'product'], how='left')
    
    # Create numeric versions of price columns for calculations
    for col in ['standard', 'gold', 'platinum', 'enterprise']:
        combined_df[f'{col}_numeric'] = combined_df[col].str.replace('$', '').astype(float)

    # Create a condition to identify rows that should have pricing calculated
    # Check if any role contains 'data' and memory is >= 2GB
    combined_df['should_calculate'] = combined_df.apply(
        lambda row: any('data' in role.lower() for role in row['roles']) and row['memory (GB)'] >= 2, 
        axis=1
    )

    # Initialize hourly and yearly columns with zeros
    for col in ['standard/hr', 'gold/hr', 'platinum/hr', 'enterprise/hr', 
                'standard/yr', 'gold/yr', 'platinum/yr', 'enterprise/yr']:
        combined_df[col] = 0.0

    # Calculate costs only for rows that meet the condition
    mask = combined_df['should_calculate']
    
    # Calculate hourly costs
    combined_df.loc[mask, 'standard/hr'] = combined_df.loc[mask, 'memory (GB)'] * combined_df.loc[mask, 'standard_numeric'] 
    combined_df.loc[mask, 'gold/hr'] = combined_df.loc[mask, 'memory (GB)'] * combined_df.loc[mask, 'gold_numeric'] 
    combined_df.loc[mask, 'platinum/hr'] = combined_df.loc[mask, 'memory (GB)'] * combined_df.loc[mask, 'platinum_numeric'] 
    combined_df.loc[mask, 'enterprise/hr'] = combined_df.loc[mask, 'memory (GB)'] * combined_df.loc[mask, 'enterprise_numeric'] 
    
    # Calculate yearly costs (8760 hours in a year)
    combined_df.loc[mask, 'standard/yr'] = combined_df.loc[mask, 'standard/hr'] * 8760
    combined_df.loc[mask, 'gold/yr'] = combined_df.loc[mask, 'gold/hr'] * 8760
    combined_df.loc[mask, 'platinum/yr'] = combined_df.loc[mask, 'platinum/hr'] * 8760
    combined_df.loc[mask, 'enterprise/yr'] = combined_df.loc[mask, 'enterprise/hr'] * 8760

    # Drop the temporary columns
    combined_df = combined_df.drop(['standard_numeric', 'gold_numeric', 'platinum_numeric', 'enterprise_numeric', 'should_calculate'], axis=1)
    
    return combined_df

def calculate_index_costs(indices_df, nodes_pricing):
    merged_df = indices_df.merge(nodes_pricing, 
                                left_on=['id', 'node'], 
                                right_on=['id', 'node'],
                                how='left')
    
    # Create a unique identifier for each index
    merged_df['index_id'] = merged_df['index']
    
    # Group by index and determine shard types
    index_shards = merged_df.groupby('index')['data_type'].apply(list).reset_index()
    index_shards['shard_type'] = index_shards['data_type'].apply(
        lambda x: 'primary+replica' if set(x) == {'primary', 'replica'} else 'primary-only'
    )
    
    # Group by index and calculate costs
    index_costs = merged_df.groupby('index').agg({
        'instance_configuration': 'first',  # Get the instance configuration
        'size': 'sum',  # Sum the sizes of primary and replica shards
        'standard/yr': 'sum',  # Sum the costs across shards
        'gold/yr': 'sum',
        'platinum/yr': 'sum',
        'enterprise/yr': 'sum'
    }).reset_index()
    
    # Add the shard type information
    index_costs = index_costs.merge(index_shards[['index', 'shard_type']], on='index', how='left')
    
    # Calculate size in MB
    index_costs['total_index_size'] = index_costs['size']
    index_costs['total_index_size (MB)'] = index_costs['size'] / (1024 * 1024)
    
    # Reorder and rename columns
    result_df = index_costs[[
        'index', 
        'instance_configuration', 
        'shard_type',
        'total_index_size', 
        'total_index_size (MB)', 
        'standard/yr', 
        'gold/yr', 
        'platinum/yr', 
        'enterprise/yr'
    ]]
    
    # Rename the instance_configuration column to match the desired output
    result_df = result_df.rename(columns={'instance_configuration': 'instance_configuration/product'})
    
    return result_df

# Example usage:
# result = calculate_index_costs(indices_df, nodes_pricing)
# print(result.loc[result['index'] == '<name of index>'])


def main():
    """Main function to process the data and display results"""
    
    # Define file paths
    current_dir = Path.cwd()
    indices_stats_file = current_dir / "indices_stats.json_fixed"
    indices_file = current_dir / "indices.json"
    nodes_file = current_dir / "nodes.json"
    ilm_explain_file = current_dir / "commercial" / "ilm_explain.json"
    nodes_stats_file = current_dir / "nodes_stats.json"
    pricing_file = current_dir / "elastic_pricing.csv"
    
    # Process the data
    indices_df = process_elasticsearch_index_data(indices_stats_file, indices_file, nodes_file, ilm_explain_file)
    pricing_df = process_elastic_pricing(pricing_file)
    nodes_stat_df = process_elasticsearch_nodes_data(nodes_stats_file)
    nodes_pricing = calculate_pricing(nodes_stat_df, pricing_df)
    index_costs = calculate_index_costs(indices_df, nodes_pricing)
    
    # Print other dataframes for inspection
    print("\n=== INDICES INFORMATION ===")
    print(indices_df)
    print("\n=== PRICING INFORMATION ===")
    print(pricing_df)
    print("\n=== NODE STATISTICS ===")
    print(nodes_stat_df)
    print("\n=== NODE PRICING ===")
    print(nodes_pricing)
    print("\n=== INDEX COSTS ===")
    print(index_costs)

    # Export the DataFrames to CSV files
    csv_file = current_dir / "elasticsearch_data.csv"
    index_costs.to_csv(csv_file, index=False)
    print(f"\nData exported to: {csv_file}")
    

if __name__ == "__main__":
    main()