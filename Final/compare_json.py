import json
import os
import argparse
import sys
from collections import defaultdict

# python compare_json.py \
#   --env bld-01=d:\Course\Code\bld-01\metric.json.template \
#   --env int-01=d:\Course\Code\int-01\metric.json.template \
#   --env prd-01=d:\Course\Code\prd-01\metric.json.template \
#   --env pre-01=d:\Course\Code\pre-01\metric.json.template

def load_json(path):
    """Loads JSON data from a file."""
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as e:
        return f"Error decoding JSON: {e}"

def get_metrics_map(data, env_name):
    """Converts list of metrics to a dict keyed by metric_id."""
    metrics_map = {}
    if isinstance(data, list):
        for item in data:
            if 'metric_id' in item:
                metrics_map[item['metric_id']] = item
            else:
                 print(f"Warning: Item in {env_name} missing 'metric_id'")
    return metrics_map

def parse_env_args(args):
    """Parses env arguments in format 'name=path'."""
    env_paths = {}
    if not args.env:
         return None
    
    for item in args.env:
        try:
            name, path = item.split('=', 1)
            env_paths[name] = path
        except ValueError:
            print(f"Error: Invalid format for --env '{item}'. Expected 'name=path'")
            sys.exit(1)
    return env_paths

def compare_envs(env_paths):
    env_data = {}
    all_metric_ids = set()

    # Load data
    print("Loading files...")
    for env, path in env_paths.items():
        data = load_json(path)
        if data is None:
            print(f"Error: File not found for {env} at {path}")
            env_data[env] = {}
        elif isinstance(data, str): # Error message
            print(f"Error in {env}: {data}")
            env_data[env] = {}
        else:
            metrics_map = get_metrics_map(data, env)
            env_data[env] = metrics_map
            all_metric_ids.update(metrics_map.keys())

    sorted_ids = sorted(list(all_metric_ids))
    
    print("\n--- Comparison Report ---")
    print(f"Comparing Environments: {', '.join(sorted(env_paths.keys()))}\n")

    for mid in sorted_ids:
        # Check presence
        present_in = [env for env in env_paths if mid in env_data[env]]
        missing_in = [env for env in env_paths if mid not in env_data[env]]
        
        if missing_in:
            print(f"Metric ID {mid}: MISMATCH - Missing in: {', '.join(missing_in)}")
            continue

        # Compare content
        ref_env = present_in[0]
        ref_obj = env_data[ref_env][mid]
        
        diffs = defaultdict(dict)
        has_diff = False
        
        # Check against other environments
        for env in present_in[1:]:
            curr_obj = env_data[env][mid]
            
            # Check all keys in ref_obj
            for k, v in ref_obj.items():
                if k not in curr_obj:
                    diffs[k][ref_env] = v
                    diffs[k][env] = "<MISSING KEY>"
                    has_diff = True
                elif curr_obj[k] != v:
                    diffs[k][ref_env] = v
                    diffs[k][env] = curr_obj[k]
                    has_diff = True
            
            # Check for keys in curr_obj but not in ref_obj
            for k, v in curr_obj.items():
                if k not in ref_obj:
                    diffs[k][ref_env] = "<MISSING KEY>"
                    diffs[k][env] = v
                    has_diff = True

        if not has_diff:
            print(f"Metric ID {mid}: Matches across: {', '.join(present_in)}")
        else:
            print(f"Metric ID {mid}: DIFFERENCE DETECTED")
            for k, val_map in diffs.items():
                print(f"  Key '{k}':")
                # Add the reference value if not explicitly in diff map (for context)
                if ref_env not in val_map:
                     val_map[ref_env] = ref_obj.get(k, "<MISSING KEY>")
                
                for env_name, val in val_map.items():
                    print(f"    {env_name}: {repr(val)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare metric.json.template files across environments.")
    parser.add_argument('--env', action='append', required=True, help="Environment definition in format 'name=path'. Can be used multiple times.")
    
    args = parser.parse_args()
    
    env_paths = parse_env_args(args)
    if env_paths:
        compare_envs(env_paths)
