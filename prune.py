# ==============================================================================
# ADVANCED & AUTOMATED PRUNING PIPELINE
# This script automatically finds and merges similar nodes.
# ==============================================================================

from neo4j import GraphDatabase
from thefuzz import fuzz
from collections import defaultdict

# --- SECTION 1: DATABASE CREDENTIALS ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password" # IMPORTANT: Change this

# --- SECTION 2: AUTOMATION CONFIGURATION ---
# The similarity threshold (out of 100) to consider two nodes duplicates.
# Adjust this based on your data. 85-95 is usually a good range.
SIMILARITY_THRESHOLD = 90   

# List of node labels you want to process for deduplication.
LABELS_TO_PROCESS = ["Entity"] # Add other labels like "GPE", etc.

# --- SECTION 3: AUTOMATED PRUNING LOGIC ---

def get_nodes_by_label(tx, label):
    """Fetches all node names for a given label."""
    query = f"MATCH (n:{label}) RETURN n.name AS name"
    result = tx.run(query)
    return [record["name"] for record in result]

def find_duplicate_clusters(node_names, threshold):
    """
    Groups similar node names into clusters.
    Returns a dictionary mapping the "master" name to a list of its duplicates.
    """
    print(f"-> Finding duplicate clusters with threshold > {threshold}%...")
    clusters = defaultdict(list)
    processed_nodes = set()

    for i, name1 in enumerate(node_names):
        if name1 in processed_nodes:
            continue
        
        # This node is the potential "master" of a new cluster
        current_cluster = [name1]
        for j in range(i + 1, len(node_names)):
            name2 = node_names[j]
            if name2 in processed_nodes:
                continue
            
            # Calculate similarity score
            score = fuzz.token_sort_ratio(name1, name2)
            
            if score > threshold:
                current_cluster.append(name2)

        if len(current_cluster) > 1:
            # Sort by length to pick the longest name as the "master"
            current_cluster.sort(key=len, reverse=True)
            master_name = current_cluster[0]
            duplicates = current_cluster[1:]
            clusters[master_name].extend(duplicates)
            
            # Add all nodes in this cluster to the processed set
            for node_name in current_cluster:
                processed_nodes.add(node_name)
    
    print(f"  - Found {len(clusters)} clusters of duplicates.")
    return clusters

def merge_nodes_from_clusters(tx, clusters):
    """
    Takes the generated clusters and merges the duplicate nodes into the master node.
    """
    if not clusters:
        print("-> No duplicate clusters to merge.")
        return 0

    print("-> Merging nodes based on discovered clusters...")
    merged_count = 0
    for master_name, duplicates in clusters.items():
        for duplicate_name in duplicates:
            query = """
                MATCH (duplicate {name: $duplicate_name})
                MATCH (master {name: $master_name})
                CALL apoc.refactor.mergeNodes([duplicate, master], {
                    properties: 'combine', mergeRels: true
                })
                YIELD node
                RETURN node
            """
            result = tx.run(query, duplicate_name=duplicate_name, master_name=master_name)
            if result.single():
                print(f"  - Merged '{duplicate_name}' into '{master_name}'.")
                merged_count += 1
    return merged_count

# --- SECTION 4: MAIN PIPELINE EXECUTION ---

def main():
    """The main function to run the entire automated pruning pipeline."""
    print("Starting ADVANCED graph pruning pipeline...")
    
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        
        with driver.session() as session:
            for label in LABELS_TO_PROCESS:
                print(f"\n--- Processing Label: {label} ---")
                
                # Step 1: Fetch all node names for the current label
                node_names = session.execute_read(get_nodes_by_label, label)
                
                # Step 2: Automatically find duplicate clusters
                clusters = find_duplicate_clusters(node_names, SIMILARITY_THRESHOLD)
                
                # Step 3: Merge the nodes found in the clusters
                session.execute_write(merge_nodes_from_clusters, clusters)

        print("\nAutomated pruning pipeline finished successfully.")

    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        if 'driver' in locals() and driver:
            driver.close()

if __name__ == "__main__":
    main()