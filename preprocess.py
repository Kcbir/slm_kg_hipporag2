import time
import sys
from pathlib import Path

# Add current directory to path
sys.path.append(str(Path(__file__).parent))

from entity_extractor import load_nlp, extract_entities
from relationship_extractor import find_relationships  
from construct_kg import load_data, write_to_neo4j, get_stats

def run_pipeline():
    """Complete pipeline execution with quality and speed"""
    print("KNOWLEDGE GRAPH PIPELINE")
    print("=" * 50)
    start_time = time.time()
    
    # Step 1: Initialize
    print("\nStep 1: Loading spaCy model...")
    try:
        nlp = load_nlp()
        print("spaCy loaded successfully")
    except Exception as e:
        print(f"Failed to load spaCy: {e}")
        return
    
    # Step 2: Load data
    print("Step 2: Loading data...")
    try:
        texts = load_data(max_records=1000)  # Balanced batch
        if not texts:
            print("No data loaded")
            return
        print(f"Loaded {len(texts)} texts")
    except Exception as e:
        print(f"Failed to load data: {e}")
        return
    
    # Step 3: Entity extraction
    print("Step 3: Extracting entities...")
    total_entities = 0
    valid_texts = []
    
    for i, item in enumerate(texts):
        if i % 100 == 0:
            print(f"  Processing {i}/{len(texts)}...")
        
        try:
            entities = extract_entities(item['text'], nlp)
            if len(entities) >= 2:  # Need minimum entities for relationships
                item['entities'] = entities
                valid_texts.append(item)
                total_entities += len(entities)
        except Exception as e:
            continue  # Skip problematic texts
    
    print(f"Extracted {total_entities} entities from {len(valid_texts)} valid texts")
    
    if not valid_texts:
        print("No valid texts with entities found")
        return
    
    # Step 4: Relationship extraction
    print("Step 4: Extracting relationships...")
    all_triples = []
    all_metadata = []
    
    for i, item in enumerate(valid_texts):
        if i % 50 == 0:
            print(f"  Processing relationships {i}/{len(valid_texts)}...")
        
        try:
            triples = find_relationships(item['text'], item['entities'], nlp)
            for triple in triples:
                all_triples.append(triple)
                all_metadata.append({
                    'domain': item.get('domain', 'unknown'),
                    'title': item.get('title', 'unknown')
                })
        except Exception as e:
            continue  # Skip problematic relationships
    
    print(f"Extracted {len(all_triples)} relationships")
    
    if not all_triples:
        print("No relationships found")
        return
    
    # Quick quality analysis
    print(f"\nQuality Analysis:")
    print(get_stats(all_triples))
    
    # Step 5: Graph construction
    print("\nStep 5: Building knowledge graph...")
    try:
        write_to_neo4j(all_triples, all_metadata)
        print("Knowledge graph created successfully!")
    except Exception as e:
        print(f"Failed to write to Neo4j: {e}")
        return
    
    # Final summary
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\nPIPELINE COMPLETED")
    print("=" * 50)
    print(f"Results:")
    print(f"  • Processed: {len(texts)} texts")
    print(f"  • Valid texts: {len(valid_texts)}")
    print(f"  • Entities found: {total_entities}")
    print(f"  • Relationships: {len(all_triples)}")
    print(f"  • Processing time: {duration:.1f} seconds")
    print(f"  • Speed: {len(texts)/duration:.1f} texts/second")
    
    # Performance rating
    if len(all_triples) > 100 and duration < 60:
        print("Excellent performance")
    elif len(all_triples) > 50:
        print("Good performance")
    else:
        print("Consider increasing MAX_RECORDS for better results")

def main():
    """Main pipeline execution"""
    try:
        run_pipeline()
    except KeyboardInterrupt:
        print("\nPipeline interrupted by user")
    except Exception as e:
        print(f"\nPipeline failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
