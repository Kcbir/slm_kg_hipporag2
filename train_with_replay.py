import json
import random
import spacy

# --- Configuration ---
REPLAY_BUFFER_FILE = "replay_buffer.json"
MODEL_PATH = "./my_custom_model" # Path to your spaCy model
NEW_TRAINING_DATA_FILE = "new_data.json" # Your new batch of training data
REPLAY_SAMPLE_RATIO = 0.2 # Use 20% old data in each new training batch
BUFFER_CAPACITY = 1000 # Maximum number of examples to keep in the buffer

# --- Helper Functions ---

def load_data(file_path):
    """Loads training data from a JSON file."""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return []

def save_data(file_path, data):
    """Saves data to a JSON file."""
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=2)

def update_replay_buffer(buffer_data, new_data, capacity):
    """
    Adds a random sample of new data to the buffer without exceeding capacity.
    This uses reservoir sampling to keep the buffer representative.
    """
    combined = buffer_data + new_data
    if len(combined) <= capacity:
        return combined
    
    # Reservoir sampling
    reservoir = combined[:capacity]
    for i in range(capacity, len(combined)):
        j = random.randint(0, i)
        if j < capacity:
            reservoir[j] = combined[i]
            
    return reservoir

# --- Main Training Logic ---

def train_model_with_replay(nlp, training_data):
    """

    This is a placeholder for your actual model training code.
    You would replace this with your spaCy training loop.
    For now, it just demonstrates the concept.

    """
    print(f"\n--- Starting training with {len(training_data)} examples ---")
    #
    # YOUR SPACY TRAINING LOGIC GOES HERE
    # Example: nlp.update(...), nlp.resume_training(), etc.
    #
    print("--- Model training complete ---")
    return nlp

# --- Pipeline Execution ---

def main():
    """Runs the full replay buffer pipeline."""
    
    # 1. Load all necessary data
    print("Step 1: Loading data...")
    replay_buffer_data = load_data(REPLAY_BUFFER_FILE)
    new_data = load_data(NEW_TRAINING_DATA_FILE)
    
    if not new_data:
        print("No new training data found. Exiting.")
        return

    print(f"  - Loaded {len(replay_buffer_data)} examples from the replay buffer.")
    print(f"  - Loaded {len(new_data)} new training examples.")

    # 2. Create the training set by mixing old and new data
    print("\nStep 2: Creating mixed training set...")
    num_replay_samples = int(len(new_data) * REPLAY_SAMPLE_RATIO)
    
    if replay_buffer_data:
        replay_samples = random.sample(replay_buffer_data, min(num_replay_samples, len(replay_buffer_data)))
        print(f"  - Sampling {len(replay_samples)} examples from the buffer.")
    else:
        replay_samples = []
        print("  - Replay buffer is empty, training with new data only.")
        
    final_training_set = new_data + replay_samples
    random.shuffle(final_training_set)

    # 3. Load your model and train it
    # nlp = spacy.load(MODEL_PATH)
    # nlp_updated = train_model_with_replay(nlp, final_training_set)
    print("\nStep 3: (Simulating model training)...")
    print(f"  - The model would now be trained on {len(final_training_set)} examples.")

    # 4. Save the updated model
    # nlp_updated.to_disk(MODEL_PATH)
    print("\nStep 4: (Simulating saving model)...")
    print(f"  - Updated model would be saved to '{MODEL_PATH}'.")

    # 5. Update the replay buffer with some of the new data
    print("\nStep 5: Updating the replay buffer...")
    updated_buffer = update_replay_buffer(replay_buffer_data, new_data, BUFFER_CAPACITY)
    save_data(REPLAY_BUFFER_FILE, updated_buffer)
    print(f"  - Replay buffer now contains {len(updated_buffer)} examples.")
    print("\nPipeline finished successfully! ✨")


# Run the script
if __name__ == "__main__":
    main()