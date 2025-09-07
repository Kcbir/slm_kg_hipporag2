from datasets import load_dataset
import json
import os

output_path = "/Users/kabir/Desktop/Research/Implementation/qa_replay_buffer.jsonl"
os.makedirs(os.path.dirname(output_path), exist_ok=True)

DATASETS = {
    "popqa": ("akariasai/PopQA", "test", "question", "possible_answers"),
    "hotpotqa": ("hotpot_qa", "distractor", "question", "answer"),
}

N_SAMPLES = 10000
output = []

for name, (hf_id, config_or_split, q_key, a_key) in DATASETS.items():
    print(f"Loading {name}...")

    if name == "popqa":
        # PopQA only has 'test' split
        ds = load_dataset(hf_id, split="test")
    else:
        # For HotpotQA, we pass config and get the split we want ('train')
        ds = load_dataset(hf_id, config_or_split, split="train")

    subset = ds.shuffle(seed=42).select(range(min(N_SAMPLES, len(ds))))

    for item in subset:
        question = item.get(q_key, "")
        answer = item.get(a_key, "")

        if isinstance(answer, list):
            answer = answer[0] if answer else ""
        elif isinstance(answer, dict):
            answer = answer.get("text", "") if "text" in answer else ""

        output.append({"dataset": name, "question": question, "answer": answer})

print(f"Collected {len(output)} QA pairs.")

with open(output_path, "w") as f:
    for entry in output:
        f.write(json.dumps(entry) + "\n")

print(f"Replay buffer saved at {output_path}")