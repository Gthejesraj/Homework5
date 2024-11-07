import pandas as pd
import json

# Sample data to simulate a TMDB-like dataset
sample_data = [
    {"doc_id": 1, "title": "Sample Title 1", "text": "Sample Text 1"},
    {"doc_id": 2, "title": "Sample Title 2", "text": "Sample Text 2"},
    {"doc_id": 3, "title": "Sample Title 3", "text": "Sample Text 3"},
]

# Convert the data into a JSONL file format
def create_jsonl(data, output_file):
    with open(output_file, 'w') as f:
        for entry in data:
            doc = {
                "put": f"id:search:doc::{entry['doc_id']}",
                "fields": {
                    "doc_id": entry['doc_id'],
                    "title": entry['title'],
                    "text": entry['text']
                }
            }
            f.write(json.dumps(doc) + '\n')

# Run the function to create 'clean_tmdb.jsonl'
create_jsonl(sample_data, 'clean_tmdb.jsonl')

