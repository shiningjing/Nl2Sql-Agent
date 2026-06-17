"""Build BIRD-specific few-shot examples into ChromaDB nl2sql_fewshot collection.

Usage:
  python scripts/ingest_bird_fewshot.py                # ingest from corpus/bird_fewshot/*.md
  python scripts/ingest_bird_fewshot.py --reset        # clear and re-ingest
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from retrieval.fewshot_retrieve import load_fewshot_corpus, ingest_fewshot_chunks, get_fewshot_count


def load_bird_fewshot(corpus_dir: str) -> list[dict]:
    """Parse BIRD few-shot markdown files in corpus/bird_fewshot/."""
    chunks = []
    fs_dir = os.path.join(corpus_dir, "bird_fewshot")
    if not os.path.isdir(fs_dir):
        print(f"Directory not found: {fs_dir}")
        return []

    for fname in sorted(os.listdir(fs_dir)):
        if not fname.endswith(".md"):
            continue
        db_id = fname.replace(".md", "")
        fpath = os.path.join(fs_dir, fname)
        with open(fpath, "r", encoding="utf-8") as f:
            text = f.read()

        import re
        pairs = re.findall(r"###\s*Q\s*\n(.*?)\n###\s*SQL\s*\n(.*?)(?=\n###\s*Q|\Z)", text, re.DOTALL)
        for i, (q, sql) in enumerate(pairs):
            q = q.strip()
            sql = sql.strip()
            chunks.append({
                "chunk_id": f"bird_fs:{db_id}:{i}",
                "content": q,
                "metadata": {
                    "db_id": db_id,
                    "question": q,
                    "sql": sql,
                    "source_path": fname,
                },
            })
    return chunks


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Ingest BIRD few-shot examples")
    parser.add_argument("--reset", action="store_true", help="Clear and re-ingest")
    args = parser.parse_args()

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    corpus_dir = os.path.join(project_root, "corpus")

    chunks = load_bird_fewshot(corpus_dir)
    print(f"Loaded {len(chunks)} few-shot examples from corpus/bird_fewshot/")

    if chunks:
        ingest_fewshot_chunks(chunks, reset=args.reset)
        print(f"Ingested into ChromaDB. Collection now has {get_fewshot_count()} examples.")
    else:
        print("No examples found. Nothing ingested.")
