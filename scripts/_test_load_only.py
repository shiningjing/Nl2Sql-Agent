"""Minimal test: only load_bird_dev, no execute_sql."""
import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import warnings
warnings.filterwarnings("ignore")

print("Importing bird_loader...")
t0 = time.time()
from src.eval.bird_loader import get_database_url, load_bird_dev
print(f"  import done in {time.time()-t0:.1f}s")

print("Loading samples...")
t0 = time.time()
samples = load_bird_dev()
print(f"  loaded {len(samples)} samples in {time.time()-t0:.1f}s")

print("Getting #701...")
s701 = next(s for s in samples if s.question_id == "701")
url = get_database_url(s701)
print(f"  URL: {url}")
print("load_bird_dev phase: OK")
