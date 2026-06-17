FROM python:3.10-slim

WORKDIR /app

# System deps for sentence-transformers + database drivers
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download embedding model (cached in image)
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('BAAI/bge-small-zh-v1.5')"

# Copy project (excluding items in .dockerignore)
COPY . .

# ChromaDB persistent storage + HF cache
VOLUME ["/app/.chroma"]
VOLUME ["/root/.cache/huggingface"]

EXPOSE 8501

ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0

CMD ["streamlit", "run", "app.py"]
