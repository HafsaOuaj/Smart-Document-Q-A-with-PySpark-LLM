# Smart-Document-Q-A-with-PySpark-LLM

               ┌──────────────────────────────────────────────────┐
               │                 Data Sources                      │
               │  (e.g., Wikipedia dump, Europarl, news, PDFs)     │
               └───────────────┬───────────────────────────────────┘
                               │
                        (PySpark Ingest)
                               │
             ┌─────────────────▼──────────────────┐
             │         Spark ETL/Preprocess       │
             │  - Clean/normalize text            │
             │  - Language detect (EN/ES)         │
             │  - Chunking + metadata             │
             │  - Output: Parquet                 │
             └───────────────┬────────────────────┘
                             │
               (Spark UDF for Embeddings)
                             │
             ┌───────────────▼────────────────────┐
             │     Spark Batch Embedding Job      │
             │  - Text → Embeddings (UDF)         │
             │  - Persist: Parquet + NPZ          │
             │  - Build FAISS / Pinecone index    │
             └───────────────┬────────────────────┘
                             │
                        (Vector Store)
                             │
       ┌─────────────────────▼──────────────────────┐
       │            Retrieval Layer                 │
       │  - Top-k semantic search (FAISS/Pinecone)  │
       │  - Hybrid option: BM25 + Embeddings        │
       └─────────────────────┬──────────────────────┘
                             │
                         (RAG Core)
                             │
       ┌─────────────────────▼──────────────────────┐
       │                  LLM                       │
       │  - Prompt templating (EN/ES)               │
       │  - Context stuffing / citations            │
       └─────────────────────┬──────────────────────┘
                             │
                      (Streamlit UI)
                             │
       ┌─────────────────────▼──────────────────────┐
       │ Multilingual QA App                        │
       │  - Question input (EN/ES)                  │
       │  - Answer + retrieved chunks               │
       │  - Upload-your-PDF (stretch)               │
       └────────────────────────────────────────────┘
