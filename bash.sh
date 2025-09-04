# Create the main project directory
mkdir -p llm-spark-rag

# Navigate to the project directory
cd llm-spark-rag

# Create config directory and files
mkdir -p config
touch config/config.yaml
touch config/secrets.example.env

# Create data directory structure
mkdir -p data/raw data/bronze data/silver data/gold

# Create notebooks directory
mkdir -p notebooks
touch notebooks/exploration.ipynb

# Create src directory structure
mkdir -p src/common src/etl src/embed src/rag src/app

# Create common utilities
touch src/common/io_utils.py
touch src/common/text_utils.py
touch src/common/lang_utils.py

# Create ETL modules
touch src/etl/ingest_spark.py
touch src/etl/preprocess_spark.py

# Create embedding modules
touch src/embed/embeddings_udf.py
touch src/embed/build_index.py
touch src/embed/retriever.py

# Create RAG modules
touch src/rag/prompts.py
touch src/rag/pipeline.py

# Create application module
touch src/app/streamlit_app.py

# Create tests directory
mkdir -p tests
touch tests/test_chunking.py
touch tests/test_retriever.py

# Create project files
touch requirements.txt
touch Makefile
