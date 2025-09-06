import nltk
from nltk.tokenize import sent_tokenize


def setup_nltk():
    nltk.data.path.append("./nltk_data")
    print("📥 Downloading NLTK resources...")
    nltk.download("punkt", download_dir="./nltk_data")
    nltk.download("punkt_tab", download_dir="./nltk_data")
    print("✅ NLTK resources downloaded successfully.")

    print("🧪 Testing NLTK setup...")

    test_text = "Hello, world! This is a test sentence. NLTK is working."
    sentences = sent_tokenize(test_text, language="english")
    assert len(sentences) == 3, "NLTK setup test failed!"
    print("Input text:", test_text)
    print("Tokenized sentences:", sentences)

if __name__ == "__main__":
    setup_nltk()




