import pandas as pd
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
from sklearn.metrics import accuracy_score
import re

LABEL_MAPPING = {
    'LABEL_0': 'negative',
    'LABEL_1': 'neutral',
    'LABEL_2': 'positive'
}

def preprocess(text):
    return text.lower().strip()

def main():
    file_url = 'https://storage.googleapis.com/bdt-sentiment/tweets-sentiment-synth.csv'

    df = pd.read_csv(file_url)
    df['tweet'] = df['tweet'].apply(preprocess)

    tokenizer = AutoTokenizer.from_pretrained('cardiffnlp/twitter-roberta-base-sentiment')
    model = AutoModelForSequenceClassification.from_pretrained('cardiffnlp/twitter-roberta-base-sentiment')

    sentiment_pipeline = pipeline(
        'sentiment-analysis',
        model=model,
        tokenizer=tokenizer,
        device=-1  
    )

    def get_sentiment_label(text):
        result = sentiment_pipeline(text)[0]['label']
        return LABEL_MAPPING.get(result, 'unknown')

    df['predicted_sentiment'] = df['tweet'].apply(get_sentiment_label)
    df['sentiment'] = df['sentiment'].str.strip().str.lower()

    accuracy = accuracy_score(df['sentiment'], df['predicted_sentiment'])
    print(f"Accuracy: {accuracy * 100:.2f}%")

if __name__ == "__main__":
    main()
