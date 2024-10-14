import os
from google.cloud import storage
import pandas as pd
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
from sklearn.metrics import accuracy_score
import io  
import re

LABEL_MAPPING = {
    'LABEL_0': 'negative',
    'LABEL_1': 'neutral',
    'LABEL_2': 'positive'
}

def read_csv_from_gcs(bucket_name, file_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    csv_data = blob.download_as_text()
    df = pd.read_csv(io.StringIO(csv_data)) 
    return df

def preprocess(text):
    return text.lower().strip()

def main():
    bucket_name = 'my-sentiment-bucket-1234'  
    file_name = 'tweets-sentiment-synth.csv' 

    df = read_csv_from_gcs(bucket_name, file_name)
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
    print(f" Accuracy: {accuracy * 100:.2f}%")

if __name__ == "__main__":
    main()
