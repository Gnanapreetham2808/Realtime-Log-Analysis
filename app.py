import streamlit as st
import pandas as pd
import numpy as np
import joblib

# Title and intro
st.title("Capstone Project Demo 🚀")
st.write("A minimal Streamlit deployment of your Jupyter notebook.")

# Example: upload a CSV for model prediction
uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])

if uploaded_file:
    df = pd.read_csv(uploaded_file)
    st.write("### Uploaded Data", df.head())

    # Load your trained model (update this path if needed)
    model = joblib.load("model/model.pkl")

    # Predict (example — adapt to your notebook’s output)
    preds = model.predict(df)
    st.write("### Predictions", preds)
