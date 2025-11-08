import streamlit as st

st.title("Capstone Project — Streamlit Launcher")
st.write("This repository contains a Streamlit app for log analysis.")

st.markdown(
    """
- To open the interactive log viewer, run:
```
streamlit run streamlit_app.py
```
- Or click the Run button in your editor that runs `streamlit_app.py`.
"""
)

st.info("If you want this page to directly host the app, rename `streamlit_app.py` to `app.py` or import its UI here.")
