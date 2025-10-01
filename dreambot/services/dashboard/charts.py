"""Dashboard chart helpers."""
import pandas as pd
import streamlit as st


def render_underlying_panel(status: dict) -> None:
    st.subheader("Underlying VWAP & Bands")
    data = status.get("underlying", {
        "price": [450.0, 451.2, 452.1],
        "vwap": [449.8, 450.9, 451.7],
        "upper": [452.0, 453.1, 454.0],
        "lower": [447.5, 448.6, 449.8],
    })
    df = pd.DataFrame(data)
    st.line_chart(df[["price", "vwap"]])
    st.area_chart(df[["upper", "lower"]])
