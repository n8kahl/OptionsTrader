"""Options chain heatmap."""
import pandas as pd
import streamlit as st


def render_option_heatmap(chain: pd.DataFrame) -> None:
    st.subheader("Options Chain Heatmap")
    if chain.empty:
        st.info("No option data available")
        return
    st.dataframe(chain.style.background_gradient(subset=["iv", "pot"], cmap="viridis"))
