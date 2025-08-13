import streamlit as st
import requests
import pandas as pd

API_BASE_URL = "http://localhost:8000"

st.title("Product Revenue & State Sales Reports")

with st.expander(" Initialize / Seed Database"):
    col1, col2 = st.columns(2)
    if col1.button("Create Tables"):
        r = requests.post(f"{API_BASE_URL}/init-db")
        if r.ok:
            st.success(r.json().get("message", "OK"))
        else:
            st.error(f"Error: {r.status_code}")

    num_products = col2.number_input("Products", min_value=1, value=10, step=1)
    num_invoices = col2.number_input(
        "Invoices", min_value=1, value=100, step=10)
    if col2.button("Seed Data (background)"):
        r = requests.post(f"{API_BASE_URL}/seed", params={
            "num_products": num_products,
            "num_invoices": num_invoices,
        })
        if r.ok:
            st.success(r.json().get("message", "Seeding started"))
        else:
            st.error(f"Error: {r.status_code}")

st.write("---")

report_type = st.radio(
    "Select Report", (
        "Product-wise Revenue", "State-wise Sales"
    )
)

if st.button("Load Report"):
    endpoint = "/reports/product-revenue" if report_type == "Product-wise Revenue" else "/reports/state-sales"
    r = requests.get(f"{API_BASE_URL}{endpoint}")
    if r.ok:
        data = r.json().get("data", [])
        if data:
            df = pd.DataFrame(data)
            st.dataframe(df)
        else:
            st.info("No data returned.")
    else:
        st.error(f"Error: {r.status_code}")
