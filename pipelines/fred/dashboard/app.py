import streamlit as st
import duckdb

st.title("US Macro Indicators")

con = duckdb.connect("/Users/operry/Projects/fred-pipeline/data/fred.duckdb", read_only=True)
df = con.execute("select * from fct_macro_daily order by observation_date").df()

metric = st.selectbox("Metric", ["unemployment_rate", "cpi", "fed_funds_rate", "gdp"])
st.line_chart(df.set_index("observation_date")[metric].dropna())
st.subheader("Most recent 20 rows")
st.dataframe(df.tail(20))