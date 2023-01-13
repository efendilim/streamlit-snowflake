import streamlit as st
from snowflake.snowpark import Session

session = Session.builder.configs(st.secrets.snowflake).create()

df = session.table("employee")

st.dataframe(df)
