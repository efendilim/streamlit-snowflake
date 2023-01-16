# Import required libraries
# Snowpark
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col, call_udf
# Pandas
import pandas as pd
#Streamlit
import streamlit as st

#Set page context
st.set_page_config(
     page_title="Thailand EPPO Consumption",
     page_icon="🧊",
     layout="wide",
     initial_sidebar_state="expanded",
     menu_items={
         'Get Help': 'https://developers.snowflake.com',
         'About': "This is an *extremely* cool app powered by Snowpark for Python, Streamlit, and Snowflake Marketplace"
     }
 )

# Create Session object
def create_session_object():
    session = Session.builder.configs(st.secrets.snowflake).create()
    return session
  
# Create Snowpark DataFrames that loads data from Thai EPPO
def load_data(session): 
    #Prepare data frame, set query parameters
    snow_df_pce = (session.sql("select YEAR as Year, VALUE as Consumption from PUBLIC.THAI_EPPO_CONSUMPTION order by Year desc"))
    pd_df_pce_year = snow_df_pce.to_pandas()
    pd_df_pce_year["Consumption"] = pd_df_pce_year["Consumption"].round(2)
    st.dataframe(pd_df_pce_year)

    
session = create_session_object()
load_data(session)
