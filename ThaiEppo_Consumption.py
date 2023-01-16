# Import required libraries
# Snowpark
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col, call_udf
# Pandas
import pandas as pd
#Streamlit
import streamlit as st
#JSON
import json
# Authenticator
import pickle
from pathlib import Path
import streamlit_authenticator as stauth

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
    #'LPG, Propane and Butane' consumption per year
    #Prepare data frame, set query parameters
    snow_df_pce = (session.table("PUBLIC.THAI_EPPO_CONSUMPTION"))
    #Select columns, substract 100 from value column to reference baseline
    snow_df_pce_year = snow_df_pce.select(col('"YEAR"').alias('"Year"'), col('"VALUE"').alias('"Consumption"')).sort('"Year"', ascending=False)
    #convert to pandas dataframe 
    pd_df_pce_year = snow_df_pce_year.to_pandas()
    #round the Consumption series
    pd_df_pce_year["Consumption"] = pd_df_pce_year["Consumption"].round(2)
  
    #create metrics
    latest_pce_year = pd_df_pce_year.loc[0]["Year"].astype('int')
    latest_pce_value = pd_df_pce_year.loc[0]["Consumption"]
    delta_pce_value = latest_pce_value - pd_df_pce_year.loc[1]["Consumption"]

    #Use Snowflake UDF for Model Inference
    snow_df_predict_years = session.create_dataframe([[int(latest_pce_year+1)], [int(latest_pce_year+2)],[int(latest_pce_year+3)]], schema=["Year"])
    pd_df_pce_predictions = snow_df_predict_years.select(col("year"), call_udf("predict_consumption_udf", col("year")).as_("consumption")).sort(col("year")).to_pandas()
    pd_df_pce_predictions.rename(columns={"YEAR": "Year"}, inplace=True)
    #round the Consumption prediction series
    pd_df_pce_predictions["CONSUMPTION"] = pd_df_pce_predictions["CONSUMPTION"].round(2).astype(float)
    
    #Combine actual and predictions dataframes
    pd_df_pce_all = pd.concat([
        pd_df_pce_year.set_index('Year').sort_index().rename(columns={"Consumption": "Actual"}), 
        pd_df_pce_predictions.set_index('Year').sort_index().rename(columns={"CONSUMPTION": "Prediction"})
    ])
   
    
    # Add header and a subheader
    st.title("Thailand EPPO Data")
    # st.header("Powered by Snowpark for Python and Snowflake Marketplace | Made with Streamlit")
    st.subheader("Consumption - LPG, Propane and Butane (in kt)")

    with st.expander("Upload your own prediction here!"):
        uploaded_file = st.file_uploader('Upload a CSV=  file')
        if uploaded_file is not None:
            myforecast_df = pd.read_csv(uploaded_file)
            pd_df_pce_predictions = myforecast_df.rename(columns={myforecast_df.columns[0]: "Year", myforecast_df.columns[1]: "CONSUMPTION"})
            pd_df_pce_predictions["CONSUMPTION"] = pd_df_pce_predictions["CONSUMPTION"].round(2).astype(float)
            #Update the combined dataframes
            pd_df_pce_all = pd.concat([
                pd_df_pce_year.set_index('Year').sort_index().rename(columns={"Consumption": "Actual"}), 
                pd_df_pce_predictions.set_index('Year').sort_index().rename(columns={"CONSUMPTION": "Prediction"})
            ])
    
    # Use columns to display metrics for global value and predictions
    col11, col12, col13 = st.columns(3)
    with st.container():
        with col11:
            st.metric("Consumption in " + str(latest_pce_year), round(latest_pce_value), round(delta_pce_value), delta_color=("inverse"))
        with col12:
            st.metric("Predicted Consumption for " + str(int(pd_df_pce_predictions.loc[0]["Year"])), round(pd_df_pce_predictions.loc[0]["CONSUMPTION"]), 
                round((pd_df_pce_predictions.loc[0]["CONSUMPTION"] - latest_pce_value)), delta_color=("inverse"))
        with col13:
            st.metric("Predicted Consumption for " + str(int(pd_df_pce_predictions.loc[1]["Year"])), round(pd_df_pce_predictions.loc[1]["CONSUMPTION"]), 
                round((pd_df_pce_predictions.loc[1]["CONSUMPTION"] - latest_pce_value)), delta_color=("inverse"))

    # Barchart with actual and predicted Consumption
    st.bar_chart(data=pd_df_pce_all.tail(25), width=0, height=0, use_container_width=True)

    
# --- USER AUTHENTICATION ---
names = ["Efendi Lim", "Simin Liew"]
usernames = ["elim", "syliew"]

# load hashed passwords
file_path = Path(__file__).parent / "hashed_pw.pkl"
with file_path.open("rb") as file:
    hashed_passwords = pickle.load(file)

authenticator = stauth.Authenticate(names, usernames, hashed_passwords,
    "ts_demo", "ampolts", cookie_expiry_days=30)

name, authentication_status, username = authenticator.login("Login", "main")

if authentication_status == False:
    st.error("Username/password is incorrect")

if authentication_status == None:
    st.warning("Please enter your username and password")

if authentication_status:
    # ---- SIDEBAR ----
    st.sidebar.title(f"Welcome {name}")
    authenticator.logout("Logout", "sidebar")
    session = create_session_object()
    load_data(session)
