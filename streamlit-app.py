import streamlit as st
import pandas as pd
import requests
import os
import urllib.parse
import time
import io
from tqdm import tqdm

st.set_page_config(page_title="Property Address Scraper", layout="wide")

def format_addresses_from_dataframe(df):
    """
    Format addresses from a dataframe into a list of formatted strings
    Format: ADDRESS, CITY, STATE ZIP
    """
    formatted_addresses = []
    
    for _, row in df.iterrows():
        address = str(row.get('Address', '')).strip().upper()
        city = str(row.get('City', '')).strip().upper()
        state = str(row.get('State', '')).strip().upper()
        zip_code = str(row.get('Zip', '')).strip()
        
        # Format: ADDRESS, CITY, STATE ZIP (no comma between STATE and ZIP)
        formatted = f"{address}, {city}, {state} {zip_code}"
        formatted_addresses.append(formatted)
    
    return formatted_addresses

def save_formatted_addresses(addresses):
    """
    Save formatted addresses to a text file and return the content
    """
    content = "\n".join(addresses)
    return content

def submit_batch_with_webhook(api_key, addresses, webhook_url, batch_size=20):
    """
    Submit addresses with webhook for async processing using GET request
    Processes addresses in batches to avoid URL length limitations
    """
    results = []
    total_batches = (len(addresses) + batch_size - 1) // batch_size
    
    progress_text = f"Submitting {len(addresses)} addresses in {total_batches} batches"
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # Send a test POST to the webhook to make sure it's working
    try:
        requests.post(webhook_url, json={"test": "true", "message": "Starting processing"})
    except Exception as e:
        st.warning(f"Webhook test failed, but continuing: {e}")
    
    for i in range(0, len(addresses), batch_size):
        batch = addresses[i:i + batch_size]
        current_batch = i // batch_size + 1
        status_text.text(f"Processing batch {current_batch}/{total_batches}")
        
        # Build query URL
        base_url = "https://api.app.outscraper.com/whitepages-addresses"
        query_params = [f"webhook_url={urllib.parse.quote(webhook_url)}"]
        
        # Add each address as a separate query parameter
        for address in batch:
            query_params.append(f"query={urllib.parse.quote(address)}")
        
        url = f"{base_url}?{'&'.join(query_params)}"
        headers = {"X-API-KEY": api_key}
        
        # Send GET request
        try:
            response = requests.get(url, headers=headers)
            
            if response.status_code in [200, 202]:
                data = response.json()
                if response.status_code == 202:
                    request_id = data.get("id")
                    results.append({
                        "batch": current_batch,
                        "status": "submitted",
                        "request_id": request_id
                    })
                    
                    # Also send a direct POST to the webhook with the same data
                    try:
                        requests.post(webhook_url, json=data)
                    except:
                        pass
                else:
                    results.append({
                        "batch": current_batch,
                        "status": "completed",
                        "data": data
                    })
                    
                    # For immediate results, send them to webhook
                    try:
                        requests.post(webhook_url, json=data)
                    except:
                        pass
            else:
                st.error(f"Error with batch {current_batch}: {response.status_code} - {response.text}")
                results.append({
                    "batch": current_batch,
                    "status": "error",
                    "error": f"{response.status_code} - {response.text}"
                })
            
            # Avoid rate limiting
            if i + batch_size < len(addresses):
                time.sleep(1)
                
        except Exception as e:
            st.error(f"Error sending batch {current_batch}: {e}")
            results.append({
                "batch": current_batch,
                "status": "error",
                "error": str(e)
            })
        
        # Update progress bar
        progress_bar.progress((i + len(batch)) / len(addresses))
    
    progress_bar.progress(1.0)
    status_text.text("Processing complete!")
    
    # Summarize results
    submitted = sum(1 for r in results if r["status"] == "submitted")
    completed = sum(1 for r in results if r["status"] == "completed")
    errors = sum(1 for r in results if r["status"] == "error")
    
    summary = {
        "total_batches": total_batches,
        "submitted": submitted,
        "completed": completed,
        "errors": errors
    }
    
    if submitted > 0:
        # Send summary to the webhook directly for guaranteed delivery
        try:
            requests.post(webhook_url, json={
                "summary": True,
                **summary
            })
        except:
            pass
    
    return results, summary

def process_csv_file(uploaded_file):
    """
    Process uploaded CSV file and return formatted addresses
    """
    try:
        # Read the CSV file
        df = pd.read_csv(uploaded_file)
        st.success(f"Successfully loaded {len(df)} records")
        
        # Show preview of the data
        st.subheader("Data Preview")
        st.dataframe(df.head())
        
        # Format addresses
        formatted_addresses = format_addresses_from_dataframe(df)
        
        return formatted_addresses, df
    
    except Exception as e:
        st.error(f"Error processing CSV file: {e}")
        return None, None

# Main app
st.title("Property Address Scraper")
st.markdown("Upload a CSV file with address data to format and submit to Outscrapper API")

# Sidebar for configuration
st.sidebar.header("Configuration")
api_key = st.sidebar.text_input("Outscrapper API Key", value="YTFlOGFjYWUyMjExNDllZTk2Y2NkYjdmMTdlOTVhYzR8ZDQ3NTBjNGU5Mw", type="password")
webhook_url = st.sidebar.text_input("Webhook URL", value="https://n8n-6421995847235247.kloudbeansite.com/webhook/superskip")
batch_size = st.sidebar.number_input("Batch Size", min_value=1, max_value=100, value=20)

# File uploader
uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

if uploaded_file is not None:
    st.subheader("Processing CSV File")
    formatted_addresses, df = process_csv_file(uploaded_file)
    
    if formatted_addresses:
        st.subheader("Formatted Addresses")
        st.text_area("Sample Addresses (first 5)", "\n".join(formatted_addresses[:5]), height=150)
        
        # Create downloadable text file
        formatted_text = save_formatted_addresses(formatted_addresses)
        st.download_button(
            label="Download Formatted Addresses",
            data=formatted_text,
            file_name="formatted_addresses.txt",
            mime="text/plain"
        )
        
        # Action buttons
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Submit to Outscrapper API"):
                st.subheader("Submission Results")
                results, summary = submit_batch_with_webhook(api_key, formatted_addresses, webhook_url, batch_size)
                
                # Display summary
                st.subheader("Submission Summary")
                st.json(summary)
                
                # Display detailed results
                with st.expander("Detailed Results"):
                    st.json(results)
                
                st.success("Process completed! Results will be sent to your webhook.")
        
        with col2:
            if st.button("Clear Results"):
                st.experimental_rerun()
