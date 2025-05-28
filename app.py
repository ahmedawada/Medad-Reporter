#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import requests
import json
import numpy as np
import streamlit as st
import time
import re
import ast
import io
import base64
import datetime

# Set page title and configuration
st.set_page_config(
    page_title="Medad Reporter",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Create app header
st.title("📚 Medad Reporter")
st.markdown("Seamlessly integrate with Medad to harvest rich bibliographic insights and craft bespoke analytical reports")

# Function to login to tenant
def tenant_login(okapi, tenant, username, password):
    myobj = {"username": username, "password": password}
    data = json.dumps(myobj)
    header = {"x-okapi-tenant": tenant}
    try:
        x = requests.post(okapi + "/authn/login", data=data, headers=header)
        if "x-okapi-token" in x.headers:
            token = x.headers["x-okapi-token"]
            return token, True, "Connected successfully!"
        else:
            return None, False, "Authentication failed. Please check your credentials."
    except Exception as e:
        return None, False, f"Connection error: {str(e)}"

# Function to get instances data
def get_instances(url, header_dict):
    limit = "?limit=2000000"
    with st.spinner('Fetching instances data...'):
        response_instances = requests.get(url+"/instance-storage/instances"+limit, headers=header_dict).json()
        df_instances = pd.json_normalize(response_instances, record_path='instances')
    return df_instances

# Function to get holdings data
def get_holdings(url, header_dict):
    limit = "?limit=2000000"
    with st.spinner('Fetching holdings data...'):
        response_instances = requests.get(url+"/holdings-storage/holdings"+limit, headers=header_dict).json()
        df_holdings = pd.json_normalize(response_instances, record_path='holdingsRecords')
    return df_holdings

# Function to get items data
def get_items(url, header_dict):
    limit = "?limit=2000000"
    with st.spinner('Fetching items data...'):
        response_instances = requests.get(url+"/item-storage/items"+limit, headers=header_dict).json()
        df_items = pd.json_normalize(response_instances, record_path='items')
    return df_items

# Function to get locations
def get_locations(url, header_dict):
    limit = "?limit=2000000"
    with st.spinner('Fetching locations data...'):
        response_instances = requests.get(url+"/locations"+limit, headers=header_dict).json()
        df_location = pd.json_normalize(response_instances, record_path='locations')
    return df_location

# Function to get material types
def get_mtypes(url, header_dict):
    limit = "?limit=2000000"
    with st.spinner('Fetching material types...'):
        response_instances = requests.get(url+"/material-types"+limit, headers=header_dict).json()
        df_mtypes = pd.json_normalize(response_instances, record_path='mtypes')
    return df_mtypes

# Function to get statistical codes
def get_statistical_codes(url, header_dict):
    limit = "?limit=2000"
    with st.spinner('Fetching statistical codes...'):
        response_instances = requests.get(url+"/statistical-codes"+limit, headers=header_dict).json()
        df_statcode = pd.json_normalize(response_instances, record_path='statisticalCodes')
    return df_statcode

# Function to get loan types
def get_loan_types(url, header_dict):
    limit = "?limit=2000000"
    with st.spinner('Fetching loan types...'):
        response_instances = requests.get(url+"/loan-types"+limit, headers=header_dict).json()
        df_loantypes = pd.json_normalize(response_instances, record_path='loantypes')
    return df_loantypes

# Function to get user information by UUID
def get_user_by_id(url, header_dict, user_id):
    """
    Fetch user information for a given user ID from the users endpoint.
    Returns the username or the original ID if user not found.
    """
    if not user_id or user_id == '':
        return "Unknown"
    
    # Cache for users to avoid repeated API calls for the same user
    if 'user_cache' not in st.session_state:
        st.session_state.user_cache = {}
    
    # Return from cache if available
    if user_id in st.session_state.user_cache:
        return st.session_state.user_cache[user_id]
    
    try:
        response = requests.get(f"{url}/users/{user_id}", headers=header_dict)
        if response.status_code == 200:
            user_data = response.json()
            username = user_data.get('username', '')
            # If username is empty, try to get name from personal data
            if not username:
                personal = user_data.get('personal', {})
                last_name = personal.get('lastName', '')
                first_name = personal.get('firstName', '')
                if last_name or first_name:
                    username = f"{first_name} {last_name}".strip()
                else:
                    username = user_id  # Fall back to ID if no name found
            
            # Store in cache
            st.session_state.user_cache[user_id] = username
            return username
        else:
            # User not found, store ID in cache to avoid repeated failed lookups
            st.session_state.user_cache[user_id] = f"User {user_id[:8]}..."
            return f"User {user_id[:8]}..."
    except Exception as e:
        # Error during API call, store error in cache
        error_msg = f"Error: {str(e)[:20]}..."
        st.session_state.user_cache[user_id] = error_msg
        return error_msg

# Function to process user IDs in batch
def process_user_ids(df, url, header_dict):
    """
    Process all user IDs in the dataframe and add username columns.
    """
    with st.spinner('Fetching user information...'):
        # Create new columns for usernames
        df['instance_creator_name'] = df['metadata.createdByUserId_x'].apply(
            lambda x: get_user_by_id(url, header_dict, x) if pd.notna(x) else "Unknown"
        )
        df['instance_updater_name'] = df['metadata.updatedByUserId_x'].apply(
            lambda x: get_user_by_id(url, header_dict, x) if pd.notna(x) else "Unknown"
        )
        df['holding_creator_name'] = df['metadata.createdByUserId_y'].apply(
            lambda x: get_user_by_id(url, header_dict, x) if pd.notna(x) else "Unknown"
        )
        df['holding_updater_name'] = df['metadata.updatedByUserId_y'].apply(
            lambda x: get_user_by_id(url, header_dict, x) if pd.notna(x) else "Unknown"
        )
        df['item_creator_name'] = df['metadata.createdByUserId'].apply(
            lambda x: get_user_by_id(url, header_dict, x) if pd.notna(x) else "Unknown"
        )
        df['item_updater_name'] = df['metadata.updatedByUserId'].apply(
            lambda x: get_user_by_id(url, header_dict, x) if pd.notna(x) else "Unknown"
        )
    return df

# Function to get patron groups
def get_patron_groups(url, header_dict):
    """
    Fetch patron groups from the FOLIO API
    Returns a dictionary mapping patron group IDs to their names
    """
    try:
        # Construct the URL for patron groups endpoint
        groups_url = f"{url}/groups?limit=1000"
        
        # Make the API request
        response = requests.get(groups_url, headers=header_dict)
        response.raise_for_status()
        
        # Parse the response
        data = response.json()
        
        # Create a dictionary mapping group IDs to names
        group_dict = {}
        if 'usergroups' in data:
            for group in data['usergroups']:
                if 'id' in group and 'group' in group:
                    group_dict[group['id']] = group['group']
        
        return group_dict
    except Exception as e:
        st.error(f"Error fetching patron groups: {str(e)}")
        return {}

# Helper functions for data processing
def extract_and_concatenate_notes(notes_list):
    """
    Extract the 'note' field from each dictionary in the notes_list and concatenate them with a pipe ('|').
    If the list is empty or no 'note' fields are found, return NaN.
    """
    if not isinstance(notes_list, list) or not notes_list:
        # Return NaN for empty or non-list entries
        return np.nan
    # Extract 'note' from each dictionary, handling missing 'note' keys
    notes = [d.get('note', '').strip() for d in notes_list if 'note' in d and d.get('note')]
    # Remove any empty strings resulting from missing 'note' keys
    notes = [note for note in notes if note]
    if not notes:
        return np.nan
    # Join the notes with a pipe separator
    concatenated_notes = '|'.join(notes)
    return concatenated_notes

def safe_parse(x):
    """
    Safely parse a string representation of a list of dictionaries into an actual list.
    If parsing fails, return an empty list.
    """
    if isinstance(x, str):
        try:
            # Try to parse using ast.literal_eval
            parsed = ast.literal_eval(x)
            if isinstance(parsed, list):
                return parsed
        except (SyntaxError, ValueError):
            pass
    elif isinstance(x, list):
        return x
    return []

def parse_publication_info_adaptive(publication_data):
    """
    Parse publication information from various formats and extract publisher, place, and date.
    Handles both list and string representations.
    """
    publisher = ''
    place = ''
    date = ''
    
    # Convert to list format if it's a string
    publications = safe_parse(publication_data)
    
    if publications:
        for pub in publications:
            # Extract publisher
            if 'publisher' in pub and pub['publisher']:
                publisher = pub['publisher']
                
            # Extract place
            if 'place' in pub and pub['place']:
                place = pub['place']
                
            # Extract date of publication
            if 'dateOfPublication' in pub and pub['dateOfPublication']:
                date = pub['dateOfPublication']
                
            # If we have all three pieces of information, we can stop
            if publisher and place and date:
                break
    
    return pd.Series([publisher, place, date])

def extract_alternative_title(alt_titles):
    """Extract the first alternative title from a list of alternative titles."""
    alt_titles_list = safe_parse(alt_titles)
    if alt_titles_list and len(alt_titles_list) > 0 and 'alternativeTitle' in alt_titles_list[0]:
        return alt_titles_list[0]['alternativeTitle']
    return np.nan

def extract_vtls020(id_list):
    """Extract ISBN from identifiers list."""
    id_list = safe_parse(id_list)
    for identifier in id_list:
        if isinstance(identifier, dict) and identifier.get('identifierTypeId') == "8261054f-be78-422d-bd51-4ed9f33c3422":
            return identifier.get('value', '')
    return ''

# Function to get loan data
def get_loans(url, header_dict, query_param=""):
    offset = 0
    limit = 1000  # Adjust based on what the server can handle efficiently
    all_loans = []  # List to hold all records

    with st.spinner('Fetching loan data...'):
        while True:
            # Modify the request URL to include offset and limit for pagination
            paginated_url = f"{url}/circulation/loans?limit={limit}&offset={offset}{query_param}"
            response = requests.get(paginated_url, headers=header_dict)
            try:
                response.raise_for_status()  # Check for HTTP errors
                data = response.json()
                loans = data.get('loans', [])
                if not loans:
                    break  # Exit loop if no more data is returned
                all_loans.extend(loans)
                offset += limit  # Increase offset for next batch
            except requests.exceptions.HTTPError as err:
                st.error(f"HTTP error occurred: {err}")
                break
            except requests.exceptions.RequestException as e:
                st.error(f"Error making request: {e}")
                break
            except ValueError as e:
                st.error(f"Error decoding JSON: {e}")
                break

    # Once all data is fetched, convert it to a DataFrame
    if all_loans:
        df_loans = pd.json_normalize(all_loans)
        return df_loans
    else:
        st.warning("No loans data found.")
        return pd.DataFrame()

# Function to get user data
def get_users(url, header_dict):
    offset = 0
    limit = 1000  # Adjust based on what the server can handle efficiently
    all_users = []  # List to hold all records

    with st.spinner('Fetching user data...'):
        while True:
            # Modify the request URL to include offset and limit for pagination
            paginated_url = f"{url}/users?limit={limit}&offset={offset}"
            response = requests.get(paginated_url, headers=header_dict)
            try:
                response.raise_for_status()  # Check for HTTP errors
                data = response.json()
                users = data.get('users', [])
                if not users:
                    break  # Exit loop if no more data is returned
                all_users.extend(users)
                offset += limit  # Increase offset for next batch
            except requests.exceptions.HTTPError as err:
                st.error(f"HTTP error occurred: {err}")
                break
            except requests.exceptions.RequestException as e:
                st.error(f"Error making request: {e}")
                break
            except ValueError as e:
                st.error(f"Error decoding JSON: {e}")
                break

    # Once all data is fetched, convert it to a DataFrame
    if all_users:
        df_users = pd.json_normalize(all_users)
        return df_users
    else:
        st.warning("No users data found.")
        return pd.DataFrame()

# Function to get fines data
def get_fines(url, header_dict):
    offset = 0
    limit = 1000  # Adjust based on what the server can handle efficiently
    all_fines = []  # List to hold all records

    with st.spinner('Fetching fines data...'):
        while True:
            # Modify the request URL to include offset and limit for pagination
            paginated_url = f"{url}/accounts?limit={limit}&offset={offset}"
            response = requests.get(paginated_url, headers=header_dict)
            try:
                response.raise_for_status()  # Check for HTTP errors
                data = response.json()
                fines = data.get('accounts', [])
                if not fines:
                    break  # Exit loop if no more data is returned
                all_fines.extend(fines)
                offset += limit  # Increase offset for next batch
            except requests.exceptions.HTTPError as err:
                st.error(f"HTTP error occurred: {err}")
                break
            except requests.exceptions.RequestException as e:
                st.error(f"Error making request: {e}")
                break
            except ValueError as e:
                st.error(f"Error decoding JSON: {e}")
                break

    # Once all data is fetched, convert it to a DataFrame
    if all_fines:
        df_fines = pd.json_normalize(all_fines)
        return df_fines
    else:
        st.warning("No fines data found.")
        return pd.DataFrame()

# Function to get loan count data
def get_loan_count_data(url, header_dict):
    offset = 0
    limit = 1000  # Adjust based on what the server can handle efficiently
    all_loan_counts = []  # List to hold all records

    with st.spinner('Fetching loan count data...'):
        while True:
            # Modify the request URL to include offset and limit for pagination
            paginated_url = f"{url}/circulation/loans?limit={limit}&offset={offset}"
            response = requests.get(paginated_url, headers=header_dict)
            try:
                response.raise_for_status()  # Check for HTTP errors
                data = response.json()
                loan_counts = data.get('loans', [])
                if not loan_counts:
                    break  # Exit loop if no more data is returned
                all_loan_counts.extend(loan_counts)
                offset += limit  # Increase offset for next batch
            except requests.exceptions.HTTPError as err:
                st.error(f"HTTP error occurred: {err}")
                break
            except requests.exceptions.RequestException as e:
                st.error(f"Error making request: {e}")
                break
            except ValueError as e:
                st.error(f"Error decoding JSON: {e}")
                break

    # Once all data is fetched, convert it to a DataFrame
    if all_loan_counts:
        df_loan_counts = pd.json_normalize(all_loan_counts)
        return df_loan_counts
    else:
        st.warning("No loan count data found.")
        return pd.DataFrame()

# Create sidebar for login form
st.sidebar.title("Medad Login")

# Input fields for Medad credentials
okapi_url = st.sidebar.text_input("Okapi URL", placeholder="https://okapi.example.com", key="okapi_url_input")
tenant = st.sidebar.text_input("Tenant", placeholder="tenant_name", key="tenant_input") 
username = st.sidebar.text_input("Username", placeholder="your_username", key="username_input")
password = st.sidebar.text_input("Password", type="password", key="password_input")

# Login button
if st.sidebar.button("Connect to Medad", key="login_button"):
    if not (okapi_url and tenant and username and password):
        st.sidebar.error("Please fill in all credentials")
    else:
        with st.spinner("Connecting to Medad..."):
            token, success, message = tenant_login(okapi_url, tenant, username, password)
            if success:
                st.sidebar.success(message)
                st.session_state.token = token
                st.session_state.okapi_url = okapi_url
                st.session_state.tenant = tenant
                st.session_state.logged_in = True
            else:
                st.sidebar.error(message)
                st.session_state.logged_in = False

# Initialize session state variables if they don't exist
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'final_df' not in st.session_state:
    st.session_state.final_df = None
if 'loan_count_data_loaded' not in st.session_state:
    st.session_state.loan_count_data_loaded = False
if 'loan_count_df' not in st.session_state:
    st.session_state.loan_count_df = None
if 'circulation_data_loaded' not in st.session_state:
    st.session_state.circulation_data_loaded = False

# Add a Reset All Data button to the sidebar if user is logged in
if st.session_state.logged_in:
    st.sidebar.markdown("---")
    
    # Button to reset all data
    if st.sidebar.button("Reset All Data", key="reset_all_button"):
        # Reset all data-related session state variables
        st.session_state.data_loaded = False
        st.session_state.circulation_data_loaded = False
        st.session_state.loan_count_data_loaded = False
        st.session_state.final_df = None
        st.session_state.circulation_df = None
        st.session_state.loan_count_df = None
        st.session_state.fines_df = None
        st.session_state.patron_groups = None
        if 'user_cache' in st.session_state:
            st.session_state.user_cache = {}
        st.sidebar.success("All data has been reset!")
        st.experimental_rerun()

# Main content area - only show if logged in
if st.session_state.logged_in:
    # Create tabs for different reports
    tabs = st.tabs(["Bibliographic Report", "Circulation Report", "Loan Count"])
    
    with tabs[0]:  # Bibliographic Report Tab
        if not st.session_state.data_loaded:
            if st.button("Load Bibliographic Data", key="bibliographic_load_button"):
                try:
                    # Set up header for API calls
                    header_dict = {
                        "x-okapi-tenant": st.session_state.tenant,
                        "x-okapi-token": st.session_state.token
                    }
                    
                    # Fetch all the required data
                    with st.spinner("Loading data from Medad..."):
                        # Get instances, holdings, and items data
                        df_instances = get_instances(st.session_state.okapi_url, header_dict)
                        st.success("✅ Instances data loaded")
                        
                        df_holdings = get_holdings(st.session_state.okapi_url, header_dict)
                        st.success("✅ Holdings data loaded")
                        
                        df_items = get_items(st.session_state.okapi_url, header_dict)
                        st.success("✅ Items data loaded")
                        
                        # Get reference data
                        df_location = get_locations(st.session_state.okapi_url, header_dict)
                        df_mtypes = get_mtypes(st.session_state.okapi_url, header_dict)
                        df_loantypes = get_loan_types(st.session_state.okapi_url, header_dict)
                        df_statcode = get_statistical_codes(st.session_state.okapi_url, header_dict)
                        
                        # Merge the data
                        with st.spinner("Merging data..."):
                            # First merge instances with holdings
                            merged_df = df_instances.merge(df_holdings, left_on='id', right_on='instanceId', how='inner')
                            
                            # Then merge with items
                            final_df = merged_df.merge(df_items, left_on='id_y', right_on='holdingsRecordId', how='inner')
                            
                            # Process the contributors field
                            final_df['contributors'] = final_df['contributors'].apply(lambda x: x[0]['name'] if isinstance(x, list) and x else '')
                            
                            # Process publication data
                            final_df['publisher'] = ''
                            final_df['place'] = ''
                            final_df['dateOfPublication'] = ''
                            
                            for idx, row in final_df.iterrows():
                                pub_info = parse_publication_info_adaptive(row['publication'])
                                final_df.at[idx, 'publisher'] = pub_info[0]
                                final_df.at[idx, 'place'] = pub_info[1]
                                final_df.at[idx, 'dateOfPublication'] = pub_info[2]
                            
                            # Extract alternative title
                            final_df["alternativeTitleExtracted"] = final_df["alternativeTitles"].apply(extract_alternative_title)
                            
                            # Extract ISBN
                            final_df["ISBN"] = final_df["identifiers"].apply(extract_vtls020)
                            
                            # Extract and process notes
                            final_df["itemNotes"] = final_df["notes"].apply(extract_and_concatenate_notes)
                            
                            location_name = df_location.set_index('id')['name']


                            # In[31]:


                            final_df['holding_location_name'] = final_df['permanentLocationId_x'].map(location_name)


                            # In[32]:


                            final_df['item_location_name'] = final_df['effectiveLocationId_y'].map(location_name)


                            # In[33]:


                            material_types = df_mtypes.set_index('id')['name']


                            # In[34]:


                            final_df['Material_name'] = final_df['materialTypeId'].map(material_types)


                            # In[35]:


                            statistical_types = df_statcode.set_index('id')['name']


                            # In[36]:


                            final_df['statisticalCodeIds'] = final_df['statisticalCodeIds'].apply(lambda x: ','.join(map(str, x)))


                            # In[37]:


                            final_df['Statistical_code'] = final_df['statisticalCodeIds'].map(statistical_types)

                            
                            # Process user IDs to get usernames
                            final_df = process_user_ids(final_df, st.session_state.okapi_url, header_dict)
                            
                            # Rename columns to user-friendly names
                            column_renames = {
                                'title': 'Title',
                                'contributors': 'Author',
                                'publisher': 'Publisher',
                                'place': 'Place of Publication',
                                'dateOfPublication': 'Publication Date',
                                'ISBN': 'ISBN',
                                'callNumber': 'Call Number',
                                'barcode': 'Barcode',
                                'status.name': 'Item Status',
                                'locationName': 'Location',
                                'materialTypeName': 'Material Type',
                                'loanTypeName': 'Loan Type',
                                'itemNotes': 'Notes',
                                'alternativeTitleExtracted': 'Alternative Title',
                                'instance_creator_name': 'Instance Creator',
                                'instance_updater_name': 'Instance Updater',
                                'holding_creator_name': 'Holding Creator',
                                'holding_updater_name': 'Holding Updater',
                                'item_creator_name': 'Item Creator',
                                'item_updater_name': 'Item Updater'
                            }
                            
                            # Apply column renames where the columns exist
                            for old_name, new_name in column_renames.items():
                                if old_name in final_df.columns:
                                    final_df.rename(columns={old_name: new_name}, inplace=True)
                            
                            # Store the final dataframe in session state
                            st.session_state.final_df = final_df
                            st.session_state.data_loaded = True
                            
                            # Select columns to display by default
                            display_columns = [col for col in column_renames.values() if col in final_df.columns]
                            st.session_state.display_columns = display_columns
                            st.success("Data successfully loaded and processed!")
                            st.experimental_rerun()
                
                except Exception as e:
                    st.error(f"Error loading data: {str(e)}")
        else:
            # Data is loaded, display the DataFrame with filter controls
            st.subheader("Bibliographic Data")
            
            # Get the DataFrame from session state
            df = st.session_state.final_df
            
            # Select columns to display
            all_columns = df.columns.tolist()
            
            with st.expander("Select columns to display", expanded=False):
                selected_columns = st.multiselect(
                    "Choose columns",
                    options=all_columns,
                    default=st.session_state.display_columns
                )
            
            # Filter controls
            st.subheader("Bibliographic Report Filters")
            
            # Create a filtered copy of the DataFrame to preserve the original
            filtered_df = df.copy()
            
            # Location and Material Type Filters
            with st.expander("Location & Material Filters", expanded=True):
                st.markdown("### Location & Material Details")
                loc_col1, loc_col2 = st.columns(2)
                
                with loc_col1:
                    # Holding location filter
                    if 'holding_location_name' in filtered_df.columns:
                        location_col = 'holding_location_name'
                        holding_locations = sorted(filtered_df[location_col].dropna().unique().tolist())
                        selected_holding_location = st.selectbox(
                            "Holding Location",
                            options=["All"] + holding_locations,
                            key="filter_holding_location"
                        )
                        if selected_holding_location != "All":
                            filtered_df = filtered_df[filtered_df[location_col] == selected_holding_location]
                    
                    # Item location filter (if different from holding location)
                    if 'item_location_name' in filtered_df.columns:
                        item_location_col = 'item_location_name'
                        item_locations = sorted(filtered_df[item_location_col].dropna().unique().tolist())
                        selected_item_location = st.selectbox(
                            "Item Location",
                            options=["All"] + item_locations,
                            key="filter_item_location"
                        )
                        if selected_item_location != "All":
                            filtered_df = filtered_df[filtered_df[item_location_col] == selected_item_location]
                
                with loc_col2:
                    # Material name filter
                    if 'Material_name' in filtered_df.columns:
                        material_types = sorted(filtered_df['Material_name'].dropna().unique().tolist())
                        selected_material = st.selectbox(
                            "Material Type",
                            options=["All"] + material_types,
                            key="filter_material_type"
                        )
                        if selected_material != "All":
                            filtered_df = filtered_df[filtered_df['Material_name'] == selected_material]
                    
                    # Item status filter
                    if 'Item Status' in filtered_df.columns:
                        item_statuses = sorted(filtered_df['Item Status'].dropna().unique().tolist())
                        selected_status = st.selectbox(
                            "Item Status",
                            options=["All"] + item_statuses,
                            key="filter_item_status"
                        )
                        if selected_status != "All":
                            filtered_df = filtered_df[filtered_df['status.name'] == selected_status]
            
            # Statistical Codes and Discovery Settings
            with st.expander("Statistical Codes & Discovery Settings", expanded=True):
                st.markdown("### Codes & Discovery")
                code_col1, code_col2 = st.columns(2)
                
                with code_col1:
                    # Statistical code filter
                    if 'Statistical_code' in filtered_df.columns:
                        # Get unique values for the statistical code
                        stat_codes = sorted(filtered_df['Statistical_code'].dropna().unique().tolist())
                        
                        # Add options for All and No Value
                        filter_options = ["All", "No Statistical Code"] + stat_codes
                        
                        selected_stat_code = st.selectbox(
                            "Statistical Code",
                            options=filter_options,
                            key="filter_stat_code"
                        )
                        
                        if selected_stat_code == "No Statistical Code":
                            # Filter for empty or null values
                            filtered_df = filtered_df[filtered_df['Statistical_code'].isna() | 
                                                   (filtered_df['Statistical_code'] == '') | 
                                                   (filtered_df['Statistical_code'].astype(str) == 'nan')]
                        elif selected_stat_code != "All":
                            # Simple direct filter
                            filtered_df = filtered_df[filtered_df['Statistical_code'] == selected_stat_code]
                
                with code_col2:
                    # Discovery suppress filters
                    st.markdown("#### Discovery Settings")
                    suppress_col1, suppress_col2 = st.columns(2)
                    
                    with suppress_col1:
                        # Discovery suppress from instance filter
                        if 'discoverySuppress_x' in filtered_df.columns:
                            discovery_suppress_instance = st.checkbox(
                                "Suppress - Instance",
                                key="filter_discovery_suppress_instance"
                            )
                            if discovery_suppress_instance:
                                filtered_df = filtered_df[filtered_df['discoverySuppress_x'] == True]
                        
                        # Discovery suppress from holding filter
                        if 'discoverySuppress_y' in filtered_df.columns:
                            discovery_suppress_holding = st.checkbox(
                                "Suppress - Holdings",
                                key="filter_discovery_suppress_holding"
                            )
                            if discovery_suppress_holding:
                                filtered_df = filtered_df[filtered_df['discoverySuppress_y'] == True]
                    
                    with suppress_col2:
                        # Discovery suppress from item filter
                        if 'discoverySuppress' in filtered_df.columns:
                            discovery_suppress_item = st.checkbox(
                                "Suppress - Item",
                                key="filter_discovery_suppress_item"
                            )
                            if discovery_suppress_item:
                                filtered_df = filtered_df[filtered_df['discoverySuppress'] == True]
            
            # User Activity Filters
            with st.expander("User Activity Filters", expanded=False):
                st.markdown("### Created & Updated By")
                
                # Instance creators/updaters
                st.markdown("#### Instance")
                instance_col1, instance_col2 = st.columns(2)
                
                with instance_col1:
                    # Instance Creator filter
                    if 'Instance Creator' in filtered_df.columns:
                        creators = sorted(filtered_df['Instance Creator'].dropna().unique().tolist())
                        selected_creator = st.selectbox(
                            "Created By",
                            options=["All"] + creators,
                            key="filter_instance_creator"
                        )
                        if selected_creator != "All":
                            filtered_df = filtered_df[filtered_df['Instance Creator'] == selected_creator]
                
                with instance_col2:
                    # Instance Updater filter
                    if 'Instance Updater' in filtered_df.columns:
                        updaters = sorted(filtered_df['Instance Updater'].dropna().unique().tolist())
                        selected_updater = st.selectbox(
                            "Updated By",
                            options=["All"] + updaters,
                            key="filter_instance_updater"
                        )
                        if selected_updater != "All":
                            filtered_df = filtered_df[filtered_df['Instance Updater'] == selected_updater]
                
                # Holdings creators/updaters
                st.markdown("#### Holdings")
                holdings_col1, holdings_col2 = st.columns(2)
                
                with holdings_col1:
                    # Holding Creator filter
                    if 'Holding Creator' in filtered_df.columns:
                        h_creators = sorted(filtered_df['Holding Creator'].dropna().unique().tolist())
                        selected_h_creator = st.selectbox(
                            "Created By",
                            options=["All"] + h_creators,
                            key="filter_holding_creator"
                        )
                        if selected_h_creator != "All":
                            filtered_df = filtered_df[filtered_df['Holding Creator'] == selected_h_creator]
                
                with holdings_col2:
                    # Holding Updater filter
                    if 'Holding Updater' in filtered_df.columns:
                        h_updaters = sorted(filtered_df['Holding Updater'].dropna().unique().tolist())
                        selected_h_updater = st.selectbox(
                            "Updated By",
                            options=["All"] + h_updaters,
                            key="filter_holding_updater"
                        )
                        if selected_h_updater != "All":
                            filtered_df = filtered_df[filtered_df['Holding Updater'] == selected_h_updater]
                
                # Item creators/updaters
                st.markdown("#### Item")
                item_col1, item_col2 = st.columns(2)
                
                with item_col1:
                    # Item Creator filter
                    if 'Item Creator' in filtered_df.columns:
                        i_creators = sorted(filtered_df['Item Creator'].dropna().unique().tolist())
                        selected_i_creator = st.selectbox(
                            "Created By",
                            options=["All"] + i_creators,
                            key="filter_item_creator"
                        )
                        if selected_i_creator != "All":
                            filtered_df = filtered_df[filtered_df['Item Creator'] == selected_i_creator]
                
                with item_col2:
                    # Item Updater filter
                    if 'Item Updater' in filtered_df.columns:
                        i_updaters = sorted(filtered_df['Item Updater'].dropna().unique().tolist())
                        selected_i_updater = st.selectbox(
                            "Updated By",
                            options=["All"] + i_updaters,
                            key="filter_item_updater"
                        )
                        if selected_i_updater != "All":
                            filtered_df = filtered_df[filtered_df['Item Updater'] == selected_i_updater]
            





            # Advanced filtering section
            with st.expander("Advanced Filtering", expanded=False):
                st.markdown("### Advanced Filtering")
                
                st.info("""
                Enter Python code to filter the dataframe. Your code should be a condition that would go inside df[] brackets.
                
                **Available variables:**
                - `df`: The current filtered dataframe
                
                **Examples:**
                - `df['Title'].str.contains('Python')`
                - `(df['Publication Date'] > '2010') & (df['Item Status'] == 'Available')`
                - `df['Barcode'].str.startswith('123')`
                """)
                
                code_filter = st.text_area("Python Filter Code", 
                                           placeholder="Example: df['Title'].str.contains('Python', case=False)",
                                           height=100,
                                           key="advanced_filter_code")
                
                apply_col1, apply_col2 = st.columns([1, 3])
                with apply_col1:
                    apply_button = st.button("Apply Filter", key="advanced_filter_button")
                
                if apply_button and code_filter.strip():
                    try:
                        # Create a local copy of the filtered dataframe for the variable name to work
                        df_for_eval = filtered_df.copy()
                        
                        # Execute the filter code
                        filter_result = eval(code_filter)
                        
                        # Check if result is a valid boolean Series or mask
                        if isinstance(filter_result, pd.Series) and filter_result.dtype == bool:
                            # Apply the filter
                            filtered_df = filtered_df[filter_result]
                            st.success(f"Advanced filter applied successfully. {len(filtered_df)} records match.")
                        else:
                            st.error("Filter code must return a boolean Series (condition that can go inside df[])")
                    except Exception as e:
                        st.error(f"Error in filter code: {str(e)}")
            
            # Tags filter
            if 'tags.tagList' in filtered_df.columns:
                # Extract all unique tags from the tagList columns which might contain lists
                all_tags = []
                for tags in filtered_df['tags.tagList'].dropna():
                    if isinstance(tags, list):
                        all_tags.extend(tags)
                    elif isinstance(tags, str):
                        # Handle case where tags might be a string representation of a list
                        try:
                            tag_list = ast.literal_eval(tags)
                            if isinstance(tag_list, list):
                                all_tags.extend(tag_list)
                        except (ValueError, SyntaxError):
                            # If not a valid list representation, treat as a single tag
                            all_tags.append(tags)
                
                unique_tags = sorted(set(all_tags))
                selected_tags = st.multiselect("Tags", unique_tags)
                
                if selected_tags:
                    # Filter rows where any of the selected tags are present
                    def check_tags(x):
                        # Handle NaN values
                        if x is None or (hasattr(x, 'isna') and x.isna().any()):
                            return False
                        
                        tag_list = []
                        try:
                            if isinstance(x, list):
                                tag_list = x
                            elif isinstance(x, str) and x.strip():
                                try:
                                    parsed = ast.literal_eval(x)
                                    if isinstance(parsed, list):
                                        tag_list = parsed
                                    else:
                                        tag_list = [x]
                                except (ValueError, SyntaxError):
                                    tag_list = [x]
                            
                            return any(tag in tag_list for tag in selected_tags)
                        except:
                            # If any error occurs, assume no match
                            return False
                        
                    filtered_df = filtered_df[filtered_df['tags.tagList'].apply(check_tags)]
            
            # Show a sample of the filtered dataframe (10 records)
            st.subheader("Data Preview (Sample)")
            st.dataframe(filtered_df[selected_columns].head(10), use_container_width=True)
            st.info(f"Showing 10 records as sample. Total filtered records: {len(filtered_df)} out of {len(df)} total records")
            
            # Export options
            st.subheader("Export Data")
            
            export_format = st.radio("Export format", ["CSV", "Excel"], key="export_format")
            
            if export_format == "CSV":
                # Add delimiter option for CSV
                csv_delimiter = st.text_input("CSV Delimiter", value=",", max_chars=1, key="csv_delimiter")
                if not csv_delimiter:  # Default to comma if empty
                    csv_delimiter = ","
            
            if st.button("Export", key="export_button"):
                if export_format == "CSV":
                    csv = filtered_df[selected_columns].to_csv(index=False, sep=csv_delimiter)
                    b64 = base64.b64encode(csv.encode()).decode()
                    href = f'<a href="data:file/csv;base64,{b64}" download="bibliographic_report.csv">Download CSV File</a>'
                    st.markdown(href, unsafe_allow_html=True)
                else:  # Excel
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        filtered_df[selected_columns].to_excel(writer, index=False, sheet_name='Bibliographic Report')
                    excel_data = output.getvalue()
                    b64 = base64.b64encode(excel_data).decode()
                    href = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="bibliographic_report.xlsx">Download Excel File</a>'
                    st.markdown(href, unsafe_allow_html=True)
                
                st.success(f"Export complete! {len(filtered_df)} records exported.")
            
            # Export data section ends here
    
    with tabs[1]:  # Circulation Report Tab
        #st.subheader("Circulation Report")
        
        # Check if circulation data is loaded
        if 'circulation_data_loaded' not in st.session_state:
            st.session_state.circulation_data_loaded = False
        
        if not st.session_state.circulation_data_loaded:
            if st.button("Load Circulation Data", key="circulation_load_button"):
                try:
                    # Set up header for API calls
                    header_dict = {
                        "x-okapi-tenant": st.session_state.tenant,
                        "x-okapi-token": st.session_state.token
                    }
                    
                    with st.spinner("Loading circulation data from Medad..."):
                        # Get loans data
                        df_loans = get_loans(st.session_state.okapi_url, header_dict)
                        st.success("✅ Loans data loaded")
                        
                        # Get users data
                        df_users = get_users(st.session_state.okapi_url, header_dict)
                        st.success("✅ Users data loaded")
                        
                        # Get fines data
                        df_fines = get_fines(st.session_state.okapi_url, header_dict)
                        st.success("✅ Fines data loaded")
                        
                        # Get patron groups
                        patron_groups = get_patron_groups(st.session_state.okapi_url, header_dict)
                        st.success("✅ Patron groups loaded")
                        
                        # Merge loans with users
                        if not df_loans.empty and not df_users.empty:
                            merged_df = df_loans.merge(df_users, how='inner', left_on='userId', right_on='id', suffixes=('_Loans', '_Users'))
                            st.success("✅ Merged loans and users data")
                            
                            # Add patron group names
                            if patron_groups and 'patronGroup' in merged_df.columns:
                                # Create a new column with patron group names
                                merged_df['patronGroupName'] = merged_df['patronGroup'].map(patron_groups)
                                # For any missing mappings, keep the original ID
                                merged_df['patronGroupName'] = merged_df['patronGroupName'].fillna(merged_df['patronGroup'])
                        else:
                            st.warning("Could not merge loans and users data due to empty dataframes")
                            merged_df = pd.DataFrame()
                        
                        # Store data in session state
                        st.session_state.circulation_df = merged_df
                        st.session_state.fines_df = df_fines
                        st.session_state.patron_groups = patron_groups  # Store for later use
                        st.session_state.circulation_data_loaded = True
                    st.success("Circulation data successfully loaded and processed!")
                    st.experimental_rerun()
                    
                except Exception as e:
                    st.error(f"Error loading circulation data: {str(e)}")
        else:
            # Data is loaded, display the DataFrame with filter controls
            if 'circulation_df' in st.session_state and not st.session_state.circulation_df.empty:
                # Get the DataFrame from session state
                filtered_df = st.session_state.circulation_df.copy()
                
                st.subheader("Circulation Report Filters")
                
                # Create collapsible section for date filters
                with st.expander("Date Filters", expanded=True):
                    st.markdown("### Date Range Filters")
                    date_col1, date_col2 = st.columns(2)
                    
                    # Display date range filters
                    with date_col1:
                        if 'loanDate' in filtered_df.columns:
                            # Convert to datetime if not already, and handle timezone information
                            if filtered_df['loanDate'].dtype != 'datetime64[ns]':
                                filtered_df['loanDate'] = pd.to_datetime(filtered_df['loanDate'], errors='coerce', utc=True)
                            
                            # Get min and max dates
                            min_date = filtered_df['loanDate'].min().date() if not filtered_df['loanDate'].isna().all() else datetime.date.today()
                            max_date = filtered_df['loanDate'].max().date() if not filtered_df['loanDate'].isna().all() else datetime.date.today()
                            
                            # Date range picker
                            loan_date_range = st.date_input(
                                "Loan Date Range",
                                value=(min_date, max_date),
                                min_value=min_date,
                                max_value=max_date
                            )
                            
                            # Apply filter if a valid range is selected
                            if len(loan_date_range) == 2:
                                start_date, end_date = loan_date_range
                                # Convert to pandas datetime for filtering with timezone info
                                start_date = pd.Timestamp(start_date).tz_localize('UTC')
                                end_date = (pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)).tz_localize('UTC')
                                # Apply filter
                                filtered_df = filtered_df[(filtered_df['loanDate'] >= start_date) & 
                                                       (filtered_df['loanDate'] <= end_date)]
                    
                    with date_col2:
                        if 'returnDate' in filtered_df.columns:
                            # Convert to datetime if not already, and handle timezone information
                            if filtered_df['returnDate'].dtype != 'datetime64[ns]':
                                filtered_df['returnDate'] = pd.to_datetime(filtered_df['returnDate'], errors='coerce', utc=True)
                            
                            # Only proceed if there are valid dates
                            if not filtered_df['returnDate'].isna().all():
                                # Get min and max dates
                                min_date = filtered_df['returnDate'].min().date()
                                max_date = filtered_df['returnDate'].max().date()
                                
                                # Date range picker
                                checkin_date_range = st.date_input(
                                    "Check-in Date Range",
                                    value=(min_date, max_date),
                                    min_value=min_date,
                                    max_value=max_date
                                )
                                
                                # Apply filter if a valid range is selected
                                if len(checkin_date_range) == 2:
                                    start_date, end_date = checkin_date_range
                                    # Convert to pandas datetime for filtering with timezone info
                                    start_date = pd.Timestamp(start_date).tz_localize('UTC')
                                    end_date = (pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)).tz_localize('UTC')
                                    # Apply filter
                                    filtered_df = filtered_df[(filtered_df['returnDate'] >= start_date) & 
                                                            (filtered_df['returnDate'] <= end_date)]
                
                # Create collapsible section for item and circulation filters
                with st.expander("Item & Circulation Filters", expanded=True):
                    st.markdown("### Item & Circulation Details")
                    circ_col1, circ_col2, circ_col3 = st.columns(3)
                    
                    with circ_col1:
                        # Circulation Action filter
                        if 'action' in filtered_df.columns:
                            action_values = sorted(filtered_df['action'].dropna().unique().tolist())
                            selected_action = st.multiselect("Circulation Action", action_values)
                            if selected_action:
                                filtered_df = filtered_df[filtered_df['action'].isin(selected_action)]
                    
                    with circ_col2:
                        # Circulation Status filter
                        if 'status.name' in filtered_df.columns:
                            status_values = sorted(filtered_df['status.name'].dropna().unique().tolist())
                            selected_status = st.multiselect("Circulation Status", status_values)
                            if selected_status:
                                filtered_df = filtered_df[filtered_df['status.name'].isin(selected_status)]
                    
                    with circ_col3:
                        # Material Type filter
                        if 'materialType.name' in filtered_df.columns:
                            material_values = sorted(filtered_df['materialType.name'].dropna().unique().tolist())
                            selected_material = st.multiselect("Material Type", material_values)
                            if selected_material:
                                filtered_df = filtered_df[filtered_df['materialType.name'].isin(selected_material)]
                
                # Create collapsible section for patron and location filters
                with st.expander("Patron & Location Filters", expanded=True):
                    st.markdown("### Patron & Location Details")
                    patron_col1, patron_col2 = st.columns(2)
                    
                    with patron_col1:
                        # Patron Group filter
                        if 'patronGroupName' in filtered_df.columns:
                            patron_values = sorted(filtered_df['patronGroupName'].dropna().unique().tolist())
                            selected_patron = st.multiselect("Patron Group", patron_values)
                            if selected_patron:
                                filtered_df = filtered_df[filtered_df['patronGroupName'].isin(selected_patron)]
                    
                    with patron_col2:
                        # Item Location filter
                        if 'location.name' in filtered_df.columns:
                            location_values = sorted(filtered_df['location.name'].dropna().unique().tolist())
                            selected_location = st.multiselect("Item Location", location_values)
                            if selected_location:
                                filtered_df = filtered_df[filtered_df['location.name'].isin(selected_location)]
                
                # Create collapsible section for financial filters
                with st.expander("Financial Filters", expanded=False):
                    st.markdown("### Fines & Payments")
                    fine_col1, fine_col2 = st.columns(2)
                    
                    with fine_col1:
                        # Fine Status filter
                        # Using fines data from session state
                        if 'fines_df' in st.session_state and not st.session_state.fines_df.empty:
                            if 'feeFineOwner' in st.session_state.fines_df:
                                fine_values = sorted(st.session_state.fines_df['feeFineOwner'].dropna().unique().tolist())
                                selected_fine = st.multiselect("Fine Status", fine_values)
                                if selected_fine and 'id_Loans' in filtered_df.columns:
                                    # This would require joining with fines data, simplified for now
                                    pass
                    
                    with fine_col2:
                        # Payment Status filter
                        # Using fines data from session state
                        if 'fines_df' in st.session_state and not st.session_state.fines_df.empty:
                            if 'paymentStatus.name' in st.session_state.fines_df:
                                payment_values = sorted(st.session_state.fines_df['paymentStatus.name'].dropna().unique().tolist())
                                selected_payment = st.multiselect("Payment Status", payment_values)
                                if selected_payment and 'id_Loans' in filtered_df.columns:
                                    # This would require joining with fines data, simplified for now
                                    pass
                
                # Create collapsible section for tags filter
                with st.expander("Tags Filter", expanded=False):
                    # Tags filter
                    if 'tags.tagList' in filtered_df.columns:
                        # Extract all unique tags from the tagList columns which might contain lists
                        all_tags = []
                        for tags in filtered_df['tags.tagList'].dropna():
                            if isinstance(tags, list):
                                all_tags.extend(tags)
                            elif isinstance(tags, str):
                                # Handle case where tags might be a string representation of a list
                                try:
                                    tag_list = ast.literal_eval(tags)
                                    if isinstance(tag_list, list):
                                        all_tags.extend(tag_list)
                                except (ValueError, SyntaxError):
                                    # If not a valid list representation, treat as a single tag
                                    all_tags.append(tags)
                        
                        unique_tags = sorted(set(all_tags))
                        selected_tags = st.multiselect("Tags", unique_tags)
                        
                        if selected_tags:
                            # Filter rows where any of the selected tags are present
                            def check_tags(x):
                                # Handle NaN values
                                if x is None or (hasattr(x, 'isna') and x.isna().any()):
                                    return False
                                
                                tag_list = []
                                try:
                                    if isinstance(x, list):
                                        tag_list = x
                                    elif isinstance(x, str) and x.strip():
                                        try:
                                            parsed = ast.literal_eval(x)
                                            if isinstance(parsed, list):
                                                tag_list = parsed
                                            else:
                                                tag_list = [x]
                                        except (ValueError, SyntaxError):
                                            tag_list = [x]
                                    
                                    return any(tag in tag_list for tag in selected_tags)
                                except:
                                    # If any error occurs, assume no match
                                    return False
                            
                            filtered_df = filtered_df[filtered_df['tags.tagList'].apply(check_tags)]
                
                st.markdown("---")
                
                # Column selection
                all_columns = filtered_df.columns.tolist()
                default_columns = ['loanDate', 'returnDate', 'action', 'status.name', 
                                  'patronGroupName', 'materialType.name', 'location.name', 
                                  'tags.tagList']
                default_columns = [col for col in default_columns if col in all_columns]
                
                selected_columns = st.multiselect(
                    "Select columns to display",
                    options=all_columns,
                    default=default_columns
                )
                
                if selected_columns:
                    # Display only a sample of the filtered dataframe (10 records)
                    st.subheader("Data Preview (Sample)")
                    st.dataframe(filtered_df[selected_columns].head(10))
                    st.info(f"Showing 10 records as sample. Total filtered records: {len(filtered_df)} out of {len(st.session_state.circulation_df)} total records")
                    
                    # Export functionality
                    st.subheader("Export Data")
                    
                    export_format = st.radio("Export format", ["CSV", "Excel"], key="circ_export_format")
                    
                    if export_format == "CSV":
                        # Add delimiter option for CSV
                        csv_delimiter = st.text_input("CSV Delimiter", value=",", max_chars=1, key="circ_csv_delimiter")
                        if not csv_delimiter:  # Default to comma if empty
                            csv_delimiter = ","
                    
                    if st.button("Export", key="circ_export_button"):
                        if export_format == "CSV":
                            csv = filtered_df[selected_columns].to_csv(index=False, sep=csv_delimiter)
                            b64 = base64.b64encode(csv.encode()).decode()
                            href = f'<a href="data:file/csv;base64,{b64}" download="circulation_report.csv">Download CSV File</a>'
                            st.markdown(href, unsafe_allow_html=True)
                        else:  # Excel
                            output = io.BytesIO()
                            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                                filtered_df[selected_columns].to_excel(writer, sheet_name='Circulation Report', index=False)
                            b64 = base64.b64encode(output.getvalue()).decode()
                            href = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="circulation_report.xlsx">Download Excel File</a>'
                            st.markdown(href, unsafe_allow_html=True)
                        
                        st.success(f"Export complete! {len(filtered_df)} records exported.")
                else:
                    st.warning("Please select at least one column to display")
                    
                # Export data section ends here
            else:
                st.warning("No circulation data available. Please load the data first.")
                
                # Add a button to reload data
                if st.button("Reload Circulation Data", key="reload_circulation_button"):
                    st.session_state.circulation_data_loaded = False
                    st.experimental_rerun()
    
    with tabs[2]:  # Loan Count Tab
        if not st.session_state.loan_count_data_loaded:
            if st.button("Load Loan Count Data", key="loan_count_load_button"):
                try:
                    # Set up header for API calls
                    header_dict = {
                        "x-okapi-tenant": st.session_state.tenant,
                        "x-okapi-token": st.session_state.token
                    }
                    
                    with st.spinner("Loading comprehensive loan count data from Medad..."):
                        # Get instances, holdings, and items data
                        df_instances = get_instances(st.session_state.okapi_url, header_dict)
                        st.success("✅ Instances data loaded")
                        
                        df_holdings = get_holdings(st.session_state.okapi_url, header_dict)
                        st.success("✅ Holdings data loaded")
                        
                        df_items = get_items(st.session_state.okapi_url, header_dict)
                        st.success("✅ Items data loaded")
                        
                        # Get material types data
                        df_mtypes = get_mtypes(st.session_state.okapi_url, header_dict)
                        st.success("✅ Material types data loaded")
                        
                        # Get loan count data
                        df_loan_count = get_loan_count_data(st.session_state.okapi_url, header_dict)
                        st.success("✅ Loan count data loaded")
                        
                        # Create a dictionary mapping material type IDs to names
                        material_types = df_mtypes.set_index('id')['name'].to_dict()
                        
                        # Process data for merging
                        with st.spinner("Merging data..."):
                            # First merge instances with holdings
                            # Use explicit suffixes to avoid duplicate column issues
                            merged_df = df_instances.merge(
                                df_holdings, 
                                left_on='id', 
                                right_on='instanceId', 
                                how='inner',
                                suffixes=('_instance', '_holdings')
                            )
                            
                            # Then merge with items
                            merged_df = merged_df.merge(
                                df_items, 
                                left_on='id_holdings', 
                                right_on='holdingsRecordId', 
                                how='inner',
                                suffixes=('', '_item')
                            )
                            
                            # Process contributor data if it exists
                            if 'contributors' in merged_df.columns:
                                merged_df['contributors'] = merged_df['contributors'].apply(
                                    lambda x: x[0]['name'] if isinstance(x, list) and len(x) > 0 else '')
                            
                            # Add material type names
                            merged_df['materialTypeName'] = merged_df['materialTypeId'].map(material_types)
                            
                            # Format dates if present
                            date_columns = ['lastCheckIn.dateTime', 'metadata.createdDate']
                            for col in date_columns:
                                if col in merged_df.columns:
                                    merged_df[col] = pd.to_datetime(merged_df[col], errors='coerce').dt.strftime('%Y-%m-%d')
                            
                            # Get loan counts per item
                            if not df_loan_count.empty:
                                # Group by itemId and count loans
                                loan_counts = df_loan_count.groupby('itemId').size().reset_index(name='loan_count')
                                
                                # Finally, merge with loan counts
                                # Use the item's id column to match with itemId in loan_counts
                                final_df = pd.merge(
                                    merged_df, 
                                    loan_counts, 
                                    left_on='id', 
                                    right_on='itemId', 
                                    how='left'
                                )
                            else:
                                # If no loan data, just add a loan_count column with zeros
                                final_df = merged_df.copy()
                                final_df['loan_count'] = 0
                            
                            # Fill NaN loan counts with 0 and convert to integer
                            final_df['loan_count'] = final_df['loan_count'].fillna(0).astype(int)
                        
                        # Store the final dataframe in session state
                        st.session_state.loan_count_df = final_df
                        st.session_state.loan_count_data_loaded = True
                    
                    st.success("Loan count data successfully loaded and processed!")
                    st.experimental_rerun()
                    
                except Exception as e:
                    st.error(f"Error loading loan count data: {str(e)}")
        else:
            # Data is loaded, display the DataFrame with filter controls
            if 'loan_count_df' in st.session_state and not st.session_state.loan_count_df.empty:
                # Get the DataFrame from session state
                filtered_df = st.session_state.loan_count_df.copy()
                
                # Create filter columns for main filtering options
                col1, col2, col3 = st.columns(3)
                
                # Display material type filter
                with col1:
                    if 'materialTypeName' in filtered_df.columns:
                        material_types = sorted(filtered_df['materialTypeName'].dropna().unique().tolist())
                        selected_material = st.multiselect("Material Type", material_types, key="loan_count_material_type")
                        if selected_material:
                            filtered_df = filtered_df[filtered_df['materialTypeName'].isin(selected_material)]
                
                # Display loan count range filter
                with col2:
                    if 'loan_count' in filtered_df.columns:
                        min_loans = int(filtered_df['loan_count'].min())
                        max_loans = int(filtered_df['loan_count'].max())
                        
                        # Handle the case where min and max are equal
                        if min_loans == max_loans:
                            st.info(f"All items have the same loan count: {min_loans}")
                            loan_count_range = (min_loans, max_loans)
                        else:
                            # Ensure max is greater than min to avoid RangeError
                            if max_loans <= min_loans:
                                max_loans = min_loans + 1
                                
                            loan_count_range = st.slider(
                                "Loan Count Range", 
                                min_value=min_loans,
                                max_value=max_loans,
                                value=(min_loans, max_loans),
                                key="loan_count_range"
                            )
                        
                        filtered_df = filtered_df[
                            (filtered_df['loan_count'] >= loan_count_range[0]) & 
                            (filtered_df['loan_count'] <= loan_count_range[1])
                        ]
                
                # Display item status filter
                with col3:
                    if 'status.name' in filtered_df.columns:
                        status_values = sorted(filtered_df['status.name'].dropna().unique().tolist())
                        selected_status = st.multiselect("Item Status", status_values, key="loan_count_status")
                        if selected_status:
                            filtered_df = filtered_df[filtered_df['status.name'].isin(selected_status)]
                
                # Column selection
                all_columns = filtered_df.columns.tolist()
                default_columns = ['title', 'callNumber', 'barcode', 'materialTypeName', 'status.name', 
                                  'contributors', 'loan_count', 'lastCheckIn.dateTime', 'metadata.createdDate']
                default_columns = [col for col in default_columns if col in all_columns]
                
                selected_columns = st.multiselect(
                    "Select columns to display",
                    options=all_columns,
                    default=default_columns,
                    key="loan_count_columns"
                )
                
                if selected_columns:
                    # Display only a sample of the filtered dataframe (10 records)
                    st.subheader("Data Preview (Sample)")
                    st.dataframe(filtered_df[selected_columns].head(10))
                    st.info(f"Showing 10 records as sample. Total filtered records: {len(filtered_df)} out of {len(st.session_state.loan_count_df)} total records")
                    
                    # Export functionality
                    st.subheader("Export Data")
                    
                    export_format = st.radio("Export format", ["CSV", "Excel"], key="loan_count_export_format")
                    
                    if export_format == "CSV":
                        # Add delimiter option for CSV
                        csv_delimiter = st.text_input("CSV Delimiter", value=",", max_chars=1, key="loan_count_csv_delimiter")
                        if not csv_delimiter:  # Default to comma if empty
                            csv_delimiter = ","
                    
                    if st.button("Export", key="loan_count_export_button"):
                        if export_format == "CSV":
                            csv = filtered_df[selected_columns].to_csv(index=False, sep=csv_delimiter)
                            b64 = base64.b64encode(csv.encode()).decode()
                            href = f'<a href="data:file/csv;base64,{b64}" download="loan_count_report.csv">Download CSV File</a>'
                            st.markdown(href, unsafe_allow_html=True)
                        else:  # Excel
                            output = io.BytesIO()
                            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                                filtered_df[selected_columns].to_excel(writer, sheet_name='Loan Count Report', index=False)
                            b64 = base64.b64encode(output.getvalue()).decode()
                            href = f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="loan_count_report.xlsx">Download Excel File</a>'
                            st.markdown(href, unsafe_allow_html=True)
                        
                        st.success(f"Export complete! {len(filtered_df)} records exported.")
                else:
                    st.warning("Please select at least one column to display")
                    
                # Export data section ends here
else:
    # Show welcome message if not logged in
    st.info("👈 Please enter your Medad credentials in the sidebar to get started.")
    
    st.markdown("""
    ### Welcome to the Medad Reporter!
    
    This application allows you to:
    
    1. Connect to your Medad instance
    2. Retrieve bibliographic data (instances, holdings, items)
    3. View and filter the data
    4. Export selected data to CSV or Excel
    
    To get started, enter your Medad credentials in the sidebar.
    """)
