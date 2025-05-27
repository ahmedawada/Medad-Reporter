# Bibliographic Reporter

A Streamlit application for retrieving, viewing, filtering, and exporting bibliographic data, circulation information, and loan count statistics from FOLIO.

## Features

- Connect to any FOLIO instance using Okapi URL, tenant, username, and password
- Retrieve bibliographic data from FOLIO Inventory API (instances, holdings, and items)
- View circulation data including loan information and patron details
- Analyze loan count statistics with material type and item status filtering
- View merged data in an interactive table with filtering capabilities
- Customize displayed columns
- Export filtered data to CSV or Excel formats with custom delimiter options

## Requirements

- Python 3.7 or higher
- Required packages as listed in `requirements.txt`

## Installation

1. Clone this repository:
```
git clone https://github.com/yourusername/bibliographic-reporter.git
cd bibliographic-reporter
```

2. Install required packages:
```
pip install -r requirements.txt
```

## Running the Application

To run the application, use the following command:

```
streamlit run app.py
```

The application will open in your default web browser at `http://localhost:8501`.

## Usage Instructions

1. **Login**: Enter your FOLIO credentials in the sidebar:
   - Okapi URL (e.g., https://okapi.example.com)
   - Tenant name
   - Username
   - Password

2. **Bibliographic Report Tab**:
   - Click the "Load Bibliographic Data" button to retrieve data from FOLIO
   - Use the "Select columns to display" dropdown to choose which columns to show
   - Use the filter controls to filter data by column values
   - Export the filtered data in CSV or Excel format

3. **Circulation Report Tab**:
   - Click the "Load Circulation Data" button to retrieve circulation data
   - View loan information including patron details
   - Filter data by patron group, loan status, and other parameters
   - Export the filtered data in CSV or Excel format

4. **Loan Count Tab**:
   - Click the "Load Loan Count Data" button to retrieve and process loan statistics
   - View comprehensive information about items including title, barcode, material type, status, and loan count
   - Filter by material type, loan count range, or item status
   - Identify highly circulated or underused items in your collection
   - Export the filtered data in CSV or Excel format

5. **Data Management**:
   - Each tab displays a sample of 10 records for performance
   - To start over, click the "Reset Data" button in the respective tab

## Troubleshooting

- If you encounter connection issues, check that your FOLIO credentials are correct
- For large datasets, the initial loading may take some time
- If the application crashes due to memory issues, try running it on a machine with more RAM

## License

This project is licensed under the MIT License - see the LICENSE file for details.
