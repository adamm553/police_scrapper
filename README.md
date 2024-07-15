Police Statistics Data Scraper and Loader

This repository contains scripts and tools to scrape crime statistics data from the Polish Police website, process it, and load it into a PostgreSQL database.

Features:

- **Data Scraper**: Downloads crime statistics files from the Polish Police website.

- **Excel Processor**: Processes downloaded Excel files to standardize and filter data.

- **Data Loader**: Loads processed data into PostgreSQL database tables.

- **NaN Value Replacement**: Replaces 'n.d.' values in specific Excel files with '0'.

Setup:

Prerequisites:

- Python 3.x installed

- PostgreSQL database setup

- Required Python packages installed (requests, pandas, sqlalchemy, dotenv)

Installation:

1\. Clone the repository:

   ```

   git clone https://github.com/yourusername/your-repository.git

   cd your-repository

   ```

2\. Install dependencies:

   ```

   pip install -r requirements.txt

   ```

3\. Set up your .env file:

   Create a .env file in the root directory of the project with the following variables:

   ```

   USER=your_database_user

   PASSWORD=your_database_password

   HOST=your_database_host

   PORT=your_database_port

   DBNAME=your_database_name

   PATH=/path/to/your/data/files

   ```

Usage:

1\. Run the data scraper to download files:

   ```

   python scraper.py

   ```

2\. Process Excel files:

   ```

   python excel_processor.py

   ```

3\. Replace 'n.d.' values in specific Excel files:

   ```

   python nd_replacer.py

   ```

4\. Load processed data into the PostgreSQL database:

   ```

   python data_loader.py

   ```

Database Schema:

The data is loaded into the following database schema:

- obszar: Contains administrative regions.

- Various tables (przestepstwa_*): Contains crime statistics data categorized by crime type.

Notes:

- Ensure sensitive information in .env file (like database credentials) is kept private and not shared publicly.

- Adjust file paths (download_path, new_path) in excel_processor.py and nd_replacer.py as per your local setup.

Feel free to customize and extend these scripts based on your specific requirements.

---

