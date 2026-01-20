# Gender Inclusion Project

A data pipeline project that collects, processes, and analyzes survey responses related to gender and inclusion issues across different countries using KoboToolbox and PostgreSQL.

## Project Overview

This project fetches survey data from KoboToolbox, processes it with Python, and stores it in a PostgreSQL database for analysis. It includes statistical analysis using chi-square tests to examine relationships between demographic factors and gender/inclusion attitudes.

## Features

- **KoboToolbox Integration**: Automatically fetches survey responses from KoboToolbox API
- **Data Processing**: Cleans and transforms survey data with intelligent column mapping
- **Database Management**: Stores processed data in PostgreSQL with proper schema organization
- **Statistical Analysis**: Performs chi-square tests to identify significant relationships
- **Lookup Tables**: Maintains reference tables for gender, age groups, education levels, and countries
- **Response Tracking**: Records responsibility assignments and prioritized actions

## Prerequisites

- Python 3.7+
- PostgreSQL database
- KoboToolbox account with survey setup

## Installation

1. Clone or download the project:
```bash
cd Gender-Inclusion-Project
```

2. Install required dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables in a `.env` file:
```
KOBO_USERNAME=your_kobo_username
KOBO_PASSWORD=your_kobo_password
KOBO_CSV_URL=your_kobo_survey_csv_url
PG_HOST=your_postgres_host
PG_DATABASE=your_database_name
PG_USER=your_postgres_user
PG_PASSWORD=your_postgres_password
PG_PORT=5432
```

## Project Structure

```
Gender-Inclusion-Project/
├── pipeline.py              # Main data processing pipeline
├── requirements.txt         # Python package dependencies
├── output_check.txt         # Sample output log
├── LICENSE                  # Project license
└── README.md               # This file
```

## Usage

Run the data pipeline:
```bash
python pipeline.py
```

The script will:
1. Fetch survey data from KoboToolbox
2. Map and clean column names
3. Create/update lookup tables in PostgreSQL
4. Insert survey responses into the database
5. Perform statistical analysis on the data

## Database Schema

The project creates the following tables in the `gender_inclusion_project` schema:

- **blossom_academy**: Main survey response data
- **gender_lookup**: Gender classification reference
- **age_group_lookup**: Age group categories
- **education_lookup**: Education level reference
- **country_lookup**: Country reference data
- **responsibility_responses**: Tracks responsibility assignments
- **prioritized_actions**: Records prioritized actions

## Survey Questions Tracked

The pipeline captures responses to questions including:
- Gender identity
- Age group
- Education level
- Country of residence
- Understanding of gender and inclusion
- Personal experiences with exclusion
- Barriers to gender inclusion
- Responsibility for promoting inclusion
- Government policy preferences

## Dependencies

- `requests`: HTTP library for KoboToolbox API
- `pandas`: Data manipulation and analysis
- `numpy`: Numerical computing
- `scipy`: Statistical analysis (chi-square tests)
- `psycopg2-binary`: PostgreSQL database adapter
- `python-dotenv`: Environment variable management
- `sqlalchemy`: SQL toolkit and ORM
- `openpyxl`: Excel file support
- `xlrd`: Legacy Excel file support

## License

See [LICENSE](LICENSE) for details.

## Contributing

For questions or improvements, please refer to the project documentation or contact the project maintainer.
