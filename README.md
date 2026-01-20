# Data Pipeline - Gender Inclusion Project: Awareness, Education and Perception

This is a complete ETL (Extract, Transform, Load) pipeline project that collects, processes, and analyzes survey responses related to gender and inclusion issues using KoboToolbox, Python, PostgreSQL and PowerBI.

## Project Overview

This project fetches survey data from KoboToolbox, processes it with Python, and stores it in a PostgreSQL database for analysis and use PowerBI for visualization and reporting.


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
