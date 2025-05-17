# Movie Recommendation Pipeline

A scalable movie recommendation system built with PySpark, implementing collaborative filtering using the Alternating Least Squares (ALS) algorithm. The pipeline processes movie ratings data, trains a recommendation model, and generates personalized movie recommendations for users.

## Overview

This project implements a movie recommendation system that:
- Processes movie ratings, metadata, and user interactions
- Trains a collaborative filtering model using ALS
- Generates personalized movie recommendations
- Stores results in PostgreSQL for easy access
- Supports both local and S3-based data storage

## Tech Stack

- **Data Processing**: Apache Spark (PySpark)
- **Machine Learning**: Spark ML (ALS algorithm)
- **Storage**: 
  - Local filesystem (development)
  - Amazon S3 (production)
  - PostgreSQL (recommendations storage)
- **Testing**: pytest, pytest-spark
- **Configuration**: YAML
- **AWS Integration**: boto3, moto (for testing)

## Project Structure

```
movie-recommendation-pipeline/
├── data/                    # Data files
│   ├── movies.csv          # Movie metadata
│   ├── ratings.csv         # User ratings
│   ├── tags.csv           # User tags
│   └── links.csv          # Movie links
├── src/                    # Source code
│   ├── data_loader.py     # Data loading utilities
│   ├── cleaner.py         # Data cleaning functions
│   ├── feature_engineer.py # Feature engineering
│   ├── model.py           # ALS model implementation
│   ├── writer.py          # Database writing utilities
│   └── utils.py           # Common utilities
├── tests/                  # Test files
│   ├── data/              # Test data
│   ├── test_*.py          # Test modules
│   └── conftest.py        # Test configuration
├── logs/                   # Log files
├── models/                   # ML models
├── config.yaml            # Pipeline configuration
├── main.py               # Pipeline entry point
└── requirements.txt      # Project dependencies
```

## Setup

1. **Prerequisites**
   - Python 3.8+
   - Java 8+ (required for PySpark)
   - PostgreSQL (for storing recommendations)

2. **Installation**
   ```bash
   # Clone the repository
   git clone <repository-url>
   cd movie-recommendation-pipeline

   # Create and activate virtual environment
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate

   # Install dependencies
   pip install -r requirements.txt
   ```

3. **PostgreSQL Setup**
   ```sql
   -- Create database and user
   CREATE DATABASE {YOUR-DB-NAME};
   CREATE USER {YOUR-USERNAME} WITH PASSWORD '{YOUR-PASSWORD}';
   GRANT ALL PRIVILEGES ON DATABASE {YOUR-DB-NAME} TO {YOUR-USERNAME};
   ```

## Configuration

The pipeline is configured through `config.yaml`. Key sections include:

- **Storage**: Configure data source (local/S3)
- **Database**: PostgreSQL connection settings
- **Spark**: Application settings
- **ALS Model**: Algorithm parameters
- **Output**: Recommendation settings
- **Processing**: Data processing parameters
- **Logging**: Log configuration

Example configuration:
```yaml
storage:
  mode: "local"  # or "s3" for production
  local:
    movies: "data/movies.csv"
    ratings: "data/ratings.csv"
    # ...

postgres:
  host: "localhost"
  port: 5432
  database: "moviedb"
  # ...
```

## Running the Pipeline

1. **Local Mode**
   ```bash
   python main.py
   ```

2. **S3 Mode**
   ```bash
   # Set AWS credentials
   export AWS_ACCESS_KEY_ID=your_access_key
   export AWS_SECRET_ACCESS_KEY=your_secret_key
   
   # Update config.yaml to use S3
   # storage:
   #   mode: "s3"
   
   python main.py
   ```

## Testing

1. **Local Tests**
   ```bash
   # Run all tests
   pytest -v
   
   # Run specific test file
   pytest -v tests/test_data_loader.py
   ```

2. **Integration Tests**
   ```bash
   # Set AWS credentials for S3 tests
   export AWS_ACCESS_KEY_ID=your_access_key
   export AWS_SECRET_ACCESS_KEY=your_secret_key
   
   # Run integration tests
   pytest -v -m integration
   ```

3. **Mock Tests**
   ```bash
   # Run tests with mocked S3
   pytest -v tests/test_data_loader.py
   ```

## Future Enhancements

1. **Model Improvements**
   - Implement content-based filtering
   - Add hybrid recommendation approach
   - Optimize ALS parameters

2. **Performance**
   - Add caching for intermediate results
   - Implement batch processing
   - Optimize Spark configurations

3. **Features**
   - Add user profile analysis
   - Implement recommendation explanation
   - Add real-time recommendation updates

4. **Infrastructure**
   - Add Docker support
   - Implement CI/CD pipeline
   - Add monitoring and alerting

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
