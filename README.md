
# IMDb Movie Review Analysis using PySpark

## Project Overview

This project uses Apache PySpark to process and analyze
the IMDb Dataset of 50K Movie Reviews.

The dataset contains movie reviews classified into
positive and negative sentiments.

## Objectives

1. Load the IMDb CSV dataset using PySpark.
2. Explore the DataFrame schema and first five rows.
3. Remove null and empty reviews.
4. Count total, positive and negative reviews.
5. Calculate the character length of each review.
6. Filter reviews containing more than 500 characters.
7. Save the cleaned and transformed dataset as CSV.

## Technologies Used

- Python
- Apache PySpark
- Spark SQL
- GitHub
- Google Colab

## Dataset

IMDb Dataset of 50K Movie Reviews:

https://www.kaggle.com/datasets/lakshmi25npathi/imdb-dataset-of-50k-movie-reviews

The dataset contains two columns:

- review
- sentiment

## Project Files

- movie_reviews.py: Main PySpark implementation.
- requirements.txt: Required Python dependency.
- README.md: Project documentation.

## Installation

Install the required dependency:

pip install -r requirements.txt

## Execution

Place the IMDb Dataset.csv file in the project directory.

Run:

python movie_reviews.py

## Output

The program generates a cleaned and transformed
CSV dataset containing the original review,
sentiment and review_length columns.

## Author

Nikhil Prabhakar
