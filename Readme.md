# Github project health metric
## Dependencies
The external libraries are included in `build.gradle` file.
- `sqlite-jdbc`: is a library for accessing and creating SQLite database files in Java.
- `gson`: used to convert Java Objects into their JSON representation and vice versa.
- `opencsv`: is a CSV parser library for Java.

## How to get project runs
- The project is built with Gradle (5.6.2) and Java (1.8). Ensure your development environtment is prepared similarly.
- `git clone https://github.com/vanducng/HealthScoreCalculator.git`
- `cd HealthScoreCalculator`
- To check health score last hour
   > `gradle run`
- To check health score for a period
   > `gradle run --args='2019-10-10T00:00:00Z 2019-10-11T03:00:00Z'`

## Technical decisions
- The SQLite is selected because:
   - The application is able to run in local machine with out pre-installed SQL engine
   - It is easier in performing the metric extraction with getting MAX, MIN, AVG, JOIN, etc.
   - The limitation is the speed impact when dealing with big data (eg. data for 1 month period). In this scenario, the more powerful techque should be in place instead of SQLite, such as Paquet with Spark, Postgres, etc.
- `gson` and `opencsv` are used to simplify the task of handling with file structure.
- Since the data is also hosted in Google Bigquery, the better approach to improve the data extraction speed is to apply [Google BigQuery API](https://developers.google.com/api-client-library/java/apis/bigquery/v2)