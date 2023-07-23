import os
import pandas
import matplotlib.pyplot as plt
from google.cloud import bigquery


def bq_job():
    gcp_project = 'silent-star-287715'
    client = bigquery.Client(gcp_project)

    # Perform a query.
    query = (
        """
        SELECT
          *
        FROM
          `bigquery-public-data.wikipedia.pageviews_2023`
        WHERE
          TIMESTAMP_TRUNC(datehour, DAY)  <= TIMESTAMP("2023-07-23") 
        ORDER BY views DESC
        LIMIT
          1000
    """)

    query2 = (
        """
        SELECT
          *
        FROM
          `bigquery-public-data.world_bank_global_population.population_by_country`
        WHERE
          country in ('United States', 'United Kingdom', 'Germany', 'Australia', 'Brazil', 'Russia', 'South Africa','China','India')
    """)

    print(query)

    query_job = client.query(query)  # API request

    df = pandas.DataFrame(query_job.result())  # Waits for query to finish

    print(df)

    df.plot()

    plt.show()


if __name__ == '__main__':
    bq_job()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
