import duckdb
import pandas as pd
import pyarrow.parquet as pq
import requests

_READ_TSV = (
    lambda x: f"read_csv_auto('title.{x}.tsv.gz', header=True, quote='', sep='\t', nullstr='\\N')"
)
_EXPORT_PARQUET = (
    lambda x: f"COPY (SELECT * FROM {_READ_TSV(x)}) TO '{x}.parquet' (FORMAT 'parquet');"
)

_STMT_DENORMALIZE = f"""
  WITH series AS (
    SELECT *
    FROM {_READ_TSV('basics')}
    --FROM 'basics.parquet'
    WHERE titleType IN ('tvEpisode', 'tvSeries', 'tvMiniSeries')
  ),
  ratings AS (
    SELECT *
    FROM {_READ_TSV('ratings')}
    --FROM 'ratings.parquet'
  ),
  episodes AS (
    SELECT *
    FROM {_READ_TSV('episode')}
    --FROM 'episode.parquet'
    WHERE seasonNumber IS NOT NULL
      AND episodeNumber IS NOT NULL
  )

  SELECT
    --series.titleType,
    series.primaryTitle AS episodeTitle,
    series.startYear::INT AS startYear,
    series.endYear::INT AS endYear,
    episodes.tconst AS episodeID,
    episodes.parentTconst AS seriesID,
    episodes.seasonNumber::INT AS seasonNumber,
    episodes.episodeNumber::INT AS episodeNumber,
    ratings.averageRating::FLOAT AS averageRating,
    ratings.numVotes::INT as numVotes,
    (SELECT
       primaryTitle
     FROM series
     WHERE series.tconst = episodes.parentTconst
    ) AS seriesTitle,
    --RIGHT(episodes.parentTconst, 2)::INT / 20 AS partition_col
  FROM episodes
  LEFT OUTER JOIN ratings ON (ratings.tconst = episodes.tconst)
  INNER JOIN series ON (series.tconst = episodes.tconst)
  WHERE seriesTitle IS NOT NULL
  ORDER BY seriesID
"""


def get_file(file_url):
    r = requests.get(file_url, stream=True)
    with open(file_url.split("/")[-1], "wb") as fp:
        for chunk in r.iter_content(chunk_size=1024):
            fp.write(chunk)


def main():
    con = duckdb.connect(database=":memory:")

    get_file("https://datasets.imdbws.com/title.basics.tsv.gz")
    get_file("https://datasets.imdbws.com/title.ratings.tsv.gz")
    get_file("https://datasets.imdbws.com/title.episode.tsv.gz")

    # optionally export TSV files to parquet
    # con.execute("\n".join(map(_EXPORT_PARQUET, ["basics", "ratings", "episode"])))

    table = con.execute(_STMT_DENORMALIZE).fetch_arrow_table()

    # export the Parquet file with desired options
    pq.write_to_dataset(
        table,
        root_path="dataset",
        # partition_cols=["partition_col"],
        compression="zstd",
    )


if __name__ == "__main__":
    main()
