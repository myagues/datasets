import duckdb
import pandas as pd
import pyarrow.parquet as pq

_READ_TSV = (
    lambda x: f"read_csv_auto('title.{x}.tsv.gz', header=True, quote='', sep='\t', nullstr='\\N')"
)
_EXPORT_PARQUET = (
    lambda x: f"COPY (SELECT * FROM {_READ_TSV(x)}) TO '{x}.parquet' (FORMAT 'parquet');"
)

_STMT = f"""
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
    WHERE seasonNumber IS NOT NULL AND episodeNumber IS NOT NULL
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
    ratings.averageRating,
    ratings.numVotes,
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
  ORDER BY parentTconst
"""


def main():
    con = duckdb.connect(database=":memory:")
    
    # optionally export TSV files to parquet
    # con.execute("\n".join(map(_EXPORT_PARQUET, ["basics", "ratings", "episode"])))

    table = con.execute(_STMT).fetch_arrow_table()

    # export the Parquet file with desired options
    pq.write_to_dataset(
        table,
        root_path="dataset",
        # partition_cols=["partition_col"],
        compression="zstd",
        use_dictionary=["seriesTitle"],
    )


if __name__ == "__main__":
    main()
