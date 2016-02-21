

queryMap = {

  "geo_delta_fetch": """
          SELECT new_gid as gid, max(new_updated_on) as last_updated_dt
            FROM geoinfo_delta
           WHERE log_id > %s
             AND log_id <= %s
        GROUP BY new_gid
  """,

  "geo_delta_merge": """
     INSERT INTO geo_status (gid, source_dt, bucket)
          VALUES %s
ON DUPLICATE KEY UPDATE source_dt = VALUES(source_dt)
  """,

  "max_source_log_id": """
          SELECT max(log_id) as max_id
            FROM geoinfo_delta
  """,

  "last_target_log_id": """
          SELECT log_id 
            FROM geo_bookmark
           WHERE id IN (          SELECT max(id)
                                    FROM geo_bookmark
                       )

  """,

  "geo_bookmark_insert": """
     INSERT INTO geo_bookmark(log_id, recs, time_ms) 
          VALUES (%s, %s, %s)
  """,

  "geo_id_fetch": """
          SELECT product_id, source_dt
            FROM product_status
           WHERE %s = MOD(bucket, %s) 
             AND source_dt > target_dt
  """,

  "geo_success_merge": """
     INSERT INTO geo_status (gid, source_dt, target_dt)
          VALUES %s
ON DUPLICATE KEY UPDATE target_dt = VALUES(target_dt)
  """,

  "geo_failure_merge": """
     INSERT INTO geo_status (product_id, source_dt, last_error)
          VALUES %s
ON DUPLICATE KEY UPDATE last_error = VALUES(last_error)
  """

}
