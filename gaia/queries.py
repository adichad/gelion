

queryMap = {

  "base_geo": """
          SELECT archived, 
                 cast(ST_AsGeoJSON(center) as json)->'coordinates' as center,
                 containers,
                 created_on::timestamp,
                 g.gid,
                 name,
                 related,
                 cast(ST_AsGeoJSON(bbox) as json) as shape,
                 s.stdcode as phone_prefix,
                 coalesce(synonyms,ARRAY[]::varchar[]) as synonyms,
                 gt.tags,
                 types,
                 updated_on::timestamp 
            FROM geoinfo_current g
 LEFT OUTER JOIN stdcodes s
              ON g.gid = s.gid
 LEFT OUTER JOIN geoinfo_tags gt
              ON g.gid = gt.gid
           WHERE g.gid in (%s)
             AND g.name IS NOT NULL
  """,

  "container_ids": """
          SELECT containers
            FROM geoinfo_current
           WHERE gid in (%s)
             AND NOT archived
             AND name IS NOT NULL
  """,

  "containers_dag": """
          SELECT cast(ST_AsGeoJSON(center) as json)->'coordinates' as center,
                 containers,
                 gid, 
                 name,
                 cast(ST_AsGeoJSON(bbox) as json) as shape,
                 coalesce(synonyms,ARRAY[]::varchar[]) as synonyms,
                 types
            FROM geoinfo_current
           WHERE gid in (%s)
             AND NOT archived
             AND name IS NOT NULL
  """,

  "related_list": """
          SELECT cast(ST_AsGeoJSON(center) as json) as center,
                 containers,
                 gid, 
                 name,
                 cast(ST_AsGeoJSON(bbox) as json) as shape,
                 coalesce(synonyms,ARRAY[]::varchar[]) as synonyms,
                 types
            FROM geoinfo_current
           WHERE gid in (%s)
             AND NOT archived
             AND name IS NOT NULL
  """,
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
          SELECT gid, source_dt
            FROM geo_status
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
