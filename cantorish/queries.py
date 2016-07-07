

queryMap = {
  "base_product": """
          SELECT bp.base_product_id as id,
                 bp.title as name,
                 bp.description as description,
                 bp.small_description as small_description,
                 bp.color as color,
                 bp.size as size,
                 bp.product_weight as weight,
                 bp.brand as brand,
                 bp.model_name as model_name,
                 bp.model_number as model_number,
                 bp.manufacture as manufacturer,
                 bp.specifications as specifications,
                 bp.key_features as key_features,
                 bp.meta_title as meta_title,
                 bp.meta_keyword as meta_keyword,
                 bp.meta_description as meta_description,
                 bp.is_configurable as is_configurable,
                 bp.configurable_with as configurable_with,
                 bp.created_date as created_dt,
                 bp.modified_date as updated_dt,
                 bp.is_deleted as is_deleted,
                 bp.status as status,
                 bp.video_url as video_url
            FROM base_product bp
           WHERE bp.base_product_id in (%s)
  """,

  "product_images": """
          SELECT media_id as id, media_url as url, 'image' as type, '' as tags, thumb_url as thumbnail, is_default as `default`
            FROM media
           WHERE base_product_id = %s
  """,

  "subscribed_ids": """
          SELECT subscribed_product_id
            FROM subscribed_product sp
           WHERE sp.base_product_id = %s
           LIMIT 100
  """,

  "product_categories": """
          SELECT c.category_id as id,
                 c.parent_category_id as parent_id,
                 c.category_name as name,
                 c.level as level,
                 c.path as path,
                 c.created_date as created_dt,
                 c.modified_date as updated_dt,
                 c.is_deleted as is_deleted
            FROM category c
      INNER JOIN product_category_mapping pcm
              ON c.category_id = pcm.category_id
           WHERE pcm.base_product_id = %s
           LIMIT 100
  """,

  "parent_categories": """
          SELECT c.category_id, c.parent_category_id as parent_id
            FROM category c
           WHERE c.category_id in (%s)
  """,

  "categories": """
          SELECT c.category_id as id,
                 c.parent_category_id as parent_id,
                 c.category_name as name,
                 c.level as level,
                 c.path as path,
                 c.created_date as created_dt,
                 c.modified_date as updated_dt,
                 c.is_deleted as is_deleted
            FROM category c
           WHERE c.category_id in (%s)
  """,

  "subscribed_product": """
          SELECT sp.subscribed_product_id as id,
                 sp.store_id as store_id,
                 sp.store_price as seller_indicated_price,
                 case when sp.transfer_price > 0 then 100*(sp.store_offer_price - sp.transfer_price)/sp.transfer_price else 0.0 end as take_rate,
                 sp.store_offer_price as store_offer_price,
                 case when sp.store_price > 0 then 100*(sp.store_price - sp.store_offer_price)/sp.store_price else 0.0 end as default_discount_percent,
                 sp.bazaar_price as bazaar_price,
                 sp.bazaar_date_from as bazaar_price_valid_from,
                 sp.bazaar_date_to as bazaar_price_valid_thru,
                 case when sp.store_price > 0 then 100*(sp.store_price - sp.bazaar_price)/sp.store_price else 0.0 end as bazaar_discount_percent,
                 sp.weight,
                 sp.length,
                 sp.width,
                 sp.height,
                 sp.status,
                 sp.created_date as created_dt,
                 sp.modified_date as updated_dt,
                 sp.is_deleted,
                 sp.sku, 
                 sp.quantity as quantity_available,
                 sp.is_cod,
                 sp.subscribe_shipping_charge,
                 sp.campaign_flag,
                 sp.campaign_from,
                 sp.campaign_to,
                 sp.transfer_price,
                 s.store_code,
                 s.store_name,
                 s.store_details,
                 s.store_logo,
                 s.seller_name,
                 s.business_address,
                 s.mobile_numbers,
                 s.telephone_numbers,
                 s.visible,
                 s.meta_title,
                 s.meta_keywords,
                 s.meta_description,
                 s.customer_value,
                 s.chat_id,
                 s.email,
                 s.status as store_status,
                 s.vtiger_status,
                 s.vtiger_accountid as vtiger_account_id,
                 s.created_date as store_created_dt,
                 s.modified_date as store_modified_dt,
                 s.is_deleted as store_is_deleted,
                 s.tagline as tagline,
                 s.store_api_key,
                 s.seller_mailer_flag,
                 s.buyer_mailer_flag,
                 s.store_shipping_charge,
                 s.is_active_valid,
                 sp.vat_per as vat_percent,
                 sp.cst_per as cst_percent
            FROM subscribed_product sp
      INNER JOIN store s
              ON sp.store_id = s.store_id
           WHERE sp.base_product_id = %s
  """,


  "cantorish_base_delta_fetch": """
          SELECT bpl.base_product_id, max(log_id) as log_id
            FROM base_product_logs bpl
           WHERE log_id > %s
             AND log_id <= %s
        GROUP BY bpl.base_product_id
  """,

  "cantorish_base_delta_merge": """
     INSERT INTO mpdm_status (base_product_id, source_base_log_id, bucket)
          VALUES %s
ON DUPLICATE KEY UPDATE source_base_log_id = VALUES(source_base_log_id)
  """,

  "max_source_base_log_id": """
          SELECT max(log_id) as max_id
            FROM base_product_logs
  """,

  "last_target_base_log_id": """
          SELECT log_id 
            FROM mpdm_base_bookmark
           WHERE id IN (          SELECT max(id)
                                    FROM mpdm_base_bookmark
                       )

  """,

  "cantorish_base_bookmark_insert": """
     INSERT INTO mpdm_base_bookmark(log_id, recs, time_ms) 
          VALUES (%s, %s, %s)
  """,


  "cantorish_subscribed_delta_fetch": """
          SELECT base_product_id, max(log_id) as log_id
            FROM (          SELECT spl.new_base_product_id as base_product_id, max(spl.log_id) as log_id 
                              FROM subscribed_product_logs spl
                             WHERE log_id > %s
                               AND log_id <= %s
                               AND spl.new_base_product_id is not null
                          GROUP BY spl.new_base_product_id
                             UNION
                            SELECT spl.old_base_product_id as base_product_id, max(spl.log_id) as log_id
                              FROM subscribed_product_logs spl
                             WHERE log_id > %s
                               AND log_id <= %s
                               AND spl.old_base_product_id is not null
                          GROUP BY spl.old_base_product_id
                 ) a
        GROUP BY base_product_id;
  """,

  "cantorish_subscribed_delta_merge": """
     INSERT INTO mpdm_status (base_product_id, source_subscribed_log_id, bucket)
          VALUES %s
ON DUPLICATE KEY UPDATE source_subscribed_log_id = VALUES(source_subscribed_log_id)
  """,

  "max_source_subscribed_log_id": """
          SELECT max(log_id) as max_id
            FROM subscribed_product_logs
  """,

  "last_target_subscribed_log_id": """
          SELECT log_id 
            FROM mpdm_subscribed_bookmark
           WHERE id IN (          SELECT max(id)
                                    FROM mpdm_subscribed_bookmark
                       )

  """,

  "cantorish_subscribed_bookmark_insert": """
     INSERT INTO mpdm_subscribed_bookmark(log_id, recs, time_ms) 
          VALUES (%s, %s, %s)
  """,




  "product_id_fetch": """
          SELECT base_product_id as id, source_base_log_id, source_subscribed_log_id
            FROM mpdm_status
           WHERE %s = MOD(bucket, %s) 
             AND (source_base_log_id > target_base_log_id
                  OR source_subscribed_log_id > target_subscribed_log_id)
        ORDER BY base_product_id desc
  """,

  "product_success_merge": """
     INSERT INTO mpdm_status (base_product_id, source_base_log_id, source_subscribed_log_id, target_base_log_id, target_subscribed_log_id)
          VALUES %s
ON DUPLICATE KEY UPDATE target_base_log_id = VALUES(target_base_log_id), target_subscribed_log_id = VALUES(target_subscribed_log_id)
  """,

  "product_failure_merge": """
     INSERT INTO mpdm_status (base_product_id, last_error)
          VALUES %s
ON DUPLICATE KEY UPDATE last_error = VALUES(last_error)
  """

}
