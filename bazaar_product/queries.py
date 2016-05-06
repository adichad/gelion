

queryMap = {
  "base_product": """
          SELECT p.product_id,
                 p.base_product_id,
                 p.model,
                 p.sku,
                 p.upc,
                 p.ean,
                 p.jan,
                 p.isbn,
                 p.mpn,
                 p.quantity,
                 p.stock_status_id,
                 ss.name as stock_status,
                 p.image,
                 p.manufacturer_id,
                 p.shipping,
                 p.price,
                 p.points,
                 p.tax_class_id,
                 p.date_available,
                 p.weight,
                 p.weight_class_id,
                 p.length,
                 p.width,
                 p.height,
                 p.length_class_id,
                 pgt.pg_layout,
                 p.subtract,
                 p.minimum,
                 p.sort_order,
                 p.status, 
                 p.date_added,
                 p.date_modified,
                 p.viewed,
                 p.pgvisibility,
                 p.pgprice_from,
                 p.pgprice_to,
                 pd.language_id,
                 pd.name,
                 pd.description,
                 pd.meta_description,
                 pd.meta_keyword,
                 pd.tag,
                 pd.tag_title,
                 pd.key_feature,
                 AVG(r.rating) AS avg_rating, 
                 COUNT(r.review_id) AS review_count,
                 MAX(r.date_modified) AS max_review_modified_dt,
                 GROUP_CONCAT(DISTINCT pi.image ORDER BY pi.sort_order ASC, pi.image ASC) as images
            FROM oc_product p
 LEFT OUTER JOIN oc_product_description pd
              ON p.product_id = pd.product_id
 LEFT OUTER JOIN oc_review r
              ON p.product_id = r.product_id
             AND r.status = 1
 LEFT OUTER JOIN oc_stock_status ss
              ON ss.stock_status_id = p.stock_status_id
 LEFT OUTER JOIN oc_product_image pi
              ON p.product_id = pi.product_id
 LEFT OUTER JOIN oc_product_grouped_type pgt
              ON p.product_id = pgt.product_id
           WHERE p.product_id in (%s)
             AND pd.language_id = 1
             AND p.model = 'grouped'
    --       AND cd.language_id = 1
        GROUP BY p.product_id
  """,

  "subscribed_ids": """
          SELECT DISTINCT pg.grouped_id
            FROM oc_product_grouped pg
      INNER JOIN oc_product p
              ON pg.grouped_id = p.product_id
           WHERE pg.product_id = %s
             AND p.status = 1
           LIMIT 100
  """,

  "product_categories": """
          SELECT cd.*, c.parent_id, c.image, c.sort_order, c.top, c.status, c.column
            FROM oc_product_to_category ptc
      INNER JOIN oc_category c
              ON ptc.category_id = c.category_id
      INNER JOIN oc_category_description cd
              ON c.category_id = cd.category_id
           WHERE ptc.product_id = %s
           LIMIT 100
  """,

  "parent_categories": """
          SELECT c.category_id, c.parent_id
            FROM oc_category c
           WHERE c.category_id in (%s)
  """,

  "categories": """
          SELECT cd.*, c.parent_id, c.image, c.sort_order, c.top, c.status, c.column
            FROM oc_category c
      INNER JOIN oc_category_description cd
              ON c.category_id = cd.category_id
           WHERE c.category_id in (%s)
  """,

  "base_product_options": """
          SELECT o.option_id as id, od.name, po.option_value as value, o.type, o.sort_order
            FROM oc_product_option po
      INNER JOIN oc_option o
              ON po.option_id = o.option_id
      INNER JOIN oc_option_description od
              ON o.option_id = od.option_id
           WHERE po.product_id = %s
  """,

  "base_product_attributes": """
          SELECT ad.attribute_id as id, trim(ad.name) as name, trim(pa.text) as value
            FROM oc_product_attribute pa
 LEFT OUTER JOIN oc_attribute_description ad
              ON pa.attribute_id = ad.attribute_id
           WHERE pa.product_id = %s;
  """,

  "base_product_stores": """
          SELECT s.store_id as id, s.name, s.ssl
            FROM oc_product_to_store pts
      INNER JOIN oc_store s
              ON (pts.store_id = s.store_id)
           WHERE pts.product_id = %s
  """,

  "base_product_reviews": """
          SELECT review_id as id, customer_id, author, text, rating, date_added, date_modified, store
            FROM oc_review
           WHERE product_id = %s
             AND status = 1
  """,

  "subscribed_product": """
          SELECT p.product_id,
                 p.model,
                 p.quantity,
                 p.stock_status_id,
                 ss.name as stock_status,
                 p.image,
                 p.shipping,
                 p.price,
                 p.points,
                 p.tax_class_id,
                 p.date_available,
                 p.subtract,
                 p.minimum,
                 p.sort_order,
                 p.status as status, 
                 p.date_added,
                 p.date_modified,
                 p.viewed,
                 p.pgvisibility,
                 p.pgprice_from,
                 p.pgprice_to,
                 pg.customer_id as seller_id,
                 pg.seller_id as crm_seller_id,
                 pg.subscribed_product_id,
                 ctc.companyname as seller_name,
                 a.firstname as seller_firstname,
                 a.lastname as seller_lastname,
                 a.address_1 as seller_address_1,
                 a.address_2 as seller_address_2,
                 a.city as seller_city,
                 a.postcode as seller_postcode,
                 c.name as seller_country,
                 c.iso_code_2 as seller_country_code_iso_2,
                 c.iso_code_3 as seller_country_code_iso_3,
                 z.name as seller_zone,
                 z.code as seller_zone_code,
                 GROUP_CONCAT(DISTINCT pi.image ORDER BY pi.sort_order ASC, pi.image ASC) as images,
                 count(distinct op.order_id) as order_count,
                 sum(op.quantity) as order_quantity,
                 sum(op.price) as order_gsv,
                 sum(op.discount) as order_discount_amount,
                 sum(op.lpoints_earned) as order_loyalty_earned,
                 max(op.date_added) as order_last_dt,
                 avg(op.discount_pct) as order_discount_pct_avg
                 
            FROM oc_product p
      INNER JOIN oc_product_grouped pg
              ON p.product_id = pg.grouped_id
 LEFT OUTER JOIN oc_customerpartner_to_customer ctc
              ON pg.customer_id = ctc.customer_id
 LEFT OUTER JOIN oc_stock_status ss
              ON ss.stock_status_id = p.stock_status_id
 LEFT OUTER JOIN oc_address a
              ON pg.customer_id = a.customer_id
 LEFT OUTER JOIN oc_country c
              ON a.country_id = c.country_id
 LEFT OUTER JOIN oc_zone z
              ON a.zone_id = z.zone_id
             AND a.country_id = z.country_id
 LEFT OUTER JOIN (          SELECT opi.product_id,
                                   opi.quantity,
                                   opi.order_id,
                                   opi.price,
                                   opi.discount,
                                   opi.lpoints_earned,
                                   oi.date_added,
                                   CASE WHEN opi.price = 0.0 THEN 0.0 ELSE opi.discount*100.0/(opi.price+opi.discount) end as discount_pct
                              FROM oc_order_product opi
                        INNER JOIN oc_order oi
                                ON opi.order_id = oi.order_id
                               AND oi.date_added > DATE_SUB(NOW(), INTERVAL 2 MONTH)
                             WHERE opi.product_id in (%s)) op
              ON p.product_id = op.product_id
  LEFT OUTER JOIN oc_product_image pi
              ON p.product_id = pi.product_id
           WHERE p.product_id in (%s)
        GROUP BY product_id,
                 model,
                 quantity,
                 stock_status_id,
                 ss.name,
                 shipping,
                 price,
                 points,
                 tax_class_id,
                 date_available,
                 subtract,
                 minimum,
                 sort_order,
                 p.status,
                 p.date_added,
                 p.date_modified,
                 p.viewed,
                 p.pgvisibility,
                 p.pgprice_from,
                 p.pgprice_to,
                 pg.customer_id,
                 pg.seller_id,
                 pg.subscribed_product_id,
                 ctc.companyname,
                 a.firstname,
                 a.lastname,
                 a.address_1,
                 a.address_2,
                 a.city,
                 a.postcode,
                 c.name,
                 c.iso_code_2,
                 c.iso_code_3,
                 z.name,
                 z.code
  """,


  "mpdm_subscribed_product": """
          SELECT coalesce(sc.shipping_charges, sp.subscribe_shipping_charge) as shipping_charge,
                 sp.transfer_price as transfer_price,
                 sp.is_cod as is_cod_apriori
            FROM subscribed_product sp
 LEFT OUTER JOIN shipping_charges sc
              ON sp.subscribed_product_id = sc.subscribed_id
           WHERE sp.subscribed_product_id = %s;
  """,

  "subscribed_product_options": """
          SELECT o.option_id as id, od.name, ovd.name as value, o.type, o.sort_order
            FROM oc_customerpartner_group_option cgo
      INNER JOIN oc_product_grouped pg
              ON pg.grouped_id = cgo.grouped_id
      INNER JOIN oc_option o
              ON cgo.option_id = o.option_id
      INNER JOIN oc_option_description od
              ON cgo.option_id = od.option_id
      INNER JOIN oc_option_value ov
              ON cgo.option_value_id = ov.option_value_id
      INNER JOIN oc_option_value_description ovd
              ON ov.option_value_id = ovd.option_value_id
           WHERE pg.grouped_id = %s
        ORDER BY ov.sort_order ASC
  """,

  "subscribed_product_store_fronts": """
          SELECT sf.store_front_id as id, sf.mpdm_store_front_id as mpdm_id, sfm.subscribed_product_boost as boost, sf.store_front_title as title, sf.status as status, sfm.status as mapping_status
            FROM oc_product_grouped pg
      INNER JOIN oc_store_front_mapping sfm 
              ON pg.subscribed_product_id = sfm.subscribed_product_id
      INNER JOIN oc_store_front sf
              ON sfm.store_front_id = sf.mpdm_store_front_id
           WHERE pg.grouped_id = %s
  """,

  "subscribed_special_price": """
          SELECT price,priority,date_start,date_end,ecflashsale_id,is_bazaar_price 
            FROM oc_product_special ps 
           WHERE ps.product_id = %s 
             AND (ps.date_end = '0000-00-00' OR ps.date_end > NOW()) 
        ORDER BY ps.priority ASC, ps.price ASC 
           LIMIT 100
  """,

  "product_delta_fetch": """
          SELECT pl.product_id, max(date_time) as last_updated_dt
            FROM oc_product_log pl
      INNER JOIN oc_product p
              ON pl.product_id = p.product_id
           WHERE id > %s
             AND id <= %s
        GROUP BY pl.product_id
  """,

  "product_delta_merge": """
     INSERT INTO product_status (product_id, source_dt, bucket)
          VALUES %s
ON DUPLICATE KEY UPDATE source_dt = VALUES(source_dt)
  """,

  "max_source_log_id": """
          SELECT max(id) as max_id
            FROM oc_product_log
  """,

  "last_target_log_id": """
          SELECT log_id 
            FROM product_bookmark
           WHERE id IN (          SELECT max(id)
                                    FROM product_bookmark
                       )

  """,

  "product_bookmark_insert": """
     INSERT INTO product_bookmark(log_id, recs, time_ms) 
          VALUES (%s, %s, %s)
  """,

  "product_id_fetch": """
          SELECT product_id, source_dt
            FROM product_status
           WHERE %s = MOD(bucket, %s) 
             AND source_dt > target_dt
  """,

  "product_success_merge": """
     INSERT INTO product_status (product_id, source_dt, target_dt)
          VALUES %s
ON DUPLICATE KEY UPDATE target_dt = VALUES(target_dt)
  """,

  "product_failure_merge": """
     INSERT INTO product_status (product_id, source_dt, last_error)
          VALUES %s
ON DUPLICATE KEY UPDATE last_error = VALUES(last_error)
  """

}
