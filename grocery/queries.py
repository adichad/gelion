

queryMap = {

  "grocery_variant": """
          SELECT 
                 v.variant_id AS variant_id
                ,v.variant_code AS variant_code
                ,v.title AS variant_title
                ,v.modify_date AS variant_modified_dt
                ,v.status AS variant_status
                ,p.product_id AS product_id
                ,p.codesufx AS product_code
                ,p.name AS product_name
                ,p.status AS product_status
                ,b.name AS brand_name
                ,b.brand_code AS brand_code
                ,b.status AS brand_status
            FROM Variant v
      INNER JOIN Ref_Product_Variant rpv
              ON v.variant_id = rpv.ref_m_variant_id
      INNER JOIN Products p
              ON rpv.ref_m_product_id = p.product_id
 LEFT OUTER JOIN Brands b
              ON p.brand = b.brands_id
           WHERE v.variant_id in (%s)
  """,

  "grocery_category": """
          SELECT
                 mc.category_id AS id
                ,ltrim(rtrim(substring(name, 0, case when charindex('1', name)>0 then charindex('1',name) else len(name)+1 end))) AS name
                ,mc.status AS status
                ,lower(mc.ParentName) AS parent_name
            FROM M_Category mc
      INNER JOIN Ref_Category_Product rcp
              ON mc.category_id = rcp.ref_m_category_id
           WHERE rcp.ref_m_product_id in (%s)
  """,

  "grocery_media": """
          SELECT
                 m.full_path as media_url
                ,m.thumbnail_full_path as thumbnail_url
            FROM Multimedia m
           WHERE m.entity_id in (%s)
        ORDER BY m.multimedia_id desc
  """,

  "grocery_item": """
          SELECT
                 i.item_id AS id
                ,i.ItemCode AS code
                ,i.Shipping_Charges AS shipping_charge
                ,i.Deals AS deal_count
                ,i.DealText AS deal_text
                ,i.isMin AS is_min
                ,i.status AS status
                ,i.VendorId AS vendor_id
                ,i.BrandDeal AS brand_deal
                ,i.SastaOffer AS sasta_offer
                ,i.AvailabilityMin AS availability_min
                ,i.AvailabilityMax AS availability_max
                ,i.customer_price AS customer_price
                ,i.display_price
                ,CAST(CAST(CASE WHEN i.display_price > 0 THEN ((i.display_price - i.customer_price) * 100 / i.display_price) ELSE 0 END AS DECIMAL(8,1)) AS FLOAT) AS discount_percent
                ,imvpla.average_price as pla_average_price
                ,imvpla.display_price as pla_display_price
                ,imvpla.customer_price as pla_customer_price
                ,imvpla.VendorId as pla_vendor_id
                ,imvpla.Status as pla_status
                ,i.update_date as modified_dt
                ,l.status as login_status
                ,mz.Id as zone_id
                ,mz.ZonalCode as zone_code
                ,mz.Status as zone_status
                ,mz.PostalCode as postal_codes
                ,mz.Area as areas
                ,mz.CityName as city
                ,mz.Statename as state
                ,mz.CountryName as country
                ,count(distinct ref_order_id) as order_count
                ,coalesce(sum(TotalPrice), 0.0) as gsv
                ,coalesce(sum(quantity), 0) as quantity_sold
            FROM Items i
      INNER JOIN Logins l
              ON i.created_by = l.login_id
      INNER JOIN M_ZoneCode mz
              ON l.ZonalCodeID = mz.Id
 LEFT OUTER JOIN ItemsMinValuePLA imvpla
              ON i.item_id = imvpla.item_id
             AND coalesce(imvpla.Sno, 0) > 0
 LEFT OUTER JOIN mstCart c
              ON c.ref_itemId = i.item_id
             AND c.AddedOn > DATEADD(month, -2, GETDATE())
           WHERE i.ref_product_id in (%s)
        GROUP BY i.item_id
                ,i.ItemCode
                ,i.Shipping_Charges
                ,i.Deals
                ,i.DealText
                ,i.isMin
                ,i.status
                ,i.VendorId
                ,i.BrandDeal
                ,i.SastaOffer
                ,i.AvailabilityMin
                ,i.AvailabilityMax
                ,i.customer_price
                ,i.display_price
                ,imvpla.average_price
                ,imvpla.display_price
                ,imvpla.customer_price
                ,imvpla.VendorId
                ,imvpla.Status
                ,i.update_date
                ,mz.Id
                ,mz.ZonalCode
                ,mz.Status
                ,mz.PostalCode
                ,mz.Area
                ,mz.CityName
                ,mz.Statename
                ,mz.CountryName
  """,

  "grocery_delta_fetch": """
          SELECT variant_id, max(log_id) as last_log_id
            FROM dbo.console_search_indexing_log
           WHERE log_id > %s
             AND log_id <= %s
        GROUP BY variant_id
  """,

  "grocery_delta_merge": """
     INSERT INTO grocery_status (variant_id, source_log_id, bucket)
          VALUES %s
ON DUPLICATE KEY UPDATE source_log_id = VALUES(source_log_id)
  """,

  "max_source_log_id": """
          SELECT max(log_id) as max_id
            FROM dbo.console_search_indexing_log
  """,

  "last_target_log_id": """
          SELECT log_id 
            FROM grocery_bookmark
           WHERE id IN (          SELECT max(id)
                                    FROM grocery_bookmark
                       )

  """,

  "grocery_bookmark_insert": """
     INSERT INTO grocery_bookmark(log_id, recs, time_ms) 
          VALUES (%s, %s, %s)
  """,

  "grocery_id_fetch": """
          SELECT variant_id, source_log_id
            FROM grocery_status
           WHERE %s = MOD(bucket, %s) 
             AND source_log_id > target_log_id
  """,

  "grocery_success_merge": """
     INSERT INTO grocery_status (variant_id, source_log_id, target_log_id)
          VALUES %s
ON DUPLICATE KEY UPDATE target_log_id = VALUES(target_log_id)
  """,

  "grocery_failure_merge": """
     INSERT INTO grocery_status (variant_id, source_log_id, last_error)
          VALUES %s
ON DUPLICATE KEY UPDATE last_error = VALUES(last_error)
  """

}
