

queryMap = {

  "grocery_variant": """
          SELECT 
                 v.variant_id AS variant_id
                ,v.variant_code AS variant_code
                ,v.title AS variant_title
                ,v.notes AS variant_notes
                ,v.full_description as variant_full_description
                ,v.modify_date AS variant_modified_dt
                ,v.status AS variant_status
                ,p.product_id AS product_id
                ,p.codesufx AS product_code
                ,p.name AS product_name
                ,p.status AS product_status
                ,b.name AS brand_name
                ,b.brand_code AS brand_code
                ,b.status AS brand_status
                ,p.brand as brand_id
                ,cast ((1000001 - coalesce(mbp.priority, 1000001)) as FLOAT) as brand_priority
            FROM Variant v
      INNER JOIN Ref_Product_Variant rpv
              ON v.variant_id = rpv.ref_m_variant_id
      INNER JOIN Products p
              ON rpv.ref_m_product_id = p.product_id
 LEFT OUTER JOIN Brands b
              ON p.brand = b.brands_id
 LEFT OUTER JOIN m_brandpriority mbp
              ON p.brand = mbp.brandid
             AND mbp.categoryid IS NULL
             AND mbp.zoneid IS NULL
             AND mbp.areaid IS NULL
           WHERE v.variant_id in (%s)
  """,
  
  "grocery_category": """
          SELECT mc.category_id as id
                ,ltrim(rtrim(substring(mc.name, 0, case when charindex('1', mc.name)>0 then charindex('1', mc.name) else len(mc.name)+1 end))) AS name
                ,case when mc.parent_id = mc.category_id then 0 else mc.parent_id end as parent_id
                ,mc.status as status
                ,lower(mc.parentname) as parent_name
                ,mc.isnav as isnav
            FROM M_Category mc
      INNER JOIN Ref_Category_Product rcp
              ON mc.category_id = rcp.ref_m_category_id
           WHERE rcp.ref_m_product_id in (%s)
  """,

  "grocery_category_hierarchy": """
WITH category_hierarchy(id, name, parent_id, status, parent_name, level, isnav)
AS
(
          SELECT mc.category_id as id
                ,ltrim(rtrim(substring(mc.name, 0, case when charindex('1', mc.name)>0 then charindex('1', mc.name) else len(mc.name)+1 end))) AS name
                ,case when mc.parent_id = mc.category_id then 0 else mc.parent_id end as parent_id
                ,mc.status as status
                ,lower(mc.parentname) as parent_name
                ,CAST (substring(ltrim(rtrim(mc.filterby6)), 2, len(mc.filterby6)+1) as INT) as level
                ,mc.isnav as isnav
            FROM M_Category mc
      INNER JOIN Ref_Category_Product rcp
              ON mc.category_id = rcp.ref_m_category_id
           WHERE rcp.ref_m_product_id in (%s)
             AND mc.ParentName = 'Fmcg'
             AND mc.status = 1
       UNION ALL
          SELECT mc.category_id as id
                ,ltrim(rtrim(substring(mc.name, 0, case when charindex('1', mc.name)>0 then charindex('1',mc.name) else len(mc.name)+1 end))) AS name
                ,case when mc.parent_id = mc.category_id then 0 else mc.parent_id end as parent_id
                ,mc.status as status
                ,lower(mc.parentname) as parent_name
                ,CAST (substring(ltrim(rtrim(mc.filterby6)), 2, len(mc.filterby6)+1) as INT) as level
                ,mc.isnav as isnav
            FROM M_Category mc
      INNER JOIN category_hierarchy ch
              ON ch.parent_id = mc.category_id
             AND mc.status = 1
)
          SELECT ch.id
                ,name
                ,parent_id
                ,status
                ,parent_name
                ,isnav
                ,level
                ,CAST((1000001 - COALESCE(mbp1.priority, 1000001)) AS FLOAT) as category_priority
                ,CAST((1000001 - COALESCE(mbp2.priority, 1000001)) AS FLOAT) as category_brand_priority
            FROM category_hierarchy ch
 LEFT OUTER JOIN m_brandpriority mbp1
              ON mbp1.categoryid = ch.id
             AND mbp1.brandid is null
             AND mbp1.zoneid is null
             AND mbp1.areaid is null
 LEFT OUTER JOIN m_brandpriority mbp2
              ON mbp2.categoryid = ch.id
             AND mbp2.brandid = %s
             AND mbp2.zoneid is null
             AND mbp2.areaid is null
        ORDER BY level ASC
          OPTION (maxrecursion 0)
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
                ,cast(ltrim(rtrim(i.Deals)) as INT) AS deal_count
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
                ,i.cost as transfer_price
                ,i.BillingPrice as billing_price
                ,(i.customer_price - (case when i.BillingPrice < i.cost then i.BillingPrice else i.cost end)) as margin
                ,CAST(CAST(CASE WHEN i.display_price > 0 THEN ((i.display_price - i.customer_price) * 100 / i.display_price) ELSE 0 END AS DECIMAL(8,1)) AS FLOAT) AS discount_percent
                ,imvpla.average_price as pla_average_price
                ,imvpla.display_price as pla_display_price
                ,imvpla.customer_price as pla_customer_price
                ,(imvpla.customer_price - (case when i.BillingPrice < i.cost then i.BillingPrice else i.cost end)) as pla_margin
                ,CAST(CAST(CASE WHEN imvpla.display_price > 0 THEN ((imvpla.display_price - imvpla.customer_price) * 100 / imvpla.display_price) ELSE 0 END AS DECIMAL(8,1)) AS FLOAT) AS pla_discount_percent
                ,imvpla.VendorId as pla_vendor_id
                ,imvpla.Status as pla_status
                ,i.update_date as modified_dt
                ,ilc.maxqty as max_quantity
                ,l.status as login_status
                ,mz.Id as zone_id
                ,mz.ZonalCode as zone_code
                ,mz.Status as zone_status
                ,mz.PostalCode as postal_codes
                ,STUFF((SELECT ','+convert(varchar(10), geoserviceareaid) FROM tblArea WHERE refzoneid=mz.Id AND status=1 FOR XML PATH('')) , 1 , 1 , '' ) as areas
                ,STUFF((SELECT ','+convert(varchar(10), DeliveryDays) FROM DeliveryDays WHERE ItemId=i.item_id AND status=1 FOR XML PATH('')) , 1 , 1 , '' ) as delivery_days
                ,mz.CityName as city
                ,mz.Statename as state
                ,mz.gid as zone_gid
                ,mz.CountryName as country
                ,mc.city_id
                ,mc.status as city_status
                ,mc.isfmcg as city_is_fmcg
                ,ms.status as state_status
                ,ms.isfmcg as state_is_fmcg
                ,mco.status as country_status
                ,count(distinct ref_order_id) as order_count
                ,coalesce(sum(TotalPrice), 0.0) as gsv
                ,coalesce(sum(quantity), 0) as quantity_sold
            FROM Items i
      INNER JOIN Logins l
              ON i.created_by = l.login_id
      INNER JOIN M_ZoneCode mz
              ON l.ZonalCodeID = mz.Id
      INNER JOIN M_City mc
              ON mz.p_ref_m_city_id = mc.city_id
      INNER JOIN M_States ms
              ON mz.p_ref_m_state_id = ms.state_id
      INNER JOIN M_Country mco
              ON mz.ref_m_country_id = mco.country_id
 LEFT OUTER JOIN ItemsMinValuePLA imvpla
              ON i.item_id = imvpla.item_id
             AND coalesce(imvpla.Sno, 0) > 0
 LEFT OUTER JOIN mstCart c
              ON c.ref_itemId = i.item_id
             AND c.AddedOn > DATEADD(month, -2, GETDATE())
 LEFT OUTER JOIN M_ItemsLevelCapping ilc
              ON i.item_id = ilc.item_id
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
                ,i.cost
                ,i.BillingPrice
                ,(i.customer_price - (case when i.BillingPrice < i.cost then i.BillingPrice else i.cost end)) 
                ,imvpla.average_price
                ,imvpla.display_price
                ,imvpla.customer_price
                ,imvpla.VendorId
                ,imvpla.Status
                ,(imvpla.customer_price - (case when i.BillingPrice < i.cost then i.BillingPrice else i.cost end))
                ,CAST(CAST(CASE WHEN imvpla.display_price > 0 THEN ((imvpla.display_price - imvpla.customer_price) * 100 / imvpla.display_price) ELSE 0 END AS DECIMAL(8,1)) AS FLOAT)
                ,i.update_date
                ,ilc.maxqty
                ,l.status
                ,mz.Id
                ,mz.ZonalCode
                ,mz.Status
                ,mz.PostalCode
                ,mz.Area
                ,mz.GID
                ,mz.CityName
                ,mz.Statename
                ,mz.CountryName
                ,mc.city_id
                ,mc.status
                ,mc.isfmcg
                ,ms.status
                ,ms.isfmcg
                ,mco.status
  """,

  "grocery_storefront": """
          select s.id
                ,s.title
                ,s.UrlSlug as url_slug
                ,s.startdate
                ,s.enddate
                ,case when s.status = 1 then 1 else 0 end as status
                ,case when sim.status = 1 then 1 else 0 end as mapping_status
                ,sim.DealText as deal_text
            from m_storefront s
      inner join m_storefront_items_mapping sim
              on s.id = sim.storefront_id
             and sim.item_id in (%s)
  """,

  "grocery_orders": """
         select oi.order_id,
                 user_id,
                 status_code,
                 updated_on,
                 geo_id,
                 parent_order_id,
                 total_offer_price as total_offer_price,
                 total_payble_amount as total_order_value,
                 min(oi.quantity) as quantity,
                 max(oi.offer_price) as offer_price,
                 group_concat(distinct oi2.item_id) as item_set
            from `order` o
      inner join status s
              on o.status_id = s.id
      inner join order_item oi
              on oi.order_id = o.order_reference_id
      inner join order_item oi2
              on oi2.order_id = o.order_reference_id
           where oi.item_id = %s
        group by order_id
  """,

  "grocery_orders_delta_fetch": """
          SELECT item_id, 
                 max(updated_on) as order_item_updated_on
            FROM `order` o
      INNER JOIN order_item oi
              ON o.order_reference_id = oi.order_id
           WHERE updated_on >= %s and updated_on < %s
        GROUP BY item_id
        ORDER BY order_item_updated_on ASC
  """,

  "grocery_item_variant_fetch": """
          SELECT ref_product_id as variant_id,
                 item_id
            FROM Items i
           WHERE i.item_id in (%s)
  """,

  "grocery_orders_delta_merge": """
     INSERT INTO grocery_status (variant_id, source_order_last_updated_on, bucket)
          VALUES %s
ON DUPLICATE KEY UPDATE source_order_last_updated_on = VALUES(source_order_last_updated_on)
  """,

  "grocery_orders_bookmark_insert": """
     INSERT INTO grocery_orders_bookmark(updated_on, recs, time_ms) 
          VALUES (%s, %s, %s)
  """,

  "orders_last_target_updated_on": """
          SELECT updated_on as last_updated_on
            FROM grocery_orders_bookmark
           WHERE id IN (SELECT max(id) from grocery_orders_bookmark)
  """,

  "orders_max_source_updated_on": """
          SELECT max(updated_on) as last_updated_on
            FROM `order` o
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
          SELECT variant_id, source_log_id, source_order_last_updated_on
            FROM grocery_status
           WHERE %s = MOD(bucket, %s) 
             AND (source_log_id > target_log_id OR source_order_last_updated_on > target_order_last_updated_on)
  """,

  "grocery_success_merge": """
     INSERT INTO grocery_status (variant_id, source_log_id, source_order_last_updated_on, target_log_id, target_order_last_updated_on)
          VALUES %s
ON DUPLICATE KEY UPDATE target_log_id = VALUES(target_log_id), target_order_last_updated_on = VALUES(target_order_last_updated_on)
  """,

  "grocery_failure_merge": """
     INSERT INTO grocery_status (variant_id, source_log_id, last_error)
          VALUES %s
ON DUPLICATE KEY UPDATE last_error = VALUES(last_error)
  """

}
