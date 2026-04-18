WITH
dwh_delta AS ( -- определяем, какие данные были изменены в витрине или добавлены в DWH. Формируем дельту изменений
    SELECT     
            dcs.customer_id AS customer_id,
            dcs.customer_name AS customer_name,
            dcs.customer_address AS customer_address,
            dcs.customer_birthday AS customer_birthday,
            dcs.customer_email AS customer_email,
            fo.order_id AS order_id,
            dc.craftsman_id AS craftsman_id,
            dp.product_id AS product_id,
            dp.product_price AS product_price,
            dp.product_type AS product_type,
            fo.order_completion_date - fo.order_created_date AS diff_order_date, 
            fo.order_status AS order_status,
            TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period,
            crd.customer_id AS exist_customer_id,
            dcs.load_dttm AS customers_load_dttm,
            dc.load_dttm AS craftsman_load_dttm,
            dp.load_dttm AS products_load_dttm
      FROM dwh.f_order fo 
        INNER JOIN dwh.d_customer dcs 
           ON fo.customer_id = dcs.customer_id 
        INNER JOIN dwh.d_craftsman dc 
           ON fo.craftsman_id = dc.craftsman_id 
        INNER JOIN dwh.d_product dp 
           ON fo.product_id = dp.product_id 
        LEFT JOIN dwh.customer_report_datamart crd 
           ON dcs.customer_id = crd.customer_id
     WHERE (fo.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
           (dcs.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
           (dc.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
           (dp.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart))
),

dwh_update_delta AS ( -- делаем выборку заказчиков, по которым были изменения в DWH
    SELECT DISTINCT
           dd.exist_customer_id AS customer_id
      FROM dwh_delta dd 
     WHERE dd.exist_customer_id IS NOT NULL        
),

dwh_delta_insert_result AS ( -- расчёт витрины по новым данным
    SELECT  
            t2.customer_id AS customer_id,
            t2.customer_name AS customer_name,
            t2.customer_address AS customer_address,
            t2.customer_birthday AS customer_birthday,
            t2.customer_email AS customer_email,
            t2.customer_money AS customer_money,
            t2.platform_money AS platform_money,
            t2.count_order AS count_order,
            t2.avg_price_order AS avg_price_order,
            t2.median_time_order_completed AS median_time_order_completed,
            t3.product_type AS top_product_category,
            t4.craftsman_id AS top_craftsman_id,
            t2.count_order_created AS count_order_created,
            t2.count_order_in_progress AS count_order_in_progress,
            t2.count_order_delivery AS count_order_delivery,
            t2.count_order_done AS count_order_done,
            t2.count_order_not_done AS count_order_not_done,
            t2.report_period AS report_period
      FROM (
            SELECT
                    t1.customer_id AS customer_id,
                    t1.customer_name AS customer_name,
                    t1.customer_address AS customer_address,
                    t1.customer_birthday AS customer_birthday,
                    t1.customer_email AS customer_email,
                    SUM(t1.product_price) AS customer_money,
                    SUM(t1.product_price) * 0.1 AS platform_money,
                    COUNT(t1.order_id) AS count_order,
                    AVG(t1.product_price) AS avg_price_order,
                    PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY t1.diff_order_date) AS median_time_order_completed,
                    SUM(CASE WHEN t1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
                    SUM(CASE WHEN t1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress,
                    SUM(CASE WHEN t1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery,
                    SUM(CASE WHEN t1.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done,
                    SUM(CASE WHEN t1.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
                    t1.report_period AS report_period
              FROM dwh_delta t1
             WHERE t1.exist_customer_id IS NULL
             GROUP BY t1.customer_id,
                      t1.customer_name,
                      t1.customer_address,
                      t1.customer_birthday,
                      t1.customer_email,
                      t1.report_period
      ) t2
        INNER JOIN (
            SELECT
                    x.customer_id,
                    x.report_period,
                    x.product_type
              FROM (
                    SELECT
                            dd.customer_id,
                            dd.report_period,
                            dd.product_type,
                            COUNT(dd.product_id) AS count_product,
                            ROW_NUMBER() OVER(
                                PARTITION BY dd.customer_id, dd.report_period
                                ORDER BY COUNT(dd.product_id) DESC
                            ) AS rn
                      FROM dwh_delta dd
                     WHERE dd.exist_customer_id IS NULL
                     GROUP BY dd.customer_id,
                              dd.report_period,
                              dd.product_type
              ) x
             WHERE x.rn = 1
        ) t3
          ON t2.customer_id = t3.customer_id
         AND t2.report_period = t3.report_period
        INNER JOIN (
            SELECT
                    y.customer_id,
                    y.report_period,
                    y.craftsman_id
              FROM (
                    SELECT
                            dd.customer_id,
                            dd.report_period,
                            dd.craftsman_id,
                            COUNT(dd.order_id) AS count_craftsman_order,
                            ROW_NUMBER() OVER(
                                PARTITION BY dd.customer_id, dd.report_period
                                ORDER BY COUNT(dd.order_id) DESC
                            ) AS rn
                      FROM dwh_delta dd
                     WHERE dd.exist_customer_id IS NULL
                     GROUP BY dd.customer_id,
                              dd.report_period,
                              dd.craftsman_id
              ) y
             WHERE y.rn = 1
        ) t4
          ON t2.customer_id = t4.customer_id
         AND t2.report_period = t4.report_period
     ORDER BY t2.report_period
),

dwh_delta_update_result AS ( -- перерасчёт для существующих записей витрины
    SELECT  
            t2.customer_id AS customer_id,
            t2.customer_name AS customer_name,
            t2.customer_address AS customer_address,
            t2.customer_birthday AS customer_birthday,
            t2.customer_email AS customer_email,
            t2.customer_money AS customer_money,
            t2.platform_money AS platform_money,
            t2.count_order AS count_order,
            t2.avg_price_order AS avg_price_order,
            t2.median_time_order_completed AS median_time_order_completed,
            t3.product_type AS top_product_category,
            t4.craftsman_id AS top_craftsman_id,
            t2.count_order_created AS count_order_created,
            t2.count_order_in_progress AS count_order_in_progress,
            t2.count_order_delivery AS count_order_delivery,
            t2.count_order_done AS count_order_done,
            t2.count_order_not_done AS count_order_not_done,
            t2.report_period AS report_period
      FROM (
            SELECT
                    t1.customer_id AS customer_id,
                    t1.customer_name AS customer_name,
                    t1.customer_address AS customer_address,
                    t1.customer_birthday AS customer_birthday,
                    t1.customer_email AS customer_email,
                    SUM(t1.product_price) AS customer_money,
                    SUM(t1.product_price) * 0.1 AS platform_money,
                    COUNT(t1.order_id) AS count_order,
                    AVG(t1.product_price) AS avg_price_order,
                    PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY t1.diff_order_date) AS median_time_order_completed,
                    SUM(CASE WHEN t1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
                    SUM(CASE WHEN t1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress,
                    SUM(CASE WHEN t1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery,
                    SUM(CASE WHEN t1.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done,
                    SUM(CASE WHEN t1.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
                    t1.report_period AS report_period
              FROM (
                    SELECT
                            dcs.customer_id AS customer_id,
                            dcs.customer_name AS customer_name,
                            dcs.customer_address AS customer_address,
                            dcs.customer_birthday AS customer_birthday,
                            dcs.customer_email AS customer_email,
                            fo.order_id AS order_id,
                            dc.craftsman_id AS craftsman_id,
                            dp.product_id AS product_id,
                            dp.product_price AS product_price,
                            dp.product_type AS product_type,
                            fo.order_completion_date - fo.order_created_date AS diff_order_date,
                            fo.order_status AS order_status,
                            TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period
                      FROM dwh.f_order fo 
                        INNER JOIN dwh.d_customer dcs 
                           ON fo.customer_id = dcs.customer_id 
                        INNER JOIN dwh.d_craftsman dc 
                           ON fo.craftsman_id = dc.craftsman_id 
                        INNER JOIN dwh.d_product dp 
                           ON fo.product_id = dp.product_id
                        INNER JOIN dwh_update_delta ud 
                           ON fo.customer_id = ud.customer_id
              ) t1
             GROUP BY t1.customer_id,
                      t1.customer_name,
                      t1.customer_address,
                      t1.customer_birthday,
                      t1.customer_email,
                      t1.report_period
      ) t2
        INNER JOIN (
            SELECT
                    x.customer_id,
                    x.report_period,
                    x.product_type
              FROM (
                    SELECT
                            dcs.customer_id,
                            TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period,
                            dp.product_type,
                            COUNT(dp.product_id) AS count_product,
                            ROW_NUMBER() OVER(
                                PARTITION BY dcs.customer_id, TO_CHAR(fo.order_created_date, 'yyyy-mm')
                                ORDER BY COUNT(dp.product_id) DESC
                            ) AS rn
                      FROM dwh.f_order fo
                        INNER JOIN dwh.d_customer dcs 
                           ON fo.customer_id = dcs.customer_id
                        INNER JOIN dwh.d_product dp 
                           ON fo.product_id = dp.product_id
                        INNER JOIN dwh_update_delta ud 
                           ON fo.customer_id = ud.customer_id
                     GROUP BY dcs.customer_id,
                              TO_CHAR(fo.order_created_date, 'yyyy-mm'),
                              dp.product_type
              ) x
             WHERE x.rn = 1
        ) t3
          ON t2.customer_id = t3.customer_id
         AND t2.report_period = t3.report_period
        INNER JOIN (
            SELECT
                    y.customer_id,
                    y.report_period,
                    y.craftsman_id
              FROM (
                    SELECT
                            dcs.customer_id,
                            TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period,
                            dc.craftsman_id,
                            COUNT(fo.order_id) AS count_craftsman_order,
                            ROW_NUMBER() OVER(
                                PARTITION BY dcs.customer_id, TO_CHAR(fo.order_created_date, 'yyyy-mm')
                                ORDER BY COUNT(fo.order_id) DESC
                            ) AS rn
                      FROM dwh.f_order fo
                        INNER JOIN dwh.d_customer dcs 
                           ON fo.customer_id = dcs.customer_id
                        INNER JOIN dwh.d_craftsman dc 
                           ON fo.craftsman_id = dc.craftsman_id
                        INNER JOIN dwh_update_delta ud 
                           ON fo.customer_id = ud.customer_id
                     GROUP BY dcs.customer_id,
                              TO_CHAR(fo.order_created_date, 'yyyy-mm'),
                              dc.craftsman_id
              ) y
             WHERE y.rn = 1
        ) t4
          ON t2.customer_id = t4.customer_id
         AND t2.report_period = t4.report_period
     ORDER BY t2.report_period
),

insert_delta AS ( -- выполняем insert новых рассчитанных данных для витрины
    INSERT INTO dwh.customer_report_datamart (
        customer_id,
        customer_name,
        customer_address,
        customer_birthday,
        customer_email,
        customer_money,
        platform_money,
        count_order,
        avg_price_order,
        median_time_order_completed,
        top_product_category,
        top_craftsman_id,
        count_order_created,
        count_order_in_progress,
        count_order_delivery,
        count_order_done,
        count_order_not_done,
        report_period
    )
    SELECT
            customer_id,
            customer_name,
            customer_address,
            customer_birthday,
            customer_email,
            customer_money,
            platform_money,
            count_order,
            avg_price_order,
            median_time_order_completed,
            top_product_category,
            top_craftsman_id,
            count_order_created,
            count_order_in_progress,
            count_order_delivery,
            count_order_done,
            count_order_not_done,
            report_period
      FROM dwh_delta_insert_result
),

update_delta AS ( -- выполняем обновление показателей в отчёте по уже существующим заказчикам
    UPDATE dwh.customer_report_datamart SET
        customer_name = updates.customer_name,
        customer_address = updates.customer_address,
        customer_birthday = updates.customer_birthday,
        customer_email = updates.customer_email,
        customer_money = updates.customer_money,
        platform_money = updates.platform_money,
        count_order = updates.count_order,
        avg_price_order = updates.avg_price_order,
        median_time_order_completed = updates.median_time_order_completed,
        top_product_category = updates.top_product_category,
        top_craftsman_id = updates.top_craftsman_id,
        count_order_created = updates.count_order_created,
        count_order_in_progress = updates.count_order_in_progress,
        count_order_delivery = updates.count_order_delivery,
        count_order_done = updates.count_order_done,
        count_order_not_done = updates.count_order_not_done,
        report_period = updates.report_period
    FROM (
        SELECT
                customer_id,
                customer_name,
                customer_address,
                customer_birthday,
                customer_email,
                customer_money,
                platform_money,
                count_order,
                avg_price_order,
                median_time_order_completed,
                top_product_category,
                top_craftsman_id,
                count_order_created,
                count_order_in_progress,
                count_order_delivery,
                count_order_done,
                count_order_not_done,
                report_period
          FROM dwh_delta_update_result
    ) AS updates
    WHERE dwh.customer_report_datamart.customer_id = updates.customer_id
      AND dwh.customer_report_datamart.report_period = updates.report_period
),

insert_load_date AS ( -- записываем дату загрузки
    INSERT INTO dwh.load_dates_customer_report_datamart (
        load_dttm
    )
    SELECT GREATEST(COALESCE(MAX(customers_load_dttm), NOW()), 
                    COALESCE(MAX(craftsman_load_dttm), NOW()), 
                    COALESCE(MAX(products_load_dttm), NOW()))
      FROM dwh_delta
)

SELECT 'increment customer datamart';