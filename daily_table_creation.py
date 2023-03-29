# =============================================================================
# system imports
# =============================================================================
import sys

# --- cred params
access_key = sys.argv[1]
secret_access_key = sys.argv[2]
sql_host = sys.argv[3]
sql_user = sys.argv[4]
sql_password = sys.argv[5]

# =============================================================================
# library import
# =============================================================================
import psycopg2

# =============================================================================
# connect to redshift and create data
# =============================================================================
# --- setup connection
conn = psycopg2.connect(
        # - connection parameters
    )

# --- setup cursor
conn.autocommit = True
cursor = conn.cursor()

# --- sql to create initial table of interactions.
sql_daily_int_setup = '''
------------------------------------------------------------------------------------------------------------------------
-- interactions - rec_system.interactions_daily
------------------------------------------------------------------------------------------------------------------------
---------------------------------------- [ CIM DATA ] -----
DROP TABLE IF EXISTS cim_data;

    SELECT
        c.cim_id
        ,c.alias_value
        ,a.alias_type
        ,coalesce(em.is_employee, 0) AS is_employee
        ,c.last_used_datetime
        ,c.first_used_datetime
    INTO TEMP cim_data
    FROM looker_prebuilds.cim_alias c
    LEFT JOIN looker_prebuilds.cim_alias_type a on a.alias_type_id = c.alias_type_id
    LEFT JOIN (
        SELECT
            c.cim_id
            ,CASE WHEN SUM(CASE WHEN regexp_instr(trim(lower(c.alias_value)), 'krazycouponlady') THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END AS is_employee
        FROM looker_prebuilds.cim_alias c
        LEFT JOIN looker_prebuilds.cim_alias_type a on a.alias_type_id = c.alias_type_id
        WHERE a.alias_type = 'email_address'
        GROUP BY 1
    ) em ON em.cim_id = c.cim_id
    WHERE c.cim_id IN (SELECT DISTINCT cim_id FROM rec_system.groups_interactions_setup)
    ORDER BY c.cim_id, a.alias_type, c.alias_value;

---------------------------------------- [ PAGE VIEWS ] -----
DROP TABLE IF EXISTS page_views;

    SELECT
        d.cim_id
        ,d.post_id
        ,max(d.max_date) AS max_date
        ,sum(d.page_views) AS page_view_count
    INTO TEMP page_views
    FROM (
        SELECT
            c.cim_id
            ,s.post_id
            ,max(s.received_at) AS max_date
            ,count(*) AS page_views
        FROM thekrazycouponlady.pages s
        INNER JOIN cim_data c ON c.alias_type = 'anonymous_id' AND c.alias_value = s.anonymous_id
        WHERE
            s.received_at >= trunc(getdate()) - 28
            AND s.post_id IS NOT NULL
        GROUP BY 1,2
        UNION ALL
        SELECT
            c.cim_id
            ,s.post_id
            ,max(s.received_at) AS max_date
            ,count(*) AS page_views
        FROM deals_mobile_droid_live.screens s
        INNER JOIN cim_data c ON c.alias_type = 'anonymous_id' AND c.alias_value = s.anonymous_id
        WHERE
            s.received_at >= trunc(getdate()) - 28
            AND s.post_id IS NOT NULL
        GROUP BY 1,2
        UNION ALL
        SELECT
            c.cim_id
            ,s.post_id
            ,max(s.received_at) AS max_date
            ,count(*) AS page_views
        FROM deals_mobile_ios.screens s
        INNER JOIN cim_data c ON c.alias_type = 'anonymous_id' AND c.alias_value = s.anonymous_id
        WHERE
            s.received_at >= trunc(getdate()) - 28
            AND s.post_id IS NOT NULL
        GROUP BY 1,2
    ) d
    GROUP BY 1,2;

---------------------------------------- [ OUTGOING LINK CLICK COUNT ] -----
DROP TABLE IF EXISTS outgoing_link_clicks;

    SELECT
        d.cim_id
        ,d.post_id
        ,SUM(d.outgoing_link_click_count) AS outgoing_link_click_count
    INTO TEMP outgoing_link_clicks
    FROM (
        SELECT
            c.cim_id
            ,s.post_id
            ,count(*) AS outgoing_link_click_count
        FROM thekrazycouponlady.outgoing_link_clicked s
        INNER JOIN cim_data c ON c.alias_type = 'anonymous_id' AND c.alias_value = s.anonymous_id
        WHERE
            s.received_at >= trunc(getdate()) - 28
            AND s.post_id IS NOT NULL
        GROUP BY 1,2
        UNION ALL
        SELECT
            c.cim_id
            ,s.post_id
            ,count(*) AS outgoing_link_click_count
        FROM deals_mobile_droid_live.outgoing_link_clicked s
        INNER JOIN cim_data c ON c.alias_type = 'anonymous_id' AND c.alias_value = s.anonymous_id
        WHERE
            s.received_at >= trunc(getdate()) - 28
            AND s.post_id IS NOT NULL
        GROUP BY 1,2
        UNION ALL
        SELECT
            c.cim_id
            ,s.post_id
            ,count(*) AS outgoing_link_click_count
        FROM deals_mobile_ios.outgoing_link_clicked s
        INNER JOIN cim_data c ON c.alias_type = 'anonymous_id' AND c.alias_value = s.anonymous_id
        WHERE
            s.received_at >= trunc(getdate()) - 28
            AND s.post_id IS NOT NULL
        GROUP BY 1,2
    ) d
    GROUP BY 1,2;

---------------------------------------- [ ORDERS ] -----
DROP TABLE IF EXISTS orders;

    SELECT
        c.cim_id
        ,a.post_id
        ,sum(a.order_count) AS order_count
    INTO TEMP orders
    FROM looker_prebuilds.affiliate_commission_by_session a
    INNER JOIN cim_data c ON c.alias_type = 'anonymous_id' AND c.alias_value = a.anonymous_id
    WHERE
        trunc(a.commission_date) >= trunc(getdate()) - 28
    GROUP BY 1,2;

---------------------------------------- [ INT_01 ] -----
DROP TABLE IF EXISTS int_01;

    SELECT
        p.cim_id
        ,g.rec_group
        ,p.post_id
        ,p.page_view_count AS view_count
        ,COALESCE(o.outgoing_link_click_count, 0) AS outgoing_link_click_count
        ,COALESCE(oo.order_count, 0) AS order_count
        ,CASE
            WHEN COALESCE(oo.order_count, 0) > 0 THEN 4
            WHEN COALESCE(oo.order_count, 0) = 0 AND COALESCE(o.outgoing_link_click_count, 0) > 0 THEN 3
            WHEN COALESCE(oo.order_count, 0) = 0 AND COALESCE(o.outgoing_link_click_count, 0) = 0 AND p.page_view_count > 1 THEN 2
            ELSE 1 END AS post_rating
        ,p.max_date
        ,coalesce(pp.prev_rec, 0) AS prev_rec
    INTO TEMP int_01
    FROM page_views p
    LEFT JOIN outgoing_link_clicks o ON o.cim_id = p.cim_id AND o.post_id = p.post_id
    LEFT JOIN orders oo ON oo.cim_id = p.cim_id AND oo.post_id = p.post_id
    LEFT JOIN (
        SELECT DISTINCT cim_id, rec_group FROM rec_system.groups_interactions_setup
    ) g ON g.cim_id = p.cim_id
    LEFT JOIN (
        SELECT p.cim_id, p.post_id, count(*) AS prev_rec FROM rec_system.post_prev_rec p GROUP BY 1,2
    ) pp ON pp.cim_id = p.cim_id AND pp.post_id = p.post_id;

---------------------------------------- [ FINAL INTERACTIONS TABLE ] -----
DROP TABLE IF EXISTS rec_system.interactions_daily;

    SELECT
        i.cim_id
        ,i.rec_group
        ,i.post_id
        ,i.view_count
        ,i.outgoing_link_click_count
        ,i.order_count
        ,i.post_rating
        ,i.max_date
        ,i.prev_rec
    INTO TABLE rec_system.interactions_daily
    FROM int_01 i
    ORDER BY i.cim_id, i.post_rating desc, i.order_count desc, i.outgoing_link_click_count desc, i.view_count desc, i.max_date desc;

------------------------------------------------------------------------------------------------------------------------
-- post meta - rec_system.rec_post_meta
------------------------------------------------------------------------------------------------------------------------
---------------------------------------- [ POST EXPIRE ] -----
DROP TABLE IF EXISTS post_expire;

    SELECT
        pe.post_id
        ,pe.post_expire_date
        ,CASE WHEN dateadd(hour, -8, pe.post_expire_date) < CONVERT_TIMEZONE('UTC', 'America/Denver', getdate()) OR pe.post_expire_date IS NULL THEN 1 ELSE 0 END AS is_expired
    INTO TEMP post_expire
    FROM (
        SELECT
            pm.post_id
            ,(TIMESTAMP 'epoch' + pm.meta_value::BIGINT * INTERVAL '1 second') AS post_expire_date
            ,row_number() OVER (PARTITION BY pm.post_id ORDER BY pm.meta_id DESC) AS rn
        FROM wordpress.postmeta pm
        INNER JOIN wordpress.posts p ON p.ID = pm.post_id
        WHERE
            p.post_type = 'post'
            AND p.post_status = 'publish'
            AND pm.meta_key = 'expiry_date'
            AND pm.meta_value IS NOT NULL
            AND pm.meta_value != ''
    ) pe
    WHERE pe.rn = 1;

---------------------------------------- [ POST META 01 ] -----
DROP TABLE IF EXISTS post_meta_01;

    SELECT DISTINCT
        p.id AS post_id
        ,p.post_type
        ,pe.is_expired
        ,pe.post_expire_date
        ,CASE WHEN p.id IN (SELECT post_id FROM rec_system.interactions_daily) THEN 1 ELSE 0 END AS in_int
    INTO TEMP post_meta_01
    FROM wordpress.posts p
    LEFT JOIN post_expire pe ON pe.post_id = p.id
    WHERE
        p.post_type IN ('post')
        AND (
            p.id IN (SELECT post_id FROM rec_system.interactions_daily)
            OR pe.is_expired = 0
        );

---------------------------------------- [ POST META 02 ] -----
DROP TABLE IF EXISTS post_meta_02;

    SELECT
        p.post_id
        ,p.post_type
        ,p.post_expire_date
        ,datediff(DAYS, CONVERT_TIMEZONE('UTC', 'America/Denver', getdate()), p.post_expire_date) AS days_till_expire
        ,p.is_expired
        ,p.in_int
    INTO TEMP post_meta_02
    FROM post_meta_01 p;

---------------------------------------- [ POST CONTENT ] -----
DROP TABLE IF EXISTS post_content;

    SELECT
        p.id AS post_id
        ,listagg(DISTINCT p.post_content, ' ') WITHIN GROUP ( ORDER BY  greatest(p.post_date, p.post_modified)) AS post_content
    INTO TEMP post_content
    FROM wordpress.posts p
    WHERE p.id IN (SELECT post_id FROM post_meta_02)
    GROUP BY 1;

---------------------------------------- [ POST PERMALINK ] -----
DROP TABLE IF EXISTS post_permalink;

    SELECT
        p.post_id
        ,p.permalink
    INTO TEMP post_permalink
    FROM (
        SELECT
           CAST(wpp.id AS BIGINT)  AS post_id
           ,wpp.post_date
           ,CAST(CONCAT(wpo_su.option_value,REPLACE(REPLACE(REPLACE(REPLACE(wpo.option_value,'%year%',TO_CHAR(wpp.post_date, 'YYYY')),'%monthnum%',TO_CHAR(wpp.post_date, 'MM')),'%day%',TO_CHAR(wpp.post_date, 'DD')),'%postname%',wpp.post_name)) AS VARCHAR(2500)) AS permalink
           ,post_type
        FROM wordpress.posts wpp
        JOIN wordpress.options wpo ON wpo.option_name = 'permalink_structure'
        JOIN wordpress.options wpo_su ON wpo_su.option_name = 'siteurl'
        WHERE
            wpp.post_type IN ('post')
            AND wpp.post_status = 'publish'
        UNION
        SELECT
            CAST(post_id AS INTEGER) AS post_id
            ,post_date
            ,TRIM(BOTH '/' FROM REPLACE(meta_value, 'http:', 'https:')) AS permalink,
            post_type
        FROM wordpress.postmeta pm
        INNER JOIN wordpress.posts wpp ON pm.post_id = wpp.id
        WHERE
            meta_key = 'published_permalink'
            AND wpp.post_type IN ('tip', 'brag')
            AND wpp.post_status = 'publish'
    ) p
    WHERE p.post_id IN (SELECT post_id FROM post_meta_02);

---------------------------------------- [ CAT 01 ] -----
DROP TABLE IF EXISTS cat_01;

    SELECT DISTINCT
        c.post_id
        ,c.taxonomy
        ,c.slug
    INTO TEMP cat_01
    FROM wordpress.post_categories_vw c
    INNER JOIN (
        SELECT
            c.post_id
            ,MAX(c.post_date) AS max_date
        FROM wordpress.post_categories_vw c
        WHERE
            c.post_id IN (SELECT post_id FROM post_meta_02)
        GROUP BY 1
    ) m ON m.post_id = c.post_id AND m.max_date = c.post_date
    WHERE
        c.post_id IN (SELECT post_id FROM post_meta_02)
        AND c.taxonomy IN ('store', 'category')
        AND NOT regexp_instr(c.slug, '(expired|newsletter|uncategorized|black-friday|online-deals|retail|^tip$)')
    ORDER BY c.post_id, c.post_date;

---------------------------------------- [ CAT 02 ] -----
DROP TABLE IF EXISTS cat_02;

    SELECT
        p.post_id
        ,listagg(p.slug, ' ') WITHIN GROUP (ORDER BY p.slug) AS category_list
    INTO TEMP cat_02
    FROM cat_01 p
    WHERE p.taxonomy = 'category'
    GROUP BY 1;

---------------------------------------- [ CAT 03 ] -----
DROP TABLE IF EXISTS cat_03;

    SELECT
        p.post_id
        ,listagg(p.slug, ' ') WITHIN GROUP (ORDER BY p.slug) AS store_list
    INTO TEMP cat_03
    FROM cat_01 p
    WHERE p.taxonomy = 'store'
    GROUP BY 1;

---------------------------------------- [ POST META FINAL ] -----
DROP TABLE IF EXISTS rec_system.rec_post_meta;

    SELECT
        p.post_id
        ,p.post_type
        ,p.post_expire_date
        ,p.days_till_expire
        ,p.is_expired
        ,p.in_int
        ,coalesce(pp.permalink, '') AS permalink
        ,coalesce(pc.post_content, '') AS post_content
        ,coalesce(c.category_list, '') AS category_list
        ,coalesce(cc.store_list, '') AS store_list
    INTO TABLE rec_system.rec_post_meta
    FROM post_meta_02 p
    LEFT JOIN post_permalink pp ON pp.post_id = p.post_id
    LEFT JOIN post_content pc on pc.post_id = p.post_id
    LEFT JOIN cat_02 c on c.post_id = p.post_id
    LEFT JOIN cat_03 cc on cc.post_id = p.post_id;
'''

# --- print message
print('starting data for daily setup')

# --- execute sql
cursor.execute(sql_daily_int_setup)

# --- print message
print('finished data for daily setup')

# --- close connection
conn.close()