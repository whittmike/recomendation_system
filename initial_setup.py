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

# --- sql to create initial table of interactions
sql_prim_setup = '''
---------------------------------------- [ EMAIL SAMPLE ] -----
DROP TABLE IF EXISTS email_sample;

    SELECT
        e.lower_email_address
        ,e.cim_id
        ,e.is_employee
        ,e.first_used_datetime
        ,e.days_since_start
        ,e.email_click_likelihood
        ,e.email_click_score
        ,e.email_engagement_persona
        ,s.total_commission
    INTO TEMP email_sample
    FROM daily_email_send_log.email_eng_daily_ext e
    LEFT JOIN (
        SELECT
            c.cim_id
            ,SUM(a.total_commission) AS total_commission
        FROM looker_prebuilds.app_web_session_by_date a
        LEFT JOIN (
            SELECT
                c.cim_id
                ,c.alias_value
                ,a.alias_type
                ,coalesce(em.is_employee, 0) AS is_employee
                ,c.last_used_datetime
                ,c.first_used_datetime
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
            ORDER BY c.cim_id, a.alias_type, c.alias_value
        ) c ON c.alias_value = a.anonymous_id
        WHERE
            trunc(a.session_start_at) >= trunc(getdate()-28)
        GROUP BY 1
    ) s ON s.cim_id = e.cim_id
    WHERE
        e.file_name_date = (select max(file_name_date) from daily_email_send_log.email_eng_daily_ext)
        AND e.email_click_likelihood = 'Most Likely'
        AND e.cim_id IS NOT NULL
        AND e.is_employee = 0
        AND s.total_commission > 0;

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
    WHERE c.cim_id IN (SELECT cim_id FROM email_sample)
    ORDER BY c.cim_id, a.alias_type, c.alias_value;

---------------------------------------- [ PAGE VIEWS ] -----
DROP TABLE IF EXISTS page_views;

    SELECT
        d.cim_id
        ,d.post_id
        ,SUM(d.page_views) AS page_view_count
    INTO TEMP page_views
    FROM (
        SELECT
            c.cim_id
            ,s.post_id
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

---------------------------------------- [ CREATE THREE GROUPS ] -----
DROP TABLE IF EXISTS rec_group_df;

    SELECT
        d.cim_id
        ,d.rand_int
        ,CASE
            WHEN rand_int <= 33 THEN 'in_house_test'
            WHEN rand_int >= 34 AND rand_int <= 67 THEN 'amz_personalize_test'
            WHEN rand_int >= 68 THEN 'control'
            END AS rec_group
    INTO TEMP rec_group_df
    FROM (
        SELECT
            d.cim_id
            ,CAST (RANDOM() * 100 AS INT) AS rand_int
        FROM (
            SELECT
                d.cim_id
            FROM (
                SELECT DISTINCT
                    i.cim_id
                FROM page_views i
            ) d
            ORDER BY RANDOM()
            LIMIT 3000
        ) d
    ) d;

---------------------------------------- [ SETUP FOR INITIAL EXCLUSION ALTER TABLE ] -----
DROP TABLE IF EXISTS rec_system.post_prev_rec;

    SELECT DISTINCT
        CAST(p.cim_id AS VARCHAR(256)) AS cim_id
        ,'1111111' AS post_id -- dummy value for setup
    INTO TABLE rec_system.post_prev_rec
    FROM page_views p;

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
        ,coalesce(pp.prev_rec, 0) AS prev_rec
    INTO TEMP int_01
    FROM page_views p
    LEFT JOIN outgoing_link_clicks o ON o.cim_id = p.cim_id AND o.post_id = p.post_id
    LEFT JOIN orders oo ON oo.cim_id = p.cim_id AND oo.post_id = p.post_id
    LEFT JOIN (
        SELECT DISTINCT cim_id, rec_group FROM rec_group_df
    ) g ON g.cim_id = p.cim_id
    LEFT JOIN (
        SELECT p.cim_id, p.post_id, count(*) AS prev_rec FROM rec_system.post_prev_rec p GROUP BY 1,2
    ) pp ON pp.cim_id = p.cim_id AND pp.post_id = p.post_id;

---------------------------------------- [ FINAL INTERACTIONS TABLE ] -----
DROP TABLE IF EXISTS rec_system.groups_interactions_setup;

    SELECT
        i.cim_id
        ,i.post_id
        ,i.view_count
        ,i.outgoing_link_click_count
        ,i.order_count
        ,i.prev_rec
        ,i.post_rating
        ,r.rec_group
    INTO TABLE rec_system.groups_interactions_setup
    FROM int_01 i
    INNER JOIN rec_group_df r ON r.cim_id = i.cim_id;

------------------------------------------------------------------------------------------------------------------------
-- initial table creation for in house recommendations
------------------------------------------------------------------------------------------------------------------------
---------------------------------------- [ CREATE TABLE FOR DAILY RECOMMENDATIONS TO BE USED IN PRODUCTION ] -----
DROP TABLE IF EXISTS rec_system.daily_rec_production;

CREATE TABLE rec_system.daily_rec_production(
    cim_id VARCHAR(256)
    ,email VARCHAR(256)
    ,subscriber_id VARCHAR(256)
    ,post_id VARCHAR(80)
    ,permalink VARCHAR(65535)
    ,post_title_mod VARCHAR(65535)
    ,post_title VARCHAR(65535)
    ,image_url VARCHAR(65535)
    ,post_order INTEGER
    ,sim_score DOUBLE PRECISION
    ,pred_rating DOUBLE PRECISION
    ,more_explore INTEGER
    ,send_date TIMESTAMP
    ,import_time_stamp TIMESTAMP
);

------------------------------------------------------------------------------------------------------------------------
-- initial table creation for amz recommendations
------------------------------------------------------------------------------------------------------------------------
---------------------------------------- [ CREATE TABLE FOR DAILY AMZ RECOMMENDATIONS TO BE USED IN PRODUCTION ] -----
DROP TABLE IF EXISTS rec_system.amz_daily_rec_production;

CREATE TABLE rec_system.amz_daily_rec_production(
    cim_id VARCHAR(256)
    ,email VARCHAR(256)
    ,subscriber_id VARCHAR(256)
    ,post_id VARCHAR(80)
    ,permalink VARCHAR(65535)
    ,post_title VARCHAR(65535)
    ,image_url VARCHAR(65535)
    ,post_order INTEGER
    ,score DOUBLE PRECISION
    ,send_date TIMESTAMP
    ,import_time_stamp TIMESTAMP
);

'''

# --- print message
print('starting data for initial setup')

# --- execute sql
cursor.execute(sql_prim_setup)

# --- close connection
conn.close()

# --- print message
print('finished data for initial setup - closing connection')

# =============================================================================
# downloads for nltk - only to be done once
# =============================================================================
import nltk
nltk.download('averaged_perceptron_tagger')
nltk.download('omw-1.4')
nltk.download('wordnet')
nltk.download('punkt')
