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
sql_final_push = '''
------------------------------------------------------------------------------------------------------------------------
-- push daily in house rec to s3
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS in_house_daily;

    SELECT
        r.email
        ,r.post_title
        ,r.valid
        ,r.send_date
        ,regexp_replace(r.image_url, '^.+/wp-content', 'https://prod-cdn.thekrazycouponlady.com/wp-content') AS image_url
        ,r.url
        ,r.slugs
        ,r.position
        ,right(r.subscriber_id, 2) AS test_group
        ,'in_house_daily' AS post_type
        ,r.post_id
        ,r.subscriber_id
        ,c.last_used_datetime
        ,r.received
    INTO TEMP in_house_daily
    FROM rec_system.final_daily_rec_production r
    LEFT JOIN (
        SELECT
            c.cim_id
            ,c.alias_value
            ,c.last_used_datetime
        FROM looker_prebuilds.cim_alias c
        LEFT JOIN looker_prebuilds.cim_alias_type a on a.alias_type_id = c.alias_type_id
        ORDER BY c.cim_id, a.alias_type, c.alias_value
    ) c ON c.alias_value = r.subscriber_id
    INNER JOIN (
        SELECT
            r.email
            ,MAX(COALESCE(c.last_used_datetime, '2000-01-01 00:00:00.000000'::timestamp)) AS last_used_date_time
        FROM rec_system.final_daily_rec_production r
        LEFT JOIN (
            SELECT
                c.cim_id
                ,c.alias_value
                ,c.last_used_datetime
            FROM looker_prebuilds.cim_alias c
            LEFT JOIN looker_prebuilds.cim_alias_type a on a.alias_type_id = c.alias_type_id
            ORDER BY c.cim_id, a.alias_type, c.alias_value
        ) c ON c.alias_value = r.subscriber_id
        GROUP BY 1
    ) rr ON rr.last_used_date_time = COALESCE(c.last_used_datetime, '2000-01-01 00:00:00.000000'::timestamp) AND rr.email = r.email
    ORDER BY r.email, r.position;

---------------------------------------- [ unload data to s3 for in house personalize ] -----
UNLOAD ('
    SELECT
        email
        ,post_title
        ,valid
        ,send_date
        ,image_url
        ,url
        ,slugs
        ,position
        ,test_group
        ,post_type
        ,post_id
        ,subscriber_id
        ,received
    FROM in_house_daily
    ORDER BY email, subscriber_id, position
')
TO 's3://kcletl/salesforce_s3/Import/kcl_in_house_personalization_test_feed.csv'
IAM_ROLE 'arn:aws:iam::098294454856:role/prod-redshift-access'
ALLOWOVERWRITE
CSV
HEADER
PARALLEL OFF;

------------------------------------------------------------------------------------------------------------------------
-- push daily amz rec to s3
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS amz_daily;

    SELECT
        r.email
        ,r.post_title
        ,r.valid
        ,r.send_date
        ,regexp_replace(r.image_url, '^.+/wp-content', 'https://prod-cdn.thekrazycouponlady.com/wp-content') AS image_url
        ,r.url
        ,r.slugs
        ,r.position
        ,right(r.subscriber_id, 2) AS test_group
        ,'amz_personalize_daily' AS post_type
        ,r.post_id
        ,r.subscriber_id
        ,c.last_used_datetime
        ,r.received
    INTO TEMP amz_daily
    FROM rec_system.amz_final_daily_rec_production r
    LEFT JOIN (
        SELECT
            c.cim_id
            ,c.alias_value
            ,c.last_used_datetime
        FROM looker_prebuilds.cim_alias c
        LEFT JOIN looker_prebuilds.cim_alias_type a on a.alias_type_id = c.alias_type_id
        ORDER BY c.cim_id, a.alias_type, c.alias_value
    ) c ON c.alias_value = r.subscriber_id
    INNER JOIN (
        SELECT
            r.email
            ,MAX(COALESCE(c.last_used_datetime, '2000-01-01 00:00:00.000000'::timestamp)) AS last_used_date_time
        FROM rec_system.amz_final_daily_rec_production r
        LEFT JOIN (
            SELECT
                c.cim_id
                ,c.alias_value
                ,c.last_used_datetime
            FROM looker_prebuilds.cim_alias c
            LEFT JOIN looker_prebuilds.cim_alias_type a on a.alias_type_id = c.alias_type_id
            ORDER BY c.cim_id, a.alias_type, c.alias_value
        ) c ON c.alias_value = r.subscriber_id
        GROUP BY 1
    ) rr ON rr.last_used_date_time = COALESCE(c.last_used_datetime, '2000-01-01 00:00:00.000000'::timestamp) AND rr.email = r.email
    ORDER BY r.email, r.position;

---------------------------------------- [ unload data to s3 for in house personalize ] -----
UNLOAD ('
    SELECT
        email
        ,post_title
        ,valid
        ,send_date
        ,image_url
        ,url
        ,slugs
        ,position
        ,test_group
        ,post_type
        ,post_id
        ,subscriber_id
        ,received
    FROM amz_daily
    ORDER BY email, subscriber_id, position
')
TO 's3://kcletl/salesforce_s3/Import/amz_personalize_personalization_test_feed.csv'
IAM_ROLE
ALLOWOVERWRITE
CSV
HEADER
PARALLEL OFF;
'''

# --- print message
print('starting data for final push')

# --- execute sql
cursor.execute(sql_final_push)

# --- close connection
conn.close()

# --- print message
print('finished data for final push - closing connection')
