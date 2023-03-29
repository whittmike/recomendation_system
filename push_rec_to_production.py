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
iam_role = sys.argv[6]

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
sql_push_to_production = f'''
------------------------------------------------------------------------------------------------------------------------
-- daily rec system staging
------------------------------------------------------------------------------------------------------------------------
---------------------------------------- [ create staging table for daily recommendation feed ] -----
DROP TABLE IF EXISTS rec_system.daily_rec_staging;

    CREATE TABLE rec_system.daily_rec_staging (
         post_id VARCHAR(80)
        ,cim_id VARCHAR(256)
        ,post_order INTEGER
        ,post_title VARCHAR(256)
        ,permalink VARCHAR(65535)
        ,sim_score DOUBLE PRECISION
        ,pred_rating DOUBLE PRECISION
        ,more_explore INTEGER
    );

    COPY rec_system.daily_rec_staging
    FROM 's3://kcletl/in_house_rec_system/final_rec_df.csv'
    CREDENTIALS '{iam_role}'
    CSV IGNOREHEADER 1;

------------------------------------------------------------------------------------------------------------------------
-- daily rec system staging to production
------------------------------------------------------------------------------------------------------------------------
---------------------------------------- [ identifiers email ] -----
DROP TABLE IF EXISTS id_01;

    SELECT
        e.lower_email_address
        ,e.cim_id
        ,e.extract_timestamp
    INTO TEMP id_01
    FROM daily_email_send_log.email_eng_daily_ext e
    INNER JOIN(
        SELECT
            e.cim_id
            ,MAX(e.extract_timestamp) AS extract_timestamp
        FROM daily_email_send_log.email_eng_daily_ext e
        WHERE
            e.cim_id IN (SELECT DISTINCT cim_id FROM rec_system.daily_rec_staging)
        GROUP BY 1
    ) ee ON ee.cim_id = e.cim_id AND ee.extract_timestamp = e.extract_timestamp
    WHERE
        e.cim_id IN (SELECT DISTINCT cim_id FROM rec_system.daily_rec_staging);

---------------------------------------- [ identifiers subscriber id ] -----
DROP TABLE IF EXISTS id_02;

    SELECT DISTINCT
        i.cim_id
        ,lower(trim(s.email_address)) AS email
        ,s.subscriber_id
    INTO TEMP id_02
    FROM salesforce_email.subscribers s
    INNER JOIN (
        SELECT
            s.email_address
            ,MAX(s.date_created) AS date_created
        FROM salesforce_email.subscribers s
        WHERE
            trim(lower(s.email_address)) IN (SELECT lower_email_address FROM id_01)
            AND s.status = 'Active'
        GROUP BY 1
    ) ss ON ss.email_address = s.email_address AND ss.date_created = s.date_created
    INNER JOIN id_01 i ON lower(trim(s.email_address)) = i.lower_email_address;

---------------------------------------- [ post image ] -----
DROP TABLE IF EXISTS post_image;

    SELECT
        pl.id post_id
        ,max(p.guid) AS guid
    INTO TEMP post_image
    FROM events.wordpress.posts pl
    JOIN events.wordpress.postmeta pm ON pl.id = pm.post_id AND pm.meta_key = '_thumbnail_id'
    LEFT JOIN wordpress.posts p ON pm.meta_value = p.id
    WHERE
        pl.post_type IN ('post','tip')
        AND pl.post_status = 'publish' AND p.guid IS NOT NULL
    GROUP BY 1;

---------------------------------------- [ identifiers subscriber id ] -----
INSERT INTO rec_system.daily_rec_production (
    SELECT
        r.cim_id
        ,i.email
        ,i.subscriber_id
        ,r.post_id
        ,r.permalink
        ,r.post_title AS post_title_mod
        ,(SELECT DISTINCT p.post_title FROM wordpress.posts p WHERE p.id = r.post_id ORDER BY p.post_date DESC LIMIT 1) AS post_title
        ,pi.guid AS image_url
        ,r.post_order
        ,r.sim_score
        ,r.pred_rating
        ,r.more_explore
        ,(trunc(convert_timezone('UTC', 'America/Denver', getdate())) || ' 00:00:00.000000')::timestamp AS send_date
        ,convert_timezone('UTC', 'America/Denver', getdate()) AS import_time_stamp
    FROM rec_system.daily_rec_staging r
    LEFT JOIN id_02 i ON i.cim_id = r.cim_id
    LEFT JOIN post_image pi ON pi.post_id = r.post_id
);

------------------------------------------------------------------------------------------------------------------------
-- post previous rec staging
------------------------------------------------------------------------------------------------------------------------
---------------------------------------- [ create table for staging previous recommendations ] -----
DROP TABLE IF EXISTS rec_system.post_prev_rec_staging;

    CREATE TABLE rec_system.post_prev_rec_staging (
        cim_id VARCHAR(256)
        ,post_id VARCHAR(80)
    );

    COPY rec_system.post_prev_rec_staging
    FROM 's3://kcletl/in_house_rec_system/already_rec_post_df.csv'
    CREDENTIALS '{iam_role}'
    CSV IGNOREHEADER 1;

---------------------------------------- [ insert into main table for filtering out previous recommendations ] -----
INSERT INTO rec_system.post_prev_rec (
    SELECT
        p.cim_id
        ,p.post_id
    FROM rec_system.post_prev_rec_staging p
);

------------------------------------------------------------------------------------------------------------------------
-- create final rec table
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS rec_system.final_daily_rec_production;

    SELECT
        r.email
        ,r.post_title
        ,1 AS valid
        ,r.send_date
        ,r.image_url
        ,r.permalink AS url
        ,NULL AS slugs
        ,r.post_order AS position
        ,'in_house_rec' AS test_group
        ,'daily' AS post_type
        ,r.post_id
        ,r.subscriber_id
        ,NULL AS received
    INTO TABLE rec_system.final_daily_rec_production
    FROM rec_system.daily_rec_production r
    WHERE 
        r.import_time_stamp = (SELECT MAX(import_time_stamp) FROM rec_system.daily_rec_production)
        AND r.subscriber_id IS NOT NULL;
'''

# --- print message
print('pushing daily in-house recommendations to production')

# --- execute sql
cursor.execute(sql_push_to_production)

# --- close connection
conn.close()

# --- print message
print('finished pushing daily in-house recommendations to production - closed connection')
