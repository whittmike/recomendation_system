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
# library import 01
# =============================================================================
import psycopg2
import pandas as pd
import pandas.io.sql as sqlio

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
sql_amz_personalize_push = """
---------------------------------------- [ temp table for interactions ] -----
DROP TABLE IF EXISTS int_tmp;

    SELECT
        i.cim_id AS user_id
        ,i.post_id AS item_id
        ,CASE
            WHEN i.post_rating IN (1, 2) THEN 'view'
            WHEN i.post_rating = 3 THEN 'click'
            WHEN i.post_rating = 4 THEN 'order'
            END AS event_type
        ,round(date_part(epoch, i.max_date)) AS timestamp
    INTO TEMP int_tmp
    FROM rec_system.interactions_daily i
    INNER JOIN rec_system.rec_post_meta m ON m.post_id = i.post_id
    WHERE
        i.rec_group = 'amz_personalize_test'
    ORDER BY i.cim_id, i.post_id, i.max_date DESC;

---------------------------------------- [ unload interactions data to s3 for amazon personalize ] -----
UNLOAD ('
    SELECT
        user_id
        ,item_id
        ,event_type
        ,timestamp
    FROM int_tmp
')
TO 's3://kcletl/personalize_etl/personalize_interactions/interactions_test.csv'
IAM_ROLE 
ALLOWOVERWRITE
CSV
HEADER
PARALLEL OFF;

---------------------------------------- [ temp table for meta ] -----
DROP TABLE IF EXISTS meta_tmp;

    SELECT
        r.post_id AS item_id
        ,r.is_expired
        ,regexp_replace(regexp_replace(r.category_list, ' ', '|') || '|' || regexp_replace(r.store_list, ' ', '|'), '(^\\\||\\\|$)', '') AS slug_lst
    INTO TEMP meta_tmp
    FROM rec_system.rec_post_meta r
    WHERE
        r.post_id IN (SELECT DISTINCT post_id FROM rec_system.interactions_daily WHERE rec_group = 'amz_personalize_test')
        OR r.is_expired = 0;

---------------------------------------- [ unload meta data to s3 for amazon personalize ] -----
UNLOAD ('
    SELECT
        item_id
        ,is_expired
        ,slug_lst
    FROM meta_tmp
')
TO 's3://kcletl/personalize_etl/personalize_item_meta/item_meta_test.csv'
IAM_ROLE
ALLOWOVERWRITE
CSV
HEADER
PARALLEL OFF;
"""

# --- execute sql
cursor.execute(sql_amz_personalize_push)

# --- sql for interactions that will be used to gather recommendations
sql_amz_int_01 = """
SELECT
    i.cim_id AS user_id
    ,i.post_id AS item_id
    ,CASE
        WHEN i.post_rating IN (1, 2) THEN 'view'
        WHEN i.post_rating = 3 THEN 'click'
        WHEN i.post_rating = 4 THEN 'order'
        END AS event_type
    ,round(date_part(epoch, i.max_date)) AS timestamp
    ,regexp_replace(m.permalink, 'https://thekrazycouponlady.com/..../../../', '') AS post_title_mod
FROM rec_system.interactions_daily i
INNER JOIN rec_system.rec_post_meta m ON m.post_id = i.post_id
WHERE
    i.rec_group = 'amz_personalize_test'
ORDER BY i.cim_id, i.post_id, i.max_date DESC
"""

# --- sql for meta data that will be used to gather recommendations
sql_amz_meta_01 = """
SELECT
    r.post_id AS item_id
    ,r.permalink
    ,r.category_list
    ,r.store_list
FROM rec_system.rec_post_meta r
WHERE r.is_expired = 0
"""

# --- use sql to pull interactions df
amz_int_01 = sqlio.read_sql_query(sql_amz_int_01, conn)

# --- use sql to pull meta df
amz_meta_01 = sqlio.read_sql_query(sql_amz_meta_01, conn)

# --- close connection
conn.close()

# --- remove unneeded
del conn, cursor, sql_amz_personalize_push, sql_amz_int_01

# --- modify interactions df
amz_int_01['user_id'] = amz_int_01['user_id'].astype(str)
amz_int_01['item_id'] = amz_int_01['item_id'].astype(str)

# --- modify meta data
amz_meta_01 = amz_meta_01.set_index('item_id')

# =============================================================================
# library import 02
# =============================================================================
import time
from time import sleep
import json
from datetime import datetime
import boto3
import os

# =============================================================================
# functions to be used later
# =============================================================================
# --- check status of create process
def check_active(response_type, arn):
    # -- set max time 
    max_time = time.time() + 3*60*60 # 3 hours
    # -- loop through
    while time.time() < max_time:
        # - logic to find status
        if(response_type == 'dataset_group'):
            status = personalize.describe_dataset_group(datasetGroupArn = arn)["datasetGroup"]["status"]
        elif(response_type == 'import_job'):
            status = personalize.describe_dataset_import_job(datasetImportJobArn = arn)["datasetImportJob"]['status']
        elif(response_type == 'solution'):
            status = personalize.describe_solution(solutionArn = arn)['solution']['status']
        elif(response_type == 'solution_version'):
            status = personalize.describe_solution_version(solutionVersionArn = arn)['solutionVersion']['status']
        elif(response_type == 'campaign'):
            status = personalize.describe_campaign(campaignArn = arn)['campaign']['status']
        elif(response_type == 'dataset'):
            status = personalize.describe_dataset(datasetArn = arn)['dataset']['status']
        elif(response_type == 'filter'):
            status = personalize.describe_filter(filterArn = arn)['filter']['status']
        # - print status update
        print(f"STATUS: {status}  --  TASK: {response_type.upper()}")
        # - logic to break or continue loop
        if status == "ACTIVE" or status == "CREATE FAILED":
            break
        # - if status is not active wait and continue
        time.sleep(30)

# =============================================================================
# schemas to be used later
# =============================================================================
# --- define interactions schema
interactions_schema = {
	"type": "record",
	"name": "Interactions",
	"namespace": "com.amazonaws.personalize.schema",
	"fields": [
		{
			"name": "user_id",
			"type": "string"
		},
		{
			"name": "item_id",
			"type": "string"
		},
		{
			"name": "event_type",
			"type": "string"
		},
		{
			"name": "timestamp",
			"type": "long"
		}
	],
	"version": "1.0"
}

# --- define metadata schema
item_metadata_schema = {
	"type": "record",
	"name": "Items",
	"namespace": "com.amazonaws.personalize.schema",
	"fields": [
		{
			"name": "item_id",
			"type": "string"
		},
		{
			"name": "is_expired",
			"type": "string"
		},
		{
			"name": "slug_lst",
			"type": [
				"null",
				"string"
			],
			"categorical": True
		}
	],
	"version": "1.0"
}

# =============================================================================
# create data set group
# =============================================================================
# --- create clients
personalize = boto3.client(
    'personalize',
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_access_key,
    region_name = 'us-west-2'
)

# --- used for getting recommedations
personalize_runtime = boto3.client(
    'personalize-runtime', 
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_access_key,
    region_name = 'us-west-2'
)

# --- create data set group
create_dataset_group_response = personalize.create_dataset_group(name = "testing_01")

# --- get arn for new data set group
dataset_group_arn = create_dataset_group_response['datasetGroupArn']

# --- execute function to find status
check_active(response_type = 'dataset_group', arn = dataset_group_arn)

# =============================================================================
# import interactions data
# =============================================================================
# --- export schema to personalize
create_schema_response = personalize.create_schema(
    name = "interactions_schema_01",
    schema = json.dumps(interactions_schema)
)

# --- get arn for interactions schema
interactions_schema_arn = create_schema_response['schemaArn']

# --- define dataset type
create_dataset_response = personalize.create_dataset(
    name = "interactions_01",
    datasetType = "INTERACTIONS",
    datasetGroupArn = dataset_group_arn,
    schemaArn = interactions_schema_arn
)

# --- get arn for interactions dataset
interactions_dataset_arn = create_dataset_response['datasetArn']

# --- check if dataset is active
check_active(response_type = 'dataset', arn = interactions_dataset_arn)

# --- create import job
create_dataset_import_job_response = personalize.create_dataset_import_job(
    jobName = "interactions_import",
    datasetArn = interactions_dataset_arn,
    dataSource = {
        "dataLocation": "s3://kcletl/personalize_etl/personalize_interactions/"
    },
    roleArn = 'roleArn' # - taken out for security reasons
)

# --- get dataset import job arn
interactions_dataset_import_job_arn = create_dataset_import_job_response['datasetImportJobArn']

# --- check if import is active
check_active(response_type = 'import_job', arn = interactions_dataset_import_job_arn)

# =============================================================================
# import item metadata
# =============================================================================
# --- get schema response
create_schema_response = personalize.create_schema(
    name = "metadata_schema_01",
    schema = json.dumps(item_metadata_schema)
)

# --- get arn for item metadata
item_metadata_schema_arn = create_schema_response['schemaArn']

# --- define dataset type 
create_dataset_response = personalize.create_dataset(
    name = "item_metadata_01",
    datasetType = "ITEMS",
    datasetGroupArn = dataset_group_arn,
    schemaArn = item_metadata_schema_arn
)

# --- get arn for item metadata dataset
metadata_dataset_arn = create_dataset_response['datasetArn']

# --- check if dataset is active
check_active(response_type = 'dataset', arn = metadata_dataset_arn)

# --- create import job
create_dataset_import_job_response = personalize.create_dataset_import_job(
    jobName = "item_metadata_import",
    datasetArn = metadata_dataset_arn,
    dataSource = {
        "dataLocation": "s3://kcletl/personalize_etl/personalize_item_meta/"
    },
    roleArn = 'roleArn' # - taken out for security reasons
)

# --- get dataset import job arn
metadata_dataset_import_job_arn = create_dataset_import_job_response['datasetImportJobArn']

# --- check if import is active
check_active(response_type = 'import_job', arn = metadata_dataset_import_job_arn)

# =============================================================================
# create personalize solution
# =============================================================================
# --- list recipes
personalize.list_recipes()

# --- create solution
user_personalization_create_solution_response = personalize.create_solution(
    name = "solution_personalize_01",
    datasetGroupArn = dataset_group_arn,
    recipeArn = "arn:aws:personalize:::recipe/aws-user-personalization" 
)

# --- get arn for personalization solution
user_personalization_solution_arn = user_personalization_create_solution_response['solutionArn']

# --- create solution response for user personalization
userpersonalization_create_solution_version_response = personalize.create_solution_version(
    solutionArn = user_personalization_solution_arn
)

# --- get solution version arn
userpersonalization_solution_version_arn = userpersonalization_create_solution_version_response['solutionVersionArn']

# --- check status on solution version
check_active(response_type = 'solution_version', arn = userpersonalization_solution_version_arn)

# =============================================================================
# create personalized campaign
# =============================================================================
# --- create personalization campaign
userpersonalization_create_campaign_response = personalize.create_campaign(
    name = "campaign_personalize_01",
    solutionVersionArn = userpersonalization_solution_version_arn,
    minProvisionedTPS = 1
)

# --- get arn for personalization campaign
userpersonalization_campaign_arn = userpersonalization_create_campaign_response['campaignArn']

# --- check if campaign is active
check_active(response_type = 'campaign', arn = userpersonalization_campaign_arn)

# =============================================================================
# create filter to exclude expired
# =============================================================================
# --- filter to exclude expired posts
expired_post_filter_response = personalize.create_filter(
    name = "expired_post_filter",
    datasetGroupArn = dataset_group_arn,
    filterExpression = 'EXCLUDE ItemID WHERE Items.is_expired IN ("1")'
)

# --- get arn for filter
expired_post_filter_arn = expired_post_filter_response['filterArn']

# --- check if filter is active
check_active(response_type = 'filter', arn = expired_post_filter_arn)

# --- metrics for personalize solution version
personalize.get_solution_metrics(solutionVersionArn = userpersonalization_solution_version_arn)

# =============================================================================
# pull sample data and run some aggs ***delete later for production***
# =============================================================================
# --- list of unique users
unique_user_lst = amz_int_01['user_id'].unique().tolist()

# --- function to get recommendations
def amz_get_rec(user_id_input, rec_count, desc):
    # --- get similar items
    rec_for_user = personalize_runtime.get_recommendations(
        campaignArn = userpersonalization_campaign_arn,
        userId = user_id_input,
        filterArn = expired_post_filter_arn,
        numResults = rec_count
    )
    
    # --- temporary df to hold vals - modify
    tmp_rec_df = pd.DataFrame(rec_for_user['itemList'])
    tmp_rec_df.columns = ['item_id', 'score']
    tmp_rec_df = tmp_rec_df.set_index('item_id')
    
    # --- join relevant data
    tmp_rec_df = tmp_rec_df.join(amz_meta_01)
    
    # --- append user id
    tmp_rec_df['user_id'] = [user_id_input for i in range(tmp_rec_df.shape[0])]
    
    # --- if true print description
    if desc:
        # --- get clicks and orders of user
        tmp_int = amz_int_01[amz_int_01['user_id'] == user_id_input]
        tmp_int = tmp_int[tmp_int['event_type'].isin(['click','order'])]
        
        print('*************************************************************************')
        print('cim_id: ', user_id_input)
        print('-------------------------------------------------------------------------')
        print(tmp_int[['post_title_mod']].to_string())
        print('-------------------------------------------------------------------------')
        print(tmp_rec_df[['permalink']].to_string())
        print('*************************************************************************')
    
    # --- delcare df to be outputed
    df_out = tmp_rec_df.copy()
    
    # --- setup for output
    df_out['post_id'] = df_out.index
    df_out = df_out.reset_index()
    df_out = df_out[['user_id','post_id','permalink','score']]
    df_out.columns = ['cim_id', 'post_id', 'permalink', 'score']
    df_out['post_order'] = [i for i in range(1,tmp_rec_df.shape[0]+1)]
    
    # --- return final df
    return df_out

# --- get recommendations
amz_final_rec_df = pd.concat([amz_get_rec(i, 20, False) for i in unique_user_lst])

# --- reset index for amz rec
amz_final_rec_df = amz_final_rec_df.reset_index()
amz_final_rec_df = amz_final_rec_df[['cim_id', 'post_id', 'permalink', 'score', 'post_order']]

# =============================================================================
# send data to s3
# =============================================================================
# --- import library
import io

# --- establish connection
s3_client_amz = boto3.client(
    "s3",
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_access_key,
    region_name = 'us-west-2'
)

# --- function sends file to s3 bucket
def send_to_s3(df_to_import, s3_bucket, file_key, s3_client):
    '''
    Parameters
    ----------
    df_to_import : TYPE DataFrame
        DESCRIPTION. data frame you wish to import to s3
    s3_bucket : TYPE string
        DESCRIPTION. the bucket in s3 dir you want the file to go to
    file_key : TYPE string
        DESCRIPTION. the path and name of file
    s3_client : TYPE client.S3
        DESCRIPTION. the s3 client setup in connection

    Returns
    -------
    None.
    prints out verification of sucess
    '''
    # -- reformat csv and send to s3
    with io.StringIO() as csv_buffer:
        # - import to csv
        df_to_import.to_csv(csv_buffer, index=False)
        # - get the response
        response = s3_client.put_object(
            Bucket = s3_bucket, Key = file_key, Body = csv_buffer.getvalue()
        )
        # - from response pull status
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        # - if status is 200 and successfull than exit loop
        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")

# --- send prev recommendation data to s3
send_to_s3(
    amz_final_rec_df,
    'kcletl',
    'personalize_etl/output/amz_final_rec.csv',
    s3_client_amz
)

# =============================================================================
# connect to redshift and push data
# =============================================================================
# --- setup connection
conn = psycopg2.connect(
        # - connection parameters
    )

# --- setup cursor
conn.autocommit = True
cursor = conn.cursor()

# --- sql to create initial table of interactions.
sql_push_amz_to_production = '''
------------------------------------------------------------------------------------------------------------------------
-- daily AMZ rec system staging
------------------------------------------------------------------------------------------------------------------------
---------------------------------------- [ create staging table for daily recommendation feed ] -----
DROP TABLE IF EXISTS rec_system.amz_daily_rec_staging;

    CREATE TABLE rec_system.amz_daily_rec_staging (
         cim_id VARCHAR(256)
        ,post_id VARCHAR(80)
        ,permalink VARCHAR(65535)
        ,score DOUBLE PRECISION
        ,post_order INTEGER
    );

    COPY rec_system.amz_daily_rec_staging
    FROM 's3://kcletl/personalize_etl/output/amz_final_rec.csv'
    CREDENTIALS
    CSV IGNOREHEADER 1;

------------------------------------------------------------------------------------------------------------------------
-- daily rec system staging to production
------------------------------------------------------------------------------------------------------------------------
---------------------------------------- [ identifiers email ] -----
DROP TABLE IF EXISTS amz_id_01;

    SELECT
        e.lower_email_address
        ,e.cim_id
        ,e.extract_timestamp
    INTO TEMP amz_id_01
    FROM daily_email_send_log.email_eng_daily_ext e
    INNER JOIN(
        SELECT
            e.cim_id
            ,MAX(e.extract_timestamp) AS extract_timestamp
        FROM daily_email_send_log.email_eng_daily_ext e
        WHERE
            e.cim_id IN (SELECT DISTINCT cim_id FROM rec_system.amz_daily_rec_staging)
        GROUP BY 1
    ) ee ON ee.cim_id = e.cim_id AND ee.extract_timestamp = e.extract_timestamp
    WHERE
        e.cim_id IN (SELECT DISTINCT cim_id FROM rec_system.amz_daily_rec_staging);

---------------------------------------- [ identifiers subscriber id ] -----
DROP TABLE IF EXISTS amz_id_02;

    SELECT DISTINCT
        i.cim_id
        ,lower(trim(s.email_address)) AS email
        ,s.subscriber_id
    INTO TEMP amz_id_02
    FROM salesforce_email.subscribers s
    INNER JOIN (
        SELECT
            s.email_address
            ,MAX(s.date_created) AS date_created
        FROM salesforce_email.subscribers s
        WHERE
            trim(lower(s.email_address)) IN (SELECT lower_email_address FROM amz_id_01)
        GROUP BY 1
    ) ss ON ss.email_address = s.email_address AND ss.date_created = s.date_created
    INNER JOIN amz_id_01 i ON lower(trim(s.email_address)) = i.lower_email_address;

---------------------------------------- [ post image ] -----
DROP TABLE IF EXISTS amz_post_image;

    SELECT
        pl.id post_id
        ,max(p.guid) AS guid
    INTO TEMP amz_post_image
    FROM events.wordpress.posts pl
    JOIN events.wordpress.postmeta pm ON pl.id = pm.post_id AND pm.meta_key = '_thumbnail_id'
    LEFT JOIN wordpress.posts p ON pm.meta_value = p.id
    WHERE
        pl.post_type IN ('post','tip')
        AND pl.post_status = 'publish' AND p.guid IS NOT NULL
    GROUP BY 1;

---------------------------------------- [ amz identifiers subscriber id ] -----
INSERT INTO rec_system.amz_daily_rec_production (
    SELECT
        r.cim_id
        ,i.email
        ,i.subscriber_id
        ,r.post_id
        ,r.permalink
        ,(SELECT DISTINCT p.post_title FROM wordpress.posts p WHERE p.id = r.post_id ORDER BY p.post_date DESC LIMIT 1) AS post_title
        ,pi.guid AS image_url
        ,r.post_order
        ,r.score
        ,(trunc(convert_timezone('UTC', 'America/Denver', getdate())) || ' 00:00:00.000000')::timestamp AS send_date
        ,convert_timezone('UTC', 'America/Denver', getdate()) AS import_time_stamp
    FROM rec_system.amz_daily_rec_staging r
    LEFT JOIN amz_id_02 i ON i.cim_id = r.cim_id
    LEFT JOIN amz_post_image pi ON pi.post_id = r.post_id
);

------------------------------------------------------------------------------------------------------------------------
-- create final amz rec table
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS rec_system.amz_final_daily_rec_production;

    SELECT
        r.email
        ,r.post_title
        ,1 AS valid
        ,r.send_date
        ,r.image_url
        ,r.permalink AS url
        ,NULL AS slugs
        ,r.post_order AS position
        ,'amz_personalize_rec' AS test_group
        ,'daily' AS post_type
        ,r.post_id
        ,r.subscriber_id
        ,NULL AS received
    INTO TABLE rec_system.amz_final_daily_rec_production
    FROM rec_system.amz_daily_rec_production r
    WHERE
        r.import_time_stamp = (SELECT MAX(import_time_stamp) FROM rec_system.amz_daily_rec_production)
        AND r.subscriber_id IS NOT NULL;
'''

# --- execute sql
cursor.execute(sql_push_amz_to_production)
