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
from time import sleep
import boto3

# =============================================================================
# create data set group
# =============================================================================

# --- create clients
personalize = boto3.client(
    'personalize', aws_access_key_id = access_key, aws_secret_access_key = secret_access_key, region_name = 'us-west-2'
    )

# --- not sure what this is for
personalize_runtime = boto3.client(
    'personalize-runtime', aws_access_key_id = access_key, aws_secret_access_key = secret_access_key, region_name = 'us-west-2'
    )

# =============================================================================
# get list of filters and remove
# =============================================================================
# --- get dataset list
filter_lst = [i['filterArn'] for i in personalize.list_filters()['Filters']]

# --- remove all filters
for i in filter_lst:
    personalize.delete_filter(filterArn = i)

# --- wait for filters to be deleted
while personalize.list_filters()['Filters']:
    print('deleting filters')
    sleep(15)

# =============================================================================
# get list of datasets and remove
# =============================================================================
# --- get dataset list
dataset_lst = [i['datasetArn'] for i in personalize.list_datasets()['datasets']]

# --- remove all datasets
for i in dataset_lst:
    personalize.delete_dataset(datasetArn = i)

# --- wait for datasets to be deleted
while personalize.list_datasets()['datasets']:
    print('deleting datasets')
    sleep(15)

# =============================================================================
# get list of schemas and remove
# =============================================================================
# --- get list of schemas
schema_lst = [i['schemaArn'] for i in personalize.list_schemas()['schemas']]

# --- remove all schemas
for i in schema_lst:
    personalize.delete_schema(schemaArn = i)

# --- wait for schemas to be deleted
while personalize.list_schemas()['schemas']:
    print('deleting schemas')
    sleep(15)

# =============================================================================
# get list of campaigns and remove
# =============================================================================
# --- get list of campaigns
campaign_lst = [i['campaignArn'] for i in personalize.list_campaigns()['campaigns']]

# --- remove all campaigns
for i in campaign_lst:
    personalize.delete_campaign(campaignArn = i)

# --- wait for campaigns to be deleted
while personalize.list_campaigns()['campaigns']:
    print('deleting campaigns')
    sleep(15)

# =============================================================================
# get list of solutions and remove
# =============================================================================
# --- get solution list
solution_lst = [i['solutionArn'] for i in personalize.list_solutions()['solutions']]

# --- remove all solutions
for i in solution_lst:
    personalize.delete_solution(solutionArn = i)

# --- wait for solutions to be deleted
while personalize.list_solutions()['solutions']:
    print('deleting solutions')
    sleep(15)

# =============================================================================
# get list of dataset groups and remove
# =============================================================================
# --- get dataset group list
dataset_group_lst = [i['datasetGroupArn'] for i in personalize.list_dataset_groups()['datasetGroups']]

# --- remove all dataset groups
for i in dataset_group_lst:
    personalize.delete_dataset_group(datasetGroupArn = i)

# --- wait for dataset groups to be deleted
while personalize.list_dataset_groups()['datasetGroups']:
    print('deleting dataset groups')
    sleep(15)
