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
# import libraries and set working directory
# =============================================================================
# --- computational imports
import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from surprise import Reader, Dataset, SVD
from surprise.model_selection import GridSearchCV
from surprise import accuracy
import random
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
from sklearn.feature_extraction import text
import nltk
# nltk.download('averaged_perceptron_tagger')
# nltk.download('omw-1.4')
# nltk.download('wordnet')
# nltk.download('punkt')
from nltk.tokenize import sent_tokenize
from nltk import word_tokenize
from nltk.stem import WordNetLemmatizer 
from nltk.corpus import wordnet as wn
import string
import re
import psycopg2
import pandas.io.sql as sqlio

# =============================================================================
# connect to redshift and pull data
# =============================================================================
# --- setup connection
conn = psycopg2.connect(
        # - connection parameters
    )

# --- sql to generate table
sql_interactions = """
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
FROM rec_system.interactions_daily i
INNER JOIN rec_system.rec_post_meta p ON p.post_id = i.post_id
"""

# --- import interactions data
int_01 = sqlio.read_sql_query(sql_interactions, conn)

# --- sql to generate table
sql_meta = """
SELECT * FROM rec_system.rec_post_meta
"""

# --- import interactions data
meta_01 = sqlio.read_sql_query(sql_meta, conn)

# --- close connection
conn.close()

# --- print message 
print('imported data - closing sql connection')

# =============================================================================
# functions for recommender setup
# =============================================================================
# --- cleans post content data
def string_mod(str_x):
    '''
    Parameters
    ----------
    str_x : TYPE string
        DESCRIPTION. the html content of the post

    Returns
    -------
    str_mod : TYPE string
        DESCRIPTION. a modified version of the html content without html code
    '''   
    # -- remove all html
    str_mod = re.sub('\\<.*\\>', ' ', str_x)
    # -- remove everything but space and alpha
    str_mod = re.sub(r'[^a-zA-Z ]', ' ', str_mod)
    # -- any space 2 or greater change to one
    str_mod = re.sub(r' {2,}', ' ', str_mod)
    # -- strip and lower
    str_mod = str_mod.strip().lower()
    # -- return final string
    return str_mod

# --- creates new column of post title
def get_post_title(str_x):
    '''
    Parameters
    ----------
    str_x : TYPE string
        DESCRIPTION. the url for the post

    Returns
    -------
    str_mod : TYPE string
        DESCRIPTION. a modified version of the url with just the title
    '''
    # -- remove prefix of url
    str_mod = re.sub('https://thekrazycouponlady.com/..../../../', '' , str_x)
    # -- remove everything but space and alpha
    str_mod = re.sub(r'[^a-zA-Z ]', ' ', str_mod)
    # -- any space 2 or greater change to one
    str_mod = re.sub(r' {2,}', ' ', str_mod)
    # -- strip and lower
    str_mod = str_mod.strip().lower()
    # -- return final string
    return str_mod

# --- function for training svd algorithm
def get_svd_algo(int_df):
    '''
    Parameters
    ----------
    int_df : TYPE DataFrame
        DESCRIPTION. the interaction data frame to be used in training svd model

    Returns
    -------
    svd_algo : TYPE algorithm
        DESCRIPTION. the svd algroithm for the recommendation system
    '''

    # -- seed number
    our_seed = 14
    
    # -- setup reader object
    reader = Reader(rating_scale=(1,4)) 
    
    # -- create dataset to be used
    data = Dataset.load_from_df(int_df, reader)
    
    # -- get raw ratings
    raw_ratings = data.raw_ratings
    
    # -- set seed and shuffle
    random.seed(our_seed)
    np.random.seed(our_seed)
    random.shuffle(raw_ratings)
    
    # -- A = 85% of the data, B = 15% of the data
    threshold = int(0.85 * len(raw_ratings))
    A_raw_ratings = raw_ratings[:threshold]
    B_raw_ratings = raw_ratings[threshold:]
    data.raw_ratings = A_raw_ratings
    
    # -- params for grid search
    params_grid = {
        'n_epochs': [15, 20, 25, 30, 35, 40, 45, 50], # (The number of iterations of the Stochastic Gradient Descent minimization procedure.)
        'lr_all': (.001, 0.025), # (The learning rate.)
        'reg_all': (.01, .05) # (The penalty for complex models.)
    }
    
    # -- setup grid search
    print('Grid Search...')
    grid_search = GridSearchCV(SVD, params_grid, measures=['rmse'], cv=3)
    grid_search.fit(data)
    
    # -- get algorithm
    svd_gs_algo = grid_search.best_estimator['rmse']
    
    # -- retrain on the whole set A
    trainset = data.build_full_trainset()
    svd_gs_algo.fit(trainset)
    
    # -- compute biased accuracy
    predictions = svd_gs_algo.test(trainset.build_testset())
    print(f'Biased accuracy on A = {accuracy.rmse(predictions)}')
    
    # -- compute unbiased accuracy on B
    testset = data.construct_testset(B_raw_ratings)  # testset is now the set B
    predictions = svd_gs_algo.test(testset)
    print(f'Unbiased accuracy on B = {accuracy.rmse(predictions)}')
    
    # -- best params
    svd_gs_algo_best_params = grid_search.best_params['rmse']
    print(svd_gs_algo_best_params)
    
    # ---------------------------------------- [ QUESTION 16 ] -----
    # -- apply seed
    random.seed(our_seed)
    np.random.seed(our_seed)
    
    # -- get raw ratings
    data.raw_ratings = raw_ratings
    
    # -- build trainset
    trainset = data.build_full_trainset()
    
    # -- setup svd object
    svd_algo = grid_search.best_estimator['rmse']
    
    # -- apply svd
    svd_algo.fit(trainset)
    
    # -- return svd algorithm for predictions
    return svd_algo

# --- function to return similarity matrix
def fetchSimilarityMatrix(df, soupCol, vectorizer, vectorType='Tfidf'):
    '''
    Parameters
    ----------
    df : TYPE DataFrame
        DESCRIPTION.
    soupCol : TYPE string
        DESCRIPTION. the column in DataFrame that will be used
    vectorizer : TYPE 
        DESCRIPTION. an initialized vectorizer
    vectorType : TYPE, string
        DESCRIPTION. The default is 'Tfidf'.

    Returns
    -------
    sparce_matrix : TYPE: matrix
        DESCRIPTION. a sparse similarity matrix detailing similarity between posts
    '''
    # -- setup col
    df[soupCol] = df[soupCol].fillna('')
    
    # -- get tfid_matrix
    fit_vector = vectorizer.fit_transform(df[soupCol])
    
    # -- transform fitted vectorizer depending on vectorType
    if vectorType == 'Tfidf':
        sparce_matrix = linear_kernel(fit_vector, fit_vector)
    if vectorType == 'Count':
        sparce_matrix = cosine_similarity(fit_vector, fit_vector)
    
    # -- return matrix
    return sparce_matrix

# --- create a helper function to get part of speech
def get_wordnet_pos(word, pretagged = False):
    '''
    Map POS tag to first character lemmatize() accepts
    
    Parameters
    ----------
    word : TYPE string
        DESCRIPTION.
    pretagged : TYPE, optional
        DESCRIPTION. The default is False.

    Returns
    -------
    TYPE
        DESCRIPTION.
    '''
    if pretagged:
        tag = word[1].upper() 
    else:
        tag = nltk.pos_tag([word])[0][1][0].upper()
    
    tag_dict = {"J": wn.ADJ,
                "N": wn.NOUN,
                "V": wn.VERB,
                "R": wn.ADV}

    return tag_dict.get(tag, wn.NOUN)

# --- create a tokenizer that uses lemmatization (word shortening)
class LemmaTokenizer(object):
    def __init__(self):
        self.wnl = WordNetLemmatizer()
    def __call__(self, articles):
        
        # -- grab article
        sents = sent_tokenize(articles)
        # -- get the parts of speech for sentence tokens
        sent_pos = [nltk.pos_tag(word_tokenize(s)) for s in sents]
        # -- flatten the list
        pos = [item for sublist in sent_pos for item in sublist]
        # -- lemmatize based on POS (otherwise, all words are nouns)
        lems = [self.wnl.lemmatize(t[0], get_wordnet_pos(t, True)) for t in pos if t[0] not in string.punctuation]
        # -- clean up in-word punctuation
        lems_clean = [''.join(c for c in s if c not in string.punctuation) for s in lems]
        return lems_clean

# =============================================================================
# functions for hybrid filter
# =============================================================================
# --- used to get the posts that will be used for recommending for each user
def get_post_for_rec(smp_cim_id, interactions_df, post_count):
    '''
    Parameters
    ----------
    smp_cim_id : TYPE string
        DESCRIPTION. cim_id for user
    interactions_df : TYPE data frame
        DESCRIPTION. the full interactions data frame
    post_count : TYPE integer
        DESCRIPTION. number of posts you are going to recommend off of

    Returns
    -------
    data frame of users - the posts that you will be using to recommend off of - and the posts the user has already converted on
    '''
    # -- order list
    tmp_order_lst = ['prev_rec', 'post_rating', 'order_count', 'outgoing_link_click_count', 'view_count', 'max_date']
    tmp_order_bool_lst = [True, False, False, False, False, False]
    
    # -- declare temporary interactions df for specific sim
    tmp_int_df = interactions_df[interactions_df['cim_id'] == smp_cim_id]
    tmp_int_df = tmp_int_df.sort_values(by = tmp_order_lst, ascending = tmp_order_bool_lst).reset_index()
    
    # -- get list of posts to recommend with
    tmp_rec_by_post = tmp_int_df['post_id'][0:post_count].to_list()
    
    # -- get list of post user has already converted on
    tmp_already_convert_post = tmp_int_df[tmp_int_df['post_rating'] == 4]['post_id'].to_list()
    
    # -- sort into returning df
    df_out = pd.DataFrame(
        data = {
            'cim_id': smp_cim_id,
            'rec_by_post': [tmp_rec_by_post],
            'already_convert_post': [tmp_already_convert_post]
        }
    )
    
    # -- return final df
    return df_out

# --- function gets a list of similar posts given user, list of post ids, list of post ids to exclude and number you want to return
def get_like_posts(smp_cim_id, rec_by_post_lst, already_convert_post_lst, similarity_df_post, meta_df, return_post_count_content):
    '''
    Parameters
    ----------
    smp_cim_id : TYPE int
        DESCRIPTION. the cim id for user
    rec_by_post_lst : TYPE list
        DESCRIPTION. list of posts you want to recommend off of
    already_convert_post_lst : TYPE list
        DESCRIPTION. list of posts the user has already converted on
    similarity_df_post : DataFrame
        DESCRIPTION. the large matrix df of similarity values for posts
    meta_df : TYPE DataFrame
        DESCRIPTION. the primary meta data data frame 
    return_post_count_content : TYPE integer
        DESCRIPTION. number of posts you want function to return for the content filter

    Returns
    -------
    DataFrame of post to recommend off of.
    '''
    # -- get all similarity scores
    tmp_all_posts_df = pd.concat(
        [
         pd.DataFrame(
             similarity_df_post[i].sort_values(ascending = False)
             ).join(
                 meta_df['is_expired']
                 )[1:].set_axis(
                     ['sim_score', 'is_expired'], axis = 1, inplace = False
                     ) for i in rec_by_post_lst
        ]
    )
                     
                     # tmp_rt_df['post_id'].isin(sim_df_post.index)
    # -- filter out expired posts
    tmp_all_posts_df = tmp_all_posts_df[tmp_all_posts_df['is_expired'] == 0]
    
    # -- filter out posts that have been previously recommended
    tmp_all_posts_df = tmp_all_posts_df[tmp_all_posts_df.index.isin(already_convert_post_lst) == False]
    
    # -- group by and get max
    tmp_all_posts_df = tmp_all_posts_df.groupby(['post_id']).max()
    
    # -- sort by sim score and grab alloted amount
    tmp_return_df = tmp_all_posts_df.sort_values(by = 'sim_score', ascending = False)[0:return_post_count_content][['sim_score']]
    
    # -- return final df
    return tmp_return_df

# --- function sets rating tier for extra exploration
def set_more_explore(x_dbl, more_explore_threshold):
    '''
    Parameters
    ----------
    x_dbl : TYPE double
        DESCRIPTION. the similarity score
    more_explore_threshold : TYPE double
        DESCRIPTION. threshold for changing output

    Returns
    -------
    x_out : int
        DESCRIPTION. level depending on threshold
    '''
    # -- simple logic statement
    if(x_dbl > more_explore_threshold):
        x_out = 2
    else:
        x_out = 1
    
    # -- return appended list
    return x_out

# --- final hybrid filter - primary function for getting recommendations
def hybrid_filter(
        smp_cim_id,
        rec_by_post_lst,
        already_convert_post_lst,
        similarity_df_post,
        meta_df,
        predictive_algo,
        return_post_count_content,
        return_post_count_final,
        more_explore,
        more_explore_threshold,
        desc
    ):

    '''
        Parameters
        ----------
        smp_cim_id : TYPE int
            DESCRIPTION. the cim id for user
        rec_by_post_lst : TYPE list
            DESCRIPTION. list of posts you want to recommend off of
        already_convert_post_lst : TYPE list
            DESCRIPTION. list of posts the user has already converted on
        similarity_df_post : DataFrame
            DESCRIPTION. the large matrix df of similarity values for posts
        meta_df : TYPE DataFrame
            DESCRIPTION. the primary meta data data frame 
        predictive_algo : TYPE predictive_algorithm
            DESCRIPTION. algorithm for collaborative filter
        return_post_count_content : TYPE integer
            DESCRIPTION. number of posts you want function to return for the content filter
        return_post_count_final : TYPE integer
            DESCRIPTION. number of posts you want function to return for the content filter
        more_explore : TYPE boolean
            DESCRIPTION. true if you want greater exploration based off similarity score
        more_explore_threshold : TYPE double
            DESCRIPTION. threshold for similarity score to give extra priority
        desc : TYPE boolean
            DESCRIPTION. true if you want print out false if you just want returned df

        Returns
        -------
        DataFrame of post we will recommend to user along with possible explanatory print out
    '''
    
    # -- get similar posts given parameters
    like_post_df = get_like_posts(
        smp_cim_id,
        rec_by_post_lst,
        already_convert_post_lst,
        similarity_df_post,
        meta_df,
        return_post_count_content
    )
    
    # -- get predicted rating
    like_post_df['pred_rating'] = [predictive_algo.predict(smp_cim_id, i).est for i in like_post_df.index]
    
    # -- apply more_explore if applicable
    if(more_explore):
        like_post_df['more_explore'] = like_post_df['sim_score'].apply(lambda x: set_more_explore(x, more_explore_threshold))
    else:
        like_post_df['more_explore'] = [1 for i in like_post_df['sim_score']]
        
    # -- sort results
    like_post_df = like_post_df.sort_values(by = ['more_explore', 'pred_rating', 'sim_score'], ascending = False)
    
    # -- with meta values
    like_post_df = like_post_df.join(meta_df[['permalink', 'post_title']])
    
    # -- append cim_id
    like_post_df['cim_id'] = [smp_cim_id for i in like_post_df.index]
    
    # -- append numeric order for posts
    like_post_df['post_order'] = list(range(1, like_post_df.shape[0] + 1))
    
    # -- get columns in preferred order
    like_post_df = like_post_df[['cim_id', 'post_order', 'post_title', 'permalink', 'sim_score', 'pred_rating', 'more_explore']]
    
    # -- final declaration
    rec_out = like_post_df[0:return_post_count_final]
    
    # -- print out desc
    if(desc):
        str_out = '********************************************************************************************************************\n'
        str_out = str_out + f'CIM_ID: {smp_cim_id}\n'
        str_out = str_out + '--------------------------------------------------------------------------------------------------------------------\n'
        str_out = str_out + 'POSTS FOR RECOMMENDATION:\n'
        for i in rec_by_post_lst:
            str_out = str_out + f'{i}: ' + meta_df['post_title'][i] + '\n'
        str_out = str_out + '--------------------------------------------------------------------------------------------------------------------\n'
        str_out = str_out + rec_out[['post_title', 'sim_score', 'pred_rating', 'more_explore']].to_string() + '\n'
        str_out = str_out + '********************************************************************************************************************\n'
        print(str_out)
    
    # -- return output
    return rec_out

# =============================================================================
# data manipulation 01
# =============================================================================
# --- set post_id to string
int_01['post_id'] = int_01['post_id'].astype('string')

# --- only in house recommendation users
int_02 = int_01[int_01['rec_group'] == 'in_house_test'].reset_index()

# --- redefine post content
meta_01['post_content'] = meta_01['post_content'].apply(string_mod)

# --- define post title
meta_01['post_title'] = meta_01['permalink'].apply(get_post_title)

# --- define soup
meta_01['soup'] = meta_01['category_list'] + ' ' + meta_01['post_title'] + ' ' + meta_01['post_content']
meta_01['soup'] = [i.strip() for i in meta_01['soup']]

# --- redefine meta
meta_02 = meta_01.set_index('post_id')

# =============================================================================
# create similarity matrix tdif
# =============================================================================
# --- lemmatize the stop words
lemmatizer = WordNetLemmatizer()
lemmatized_stop_words = [lemmatizer.lemmatize(w) for w in text.ENGLISH_STOP_WORDS]

# --- extend the stop words with any other words you want to add, these are bits of contractions
lemmatized_stop_words.extend(['ve','nt','ca','wo','ll','free','clearance','shipped','shipping','subscription'])

# --- define vectorizer object
tfidf = TfidfVectorizer(tokenizer=LemmaTokenizer(), lowercase=False, stop_words=lemmatized_stop_words, max_features = 500)

# --- similarity matrix and dataframe
sim = fetchSimilarityMatrix(meta_01, 'soup', tfidf, 'Tfidf')
sim_df_post = pd.DataFrame(sim, index = meta_01['post_id'], columns = meta_01['post_id'])

# =============================================================================
# train svd algorithm
# =============================================================================
# --- test function
svd_algo_01 = get_svd_algo(int_01[['cim_id','post_id','post_rating']])

# --- print message 
print('created algo and sim df')

# =============================================================================
# remove some unneeded variables
# =============================================================================
# --- sql string
del sql_interactions, sql_meta

# --- content filter stuff
del lemmatizer, lemmatized_stop_words, tfidf, sim

# =============================================================================
# get all users and posts for recommendations
# =============================================================================
# --- all unique cim ids
cim_id_lst = int_02['cim_id'].unique()

# --- list to append to
post_for_rec_df = pd.concat([get_post_for_rec(i, int_02, 1) for i in cim_id_lst]).set_index('cim_id')

# =============================================================================
# get all recommendations by user
# =============================================================================
# --- list of all final recommendations
final_rec_lst = []

# --- loop through all cims
for i in cim_id_lst:
    final_rec_lst.append(
        hybrid_filter(
            i,
            post_for_rec_df['rec_by_post'][i],
            post_for_rec_df['already_convert_post'][i],
            sim_df_post,
            meta_02,
            svd_algo_01,
            42,
            20,
            True,
            0.50,
            False
        )
    )

# --- define final rec df
final_rec_df = pd.concat(final_rec_lst)
final_rec_df['post_id'] = final_rec_df.index
final_rec_df = final_rec_df[['post_id','cim_id','post_order','post_title','permalink','sim_score','pred_rating','more_explore']].reset_index(drop = True)

# =============================================================================
# set up for already recommended out
# =============================================================================
# --- to send for next daily
already_rec_post_df = post_for_rec_df[['rec_by_post']].explode('rec_by_post').set_axis(['post_id'], axis = 1, inplace = False)
already_rec_post_df['cim_id'] = already_rec_post_df.index
already_rec_post_df = already_rec_post_df[['cim_id', 'post_id']].reset_index(drop = True)

# --- print message 
print('obtained recs pushing to s3')

# =============================================================================
# send data to s3
# =============================================================================
# --- import library
import boto3
import io

# --- establish connection
s3_client_in_house_rec = boto3.client(
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

# --- send interactions data
send_to_s3(
    final_rec_df,
    'kcletl',
    'in_house_rec_system/final_rec_df.csv',
    s3_client_in_house_rec
)

# --- send prev recommendation data to s3
send_to_s3(
    already_rec_post_df,
    'kcletl',
    'in_house_rec_system/already_rec_post_df.csv',
    s3_client_in_house_rec
)

# --- print message 
print('in-house recs pushed to s3')
