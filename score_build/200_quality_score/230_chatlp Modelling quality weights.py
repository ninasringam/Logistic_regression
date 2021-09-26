# %% Changing Directories and Importing Modules 
import os
import sys
import pickle

import pandas as pd
import numpy as np
from pyhive import hive
from pandas.io import sql
from sqlalchemy import create_engine

# change working directory
os.chdir('/vzwhome/sringpr/qes-data/chatlp')
print('Working in: ',os.getcwd())

# add additional paths for importing modules below (relative notation)
sys.path.append(os.path.abspath(os.path.join('/vzwhome/sringpr', 'qesmetric/cross_channel_features/thresholds_scripts')))
import threshold_build_service as tbs

# %%
############################################
########## Instantiate parameters ##########
############################################

# Instantiate Hive Connection parameters
engine = create_engine(
    'hive://tbltlhv01apd-hdp.tdc.vzwcorp.com:10000',
    connect_args={
        'auth': 'KERBEROS', 
        'kerberos_service_name': 'hive'
    }
)

# List of variable to extract (omit those which have too many nulls)
variables_used_in_sampling = [
    'q_cust_first_resptime_max',
    'q_cust_first_resptime_avg',
    'q_arr_resptime_sum',
    'q_total_msg',
    'q_ivr_bnc_3d_flipped_flag',
    'q_closed_by_customer_flipped_flag',
    'q_return_to_queue_flipped_flag',
    'q_web_bnc_3d_flipped_flag',
    'q_voice_bnc_3d_flipped_flag',
    'q_bnc_3d_flipped_flag',
    'q_agent_transfer_flag',
    'q_multiple_agent_flag',
    'q_app_bnc_3d_flag',
    'q_retail_bnc_3d_flag',
    'q_sales_conversion_flag',
    'q_closed_by_agent_flag',
    'q_chatbot_bnc_3d_flipped_flag',   
    'q_detractor_flipped_flag',
    'q_promoter_flag',
    'q_agent_score'
]

#######################################################################
################# STEP 2: MODEL RELATIONSHIP TO CHURN #################
#######################################################################

training_samples = []

for i in range(10):
    training_samples.append(pd.read_csv(f'sample_{i}.csv'))
#    print(training_samples)
# %% Start the model
variables_used_in_modelling = [
    # 'q_cust_first_resptime_max',
    # 'q_cust_first_resptime_avg',
    'q_arr_resptime_sum',
    'q_total_msg',
    'q_ivr_bnc_3d_flipped_flag',
    # 'q_closed_by_customer_flipped_flag',
    'q_return_to_queue_flipped_flag',
    'q_web_bnc_3d_flipped_flag',
    'q_voice_bnc_3d_flipped_flag',
    # 'q_bnc_3d_flipped_flag',
    # 'q_agent_transfer_flag',
    # 'q_multiple_agent_flag',
    'q_app_bnc_3d_flag',
    # 'q_retail_bnc_3d_flag',
    'q_sales_conversion_flag',
    # 'q_closed_by_agent_flag',
    # 'q_chatbot_bnc_3d_flipped_flag'   
    # 'q_detractor_flipped_flag',
    # 'q_promoter_flag', 
    # 'q_agent_score'
]

# Import module used to build model: statsmodel package
import statsmodels.api as sm

# Instantiate empty dataframes to store results of interest

# Empty table for beta coefficients
coefficients = pd.DataFrame()
# Empty table for coefficient p-values
pvalues = pd.DataFrame()
# Empty table for metric standard deviations
std_dev = pd.DataFrame()

############################################
######## Fit Model to Training Data ########
############################################

# LOOP OVER THE DIFFERENT SAMPLES EXTRACTED
for i, df in enumerate(training_samples):

    ###### SUBSET COLUMNS FOR TRAINING ########
    X_train = df.loc[:,variables_used_in_modelling].copy(deep=True)
    # Extract target variable
    y_train = df.churn_vol_m1_flag.fillna(0)

    ###### DATA PRE-PROCESSING ########
    # Transforming continuous to integer
    
    #X_train['q_cust_first_resptime_max'] = X_train['q_cust_first_resptime_max'].apply(lambda x: int(x))
    #X_train.loc[X_train['q_cust_first_resptime_max'] == 5, 'q_cust_first_resptime_max'] = 4
    
    #X_train['q_cust_first_resptime_avg'] = X_train['q_cust_first_resptime_avg'].apply(lambda x: int(x))
    #X_train.loc[X_train['q_cust_first_resptime_avg'] == 5, 'q_cust_first_resptime_avg'] = 4
    
    X_train['q_arr_resptime_sum'] = X_train['q_arr_resptime_sum'].apply(lambda x: int(x))
    X_train.loc[X_train['q_arr_resptime_sum'] == 5, 'q_arr_resptime_sum'] = 4
    
    X_train['q_total_msg'] = X_train['q_total_msg'].apply(lambda x: int(x))
    X_train.loc[X_train['q_total_msg'] == 5, 'q_total_msg'] = 4
    
    #X_train['q_agent_score'] = X_train['q_agent_score'].apply(lambda x: int(x))
    #X_train.loc[X_train['q_agent_score'] == 5, 'q_agent_score'] = 4
    
    ####### MODEL FITTING #######
    # Instantiate model object and fit to training data
    model = sm.Logit(y_train, sm.add_constant(X_train)).fit(method='bfgs', maxiter=1000)

    ####### MODEL RESULTS #####
    # Inserting the information we want into the appropriate tables
    coefficients = coefficients.append(model.params, ignore_index = True, sort = False)
    pvalues = pvalues.append(model.pvalues < 0.05, ignore_index = True, sort=False)
    std_dev = std_dev.append(X_train.std(), ignore_index = True, sort=False)

    df['fitted_values'] = model.fittedvalues

    print('iteration: ',i,'\nModel Summary:',model.summary())
    
    # conf_intervals = model.conf_int() # Not being used/inspected at the moment

# %% 
############################################
### Transform Model Outputs into Weights ###
############################################
# Clean the outputs for next step
coefficients.drop(columns='const', inplace=True)
pvalues.drop(columns='const', inplace=True)

# Factor in the boolean truth flags for whether the pvalue is less than 5% - Insignifanct parameters will be forced to 0
matrix_of_significant_parameters = coefficients * pvalues

# Average the significant parameters across samples, insignificant parameters contribute 0 to the average for that metric
vector_of_metric_parameter_means = matrix_of_significant_parameters.abs().mean()

# The sum of average for parameters will be used as a denominator when calculating the relative weights of for each metric
sum_of_metric_parameter_means = matrix_of_significant_parameters.abs().mean().sum()

# Final weight calculation results in a vector, each element is the weight for a different metric
vector_of_metric_weights = vector_of_metric_parameter_means / sum_of_metric_parameter_means

# Sense Check:
print('Sum of absolute weights: ',vector_of_metric_weights.abs().sum())

# Rename the the metrics we were using for sales and service success
print('Final Individual Metric Weights...\n')
for metric, weight in vector_of_metric_weights.items():
    print(metric,': ',f'{weight:.2%}')
	
# %% 
############################################
##### Adjust the weight table in hive #####
############################################

# %% STEP 1: Generate Statements for Table Creation

# 1a) Table creation
create_table_query = '''
create table qes_prd_qmtbls.chatlp_model_weights_nina(
        metric_name     string
        ,weight         numeric(7,6)
    ) stored as orc tblproperties("orc.compress" = "SNAPPY")
'''
# 1b) Data insertion
insert_weights = ''

for metric, weight in vector_of_metric_weights.items():
    insert_weights += f'''('{metric}',{weight}),\n'''

insert_weights = insert_weights[:-2]

insert_data_query = '''insert overwrite table qes_prd_qmtbls.chatlp_model_weights_nina values
''' + insert_weights

# Inspect the generate statement
print(insert_data_query)

# %% STEP 2: Execute queries

sql.execute('drop table if exists qes_prd_qmtbls.chatlp_model_weights_nina', engine)
sql.execute(create_table_query, engine)
sql.execute(insert_data_query, engine)

# %% 
############################################
########### Saving Model Outputs ###########
############################################
filename = 'chatlp_model_weights.obj'

outfile = open(filename,'wb')
pickle.dump(vector_of_metric_weights,outfile)
outfile.close()

# %% Sample code to load model
# filename = 'chat_model_weights.obj'
# infile = open(filename,'rb')
# x = pickle.load(infile)
# infile.close()

# %% Save CSV outputs
coefficients['output'] = 'coefficients'
std_dev['output'] = 'Std dev'
pvalues['output'] = 'pvalue'
pd.concat([coefficients, std_dev, pvalues], ignore_index = True).to_csv('chatlp_model_outputs.csv')

vector_of_metric_weights.to_csv('chatlp_model_weights.csv', header=False)

print(''' Here are the results:
 
'''
)