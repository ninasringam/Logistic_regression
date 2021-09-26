# %% Import Modules
import os
import sys
import pickle

import pandas as pd
import numpy as np
from pandas.io import sql
from sqlalchemy import create_engine

os.chdir('/vzwhome/sringpr/qes-data/chatlp')
print('Current working directory: {}'.format(os.getcwd()))

# add additional paths for importing modules below (relative notation)
sys.path.append(os.path.abspath(os.path.join('/vzwhome/sringpr', 'qesmetric/cross_channel_features/thresholds_scripts')))
import threshold_build_service as tbs

# %% Instantiate parameters

# Create engine for connection to hive with pandas io tools
host = 'tbltlhv01apd-hdp.tdc.vzwcorp.com'
port = 10000
engine = create_engine(f'hive://{host}:{port}',connect_args={'auth': 'KERBEROS', 'kerberos_service_name': 'hive'})

# %%
############################################
########## Instantiate parameters ##########
############################################

# %% Instantiate Hive Connection parameters
engine = create_engine(
    'hive://tbltlhv01apd-hdp.tdc.vzwcorp.com:10000',
    connect_args={
        'auth': 'KERBEROS', 
        'kerberos_service_name': 'hive'
    }
)

# List variables to extract
variables_used_in_sampling = [
    'q_nature_score',
    'avg_quality_score',
    'min_quality_score'
]

# %%
#######################################################################
################# STEP 2: MODEL RELATIONSHIP TO CHURN #################
#######################################################################

# %%
training_samples = []

for i in range(10):
    training_samples.append(pd.read_csv(f'level_2_sample_{i}.csv'))

# %% VARIABLES TO USE IN MODELLING
final_modelling_variables = [
     'avg_quality_score'
    ,'q_nature_score'
#    , 'min_quality_score'
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
    X_train = df.loc[:,final_modelling_variables].copy(deep=True)
    # Extract target variable
    y_train = df.churn_vol_m1_flag.fillna(0)
    
    ###### DATA PREPROCESSING ########

    # Transforming continuous to integer
    X_train['q_nature_score'] = X_train['q_nature_score'].apply(lambda x: int(x))
    X_train.loc[X_train['q_nature_score'] == 5, 'q_nature_score'] = 4

    X_train['avg_quality_score'] = X_train['avg_quality_score'].apply(lambda x: int(x))
    X_train.loc[X_train['avg_quality_score'] == 5, 'avg_quality_score'] = 4
    
    # X_train['min_quality_score'] = X_train['min_quality_score'].apply(lambda x: int(x))
    # X_train.loc[X_train['min_quality_score'] == 5, 'min_quality_score'] = 4

    ####### MODEL FITTING #######
    # Instantiate model object and fit to training data
    model = sm.Logit(y_train, sm.add_constant(X_train)).fit(method='bfgs')

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
# Clean the interim tables (drop the constant)
## coefficients.drop(columns='const', inplace=True)
## pvalues.drop(columns='const', inplace=True)

# For each training sample, multiply the coefficient * the standard deviation
matrix_of_coef_x_stdev = coefficients * std_dev

# Factor in the boolean truth flags for whether the pvalue is less than 5% - Insignifanct parameters will be forced to 0
matrix_of_significant_parameters = matrix_of_coef_x_stdev * pvalues

# Average the significant parameters across samples, insignificant parameters contribute 0 to the average for that metric
vector_of_metric_parameter_means = matrix_of_significant_parameters.abs().mean()

# The sum of average for parameters will be used as a denominator when calculating the relative weights of for each metric
sum_of_metric_parameter_means = matrix_of_significant_parameters.abs().mean().sum()

# Final weight calculation results in a vector, each element is the weight for a different metric
vector_of_metric_weights = vector_of_metric_parameter_means / sum_of_metric_parameter_means

# Sense Check:
print('Sum of absolute weights: ',vector_of_metric_weights.abs().sum())

print('Final Individual Metric Weights...\n')
for metric, weight in vector_of_metric_weights.items():
    print(metric,': ',f'{weight:.2%}')
    
# %% 
############################################
##### Adjust the weight table in hive #####
############################################

# %% STEP 1: Generate Statements for Table Creation
insert_weights = ''

for metric, weight in vector_of_metric_weights.items():
    insert_weights += f'''('{metric}',{weight}),\n'''

insert_weights = insert_weights[:-2]

insert_data_query = '''insert into table chatlp_model_weights values
''' + insert_weights

# Inspect the generate statement
print(insert_data_query)