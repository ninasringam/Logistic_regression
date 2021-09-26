# %% [markdown]
# ### Threshold Status: 
# - **Valid** Thresholds Found

# %% Changing Directories and Importing Modules 
import os
import sys


import pandas as pd
import numpy as np
from pyhive import hive

# change working directory
os.chdir('/vzwhome/sringpr/qes-data/chatlp')
print('Working in: ',os.getcwd())

# add additional paths for importing modules below (relative notation)
sys.path.append(os.path.abspath(os.path.join('/vzwhome/sringpr', 'qesmetric/cross_channel_features/thresholds_scripts')))

import threshold_build_service as tbs

# %% Import and Pre-process Data

# === cust_first_resptime_max ==========
# Read csv data
df = pd.read_csv('chatlp_raw_data.csv')
# Filter for a single metric
df = df.loc[df.metricname == 'cust_first_resptime_max']

# Filter for the appropriate typology
df = df.loc[df.primary_high_level_reason == 'all']

# Rename the main column
df.rename(columns={'value':'cust_first_resptime_max'}, inplace=True)

# Remove nulls
print(len(df))
df = df.loc[df.cust_first_resptime_max.notnull()]
print(len(df))

# %% Instantiate Metadata (class object for threshold build)
cust_first_resptime_max_object = tbs.Threshold_Build(
    metricname = 'cust_first_resptime_max', 
    channel = 'chatlp', 
    input_df = df.copy(deep=True)
)

# %% Analyze exisiting relationship: Plot Bivariate
# bins = [i for i in range(-7200,-9,60)]

cust_first_resptime_max_object.plot_churn(
    bins = [i for i in range(0,500,1)]
)

# %% Explicitly record the relationship to churn
cust_first_resptime_max_object.relationship_to_churn = 'negative'

cust_first_resptime_max_object.input_df['cust_first_resptime_max'] = cust_first_resptime_max_object.input_df['cust_first_resptime_max'] * (-1)

# %% Split the input dataframe
cust_first_resptime_max_object.split = (None, None)

# %% Create universe of potential thresholds

cust_first_resptime_max_object.create_3(
    list_of_thresholds = [-i for i in range(0, 500)]
)

# %% Establish cutoffs for each potential threshold
cust_first_resptime_max_object.create_1_5(
    ## think of argument name for how many percentiles to loop through
)

cust_first_resptime_max_object.create_2_4(
    ## create argument for how many potential 2s and 4s we cycle through
)


# %% Create bins
cust_first_resptime_max_object.create_bins(
        input_df = cust_first_resptime_max_object.cutoff_universe_df,
        bin_definitions = {
                'bin_1':['1','btwn_1_2'],
                'bin_2':['2','btwn_2_3'],
                'bin_3':['3','btwn_3_4'],
                'bin_4':['4','btwn_4_5','5']
        }
)

# %% Validity Checks
cust_first_resptime_max_object.cutoff_validity_checks(
    num_bins = 4,
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'], 
    check_monotonic = True, 
    check_pop_size = True,
    min_pop_size = (sum(cust_first_resptime_max_object.input_df.events) * 0.01)
)

# Looking at the number of valid curves
print('Total Completely Valid Cutoffs:',
    sum((cust_first_resptime_max_object.cutoff_universe_df['is_monotonic'] == 'Y') &
    (cust_first_resptime_max_object.cutoff_universe_df['is_valid_bin_size'] == 'Y')),
    '\nTotal Monotonic Cutoffs:',
    sum((cust_first_resptime_max_object.cutoff_universe_df['is_monotonic'] == 'Y')),
    '\nTotal Cutoffs with Valid Bin Sizes:',
    sum((cust_first_resptime_max_object.cutoff_universe_df['is_valid_bin_size'] == 'Y'))
)


# %% Define ranking columns for shortlisting the best curves
print('Ranking using good vs bad')
cust_first_resptime_max_object.define_ranking(
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'],
    ranking_formula = 'good_v_bad',
    ranking_column = 'good_v_bad_rank',
    ranking_method = 'dense',
    rounding_factor = 3
)

print('Ranking using dif between extremes')
cust_first_resptime_max_object.define_ranking(
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'],
    ranking_formula = 'dif_in_extremes',
    ranking_column = 'dif_in_extremes_rank',
    ranking_method = 'dense',
    rounding_factor = 4
)

print('Ranking using pairwise ratio')
cust_first_resptime_max_object.define_ranking(
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'],
    ranking_formula = 'pairwise_ratio',
    ranking_column = 'pairwise_ratio_rank',
    ranking_method = 'dense',
    rounding_factor = 1
)

print('Ranking using sum of square errors')
cust_first_resptime_max_object.define_ranking(
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'],
    ranking_formula = 'sum_square_errors',
    ranking_column = 'sum_square_errors_rank',
    ranking_method = 'dense',
    rounding_factor = None      # Does not round the squared errors
)

# %% Saving curve dataframe to csv for inspection and manual curve selection
cust_first_resptime_max_object.cutoff_universe_df.sort_values(
    by = [
        'is_monotonic',
        'is_valid_bin_size',
        'pairwise_ratio_rank',
        'dif_in_extremes_rank',
        'sum_square_errors_rank'
    ],
    ascending = [
        False,
        False,
        True,
        True,
        True
    ],
    inplace = True
)

# Save sorted dataframe as csv
cust_first_resptime_max_object.cutoff_universe_df.to_csv('chatlp_cust_first_resptime_max_cutoffs.csv')

# %% Establish best threshold (pick final candidate)

# Top row is the winner
best_cust_first_resptime_max_curve = cust_first_resptime_max_object.cutoff_universe_df.iloc[0,:]

#%%
# Assign cutoffs from the winner to object attributes
cust_first_resptime_max_object.best_threshold = best_cust_first_resptime_max_curve['threshold'] * (-1)
print('threshold:', cust_first_resptime_max_object.best_threshold)

cust_first_resptime_max_object.best_cutoff_1 =  best_cust_first_resptime_max_curve['cutoff_1'] * (-1)
print('cutoff 1:', cust_first_resptime_max_object.best_cutoff_1)

cust_first_resptime_max_object.best_cutoff_2 =  best_cust_first_resptime_max_curve['cutoff_2'] * (-1)
print('cutoff 2:', cust_first_resptime_max_object.best_cutoff_2)

cust_first_resptime_max_object.best_cutoff_4 =  best_cust_first_resptime_max_curve['cutoff_4'] * (-1)
print('cutoff 4:', cust_first_resptime_max_object.best_cutoff_4)

cust_first_resptime_max_object.best_cutoff_5 =  best_cust_first_resptime_max_curve['cutoff_5'] * (-1)
print('cutoff 5:', cust_first_resptime_max_object.best_cutoff_5)

# %% Save the output (object) in a binary file
cust_first_resptime_max_object.save_object('chatlp_cust_first_resptime_max_score.obj')

# ================ cust_first_resptime_avg ==========================
df = pd.read_csv('chatlp_raw_data.csv')
# Filter for a single metric
df = df.loc[df.metricname == 'cust_first_resptime_avg']

# Filter for the appropriate typology
df = df.loc[df.primary_high_level_reason == 'all']

# Rename the main column
df.rename(columns={'value':'cust_first_resptime_avg'}, inplace=True)

# Remove nulls
print(len(df))
df = df.loc[df.cust_first_resptime_avg.notnull()]
print(len(df))

# %% Instantiate Metadata (class object for threshold build)
cust_first_resptime_avg_object = tbs.Threshold_Build(
    metricname = 'cust_first_resptime_avg', 
    channel = 'chatlp', 
    input_df = df.copy(deep=True)
)

# %% Analyze exisiting relationship: Plot Bivariate
# bins = [i for i in range(-7200,-9,60)]

cust_first_resptime_avg_object.plot_churn(
    bins = [i for i in range(0,1500,1)]
)

# %% Explicitly record the relationship to churn
cust_first_resptime_avg_object.relationship_to_churn = 'negative'

cust_first_resptime_avg_object.input_df['cust_first_resptime_avg'] = cust_first_resptime_avg_object.input_df['cust_first_resptime_avg'] * (-1)

# %% Split the input dataframe
cust_first_resptime_avg_object.split = (None, None)

# %% Create universe of potential thresholds

cust_first_resptime_avg_object.create_3(
    list_of_thresholds = [-i for i in range(0, 600)]
)

# %% Establish cutoffs for each potential threshold
cust_first_resptime_avg_object.create_1_5(
    ## think of argument name for how many percentiles to loop through
)

cust_first_resptime_avg_object.create_2_4(
    ## create argument for how many potential 2s and 4s we cycle through
)


# %% Create bins
cust_first_resptime_avg_object.create_bins(
        input_df = cust_first_resptime_avg_object.cutoff_universe_df,
        bin_definitions = {
                'bin_1':['1','btwn_1_2'],
                'bin_2':['2','btwn_2_3'],
                'bin_3':['3','btwn_3_4'],
                'bin_4':['4','btwn_4_5','5']
        }
)

# %% Validity Checks
cust_first_resptime_avg_object.cutoff_validity_checks(
    num_bins = 4,
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'], 
    check_monotonic = True, 
    check_pop_size = True,
    min_pop_size = (sum(cust_first_resptime_avg_object.input_df.events) * 0.01)
)

# Looking at the number of valid curves
print('Total Completely Valid Cutoffs:',
    sum((cust_first_resptime_avg_object.cutoff_universe_df['is_monotonic'] == 'Y') &
    (cust_first_resptime_avg_object.cutoff_universe_df['is_valid_bin_size'] == 'Y')),
    '\nTotal Monotonic Cutoffs:',
    sum((cust_first_resptime_avg_object.cutoff_universe_df['is_monotonic'] == 'Y')),
    '\nTotal Cutoffs with Valid Bin Sizes:',
    sum((cust_first_resptime_avg_object.cutoff_universe_df['is_valid_bin_size'] == 'Y'))
)


# %% Define ranking columns for shortlisting the best curves
print('Ranking using good vs bad')
cust_first_resptime_avg_object.define_ranking(
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'],
    ranking_formula = 'good_v_bad',
    ranking_column = 'good_v_bad_rank',
    ranking_method = 'dense',
    rounding_factor = 3
)

print('Ranking using dif between extremes')
cust_first_resptime_avg_object.define_ranking(
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'],
    ranking_formula = 'dif_in_extremes',
    ranking_column = 'dif_in_extremes_rank',
    ranking_method = 'dense',
    rounding_factor = 4
)

print('Ranking using pairwise ratio')
cust_first_resptime_avg_object.define_ranking(
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'],
    ranking_formula = 'pairwise_ratio',
    ranking_column = 'pairwise_ratio_rank',
    ranking_method = 'dense',
    rounding_factor = 1
)

print('Ranking using sum of square errors')
cust_first_resptime_avg_object.define_ranking(
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'],
    ranking_formula = 'sum_square_errors',
    ranking_column = 'sum_square_errors_rank',
    ranking_method = 'dense',
    rounding_factor = None      # Does not round the squared errors
)

# %% Saving curve dataframe to csv for inspection and manual curve selection
cust_first_resptime_avg_object.cutoff_universe_df.sort_values(
    by = [
        'is_monotonic',
        'is_valid_bin_size',
        'pairwise_ratio_rank',
        'dif_in_extremes_rank',
        'sum_square_errors_rank'
    ],
    ascending = [
        False,
        False,
        True,
        True,
        True
    ],
    inplace = True
)

# Save sorted dataframe as csv
cust_first_resptime_avg_object.cutoff_universe_df.to_csv('chatlp_cust_first_resptime_avg_cutoffs.csv')

# %% Establish best threshold (pick final candidate)

# Top row is the winner
best_cust_first_resptime_avg_curve = cust_first_resptime_avg_object.cutoff_universe_df.iloc[0,:]

#%%
# Assign cutoffs from the winner to object attributes
cust_first_resptime_avg_object.best_threshold = best_cust_first_resptime_avg_curve['threshold'] * (-1)
print('threshold:', cust_first_resptime_avg_object.best_threshold)

cust_first_resptime_avg_object.best_cutoff_1 =  best_cust_first_resptime_avg_curve['cutoff_1'] * (-1)
print('cutoff 1:', cust_first_resptime_avg_object.best_cutoff_1)

cust_first_resptime_avg_object.best_cutoff_2 =  best_cust_first_resptime_avg_curve['cutoff_2'] * (-1)
print('cutoff 2:', cust_first_resptime_avg_object.best_cutoff_2)

cust_first_resptime_avg_object.best_cutoff_4 =  best_cust_first_resptime_avg_curve['cutoff_4'] * (-1)
print('cutoff 4:', cust_first_resptime_avg_object.best_cutoff_4)

cust_first_resptime_avg_object.best_cutoff_5 =  best_cust_first_resptime_avg_curve['cutoff_5'] * (-1)
print('cutoff 5:', cust_first_resptime_avg_object.best_cutoff_5)

# ==== arr_resptime_sum ============
df = pd.read_csv('chatlp_raw_data.csv')
# Filter for a single metric
df = df.loc[df.metricname == 'arr_resptime_sum']

# Filter for the appropriate typology
df = df.loc[df.primary_high_level_reason == 'all']

# Rename the main column
df.rename(columns={'value':'arr_resptime_sum'}, inplace=True)

# Remove nulls
print(len(df))
df = df.loc[df.arr_resptime_sum.notnull()]
print(len(df))

# %% Instantiate Metadata (class object for threshold build)
arr_resptime_sum_object = tbs.Threshold_Build(
    metricname = 'arr_resptime_sum', 
    channel = 'chatlp', 
    input_df = df.copy(deep=True)
)

# %% Analyze exisiting relationship: Plot Bivariate
# bins = [i for i in range(-7200,-9,60)]

arr_resptime_sum_object.plot_churn(
    bins = [i for i in range(0,2600)]
)
# %% Explicitly record the relationship to churn
arr_resptime_sum_object.relationship_to_churn = 'positive'

arr_resptime_sum_object.input_df['arr_resptime_sum'] = arr_resptime_sum_object.input_df['arr_resptime_sum'] * (-1)

# %% Split the input dataframe
arr_resptime_sum_object.split = (None, None)

# %% Create universe of potential thresholds

arr_resptime_sum_object.create_3(
    list_of_thresholds = [-i for i in range(0, 1300, 10)]
)

# %% Establish cutoffs for each potential threshold
arr_resptime_sum_object.create_1_5(
    ## think of argument name for how many percentiles to loop through
)

arr_resptime_sum_object.create_2_4(
    ## create argument for how many potential 2s and 4s we cycle through
)


# %% Create bins
arr_resptime_sum_object.create_bins(
        input_df = arr_resptime_sum_object.cutoff_universe_df,
        bin_definitions = {
                'bin_1':['1','btwn_1_2'],
                'bin_2':['2','btwn_2_3'],
                'bin_3':['3','btwn_3_4'],
                'bin_4':['4','btwn_4_5','5']
        }
)

# %% Validity Checks
arr_resptime_sum_object.cutoff_validity_checks(
    num_bins = 4,
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'], 
    check_monotonic = True, 
    check_pop_size = True,
    min_pop_size = (sum(arr_resptime_sum_object.input_df.events) * 0.01)
)

# Looking at the number of valid curves
print('Total Completely Valid Cutoffs:',
    sum((arr_resptime_sum_object.cutoff_universe_df['is_monotonic'] == 'Y') &
    (arr_resptime_sum_object.cutoff_universe_df['is_valid_bin_size'] == 'Y')),
    '\nTotal Monotonic Cutoffs:',
    sum((arr_resptime_sum_object.cutoff_universe_df['is_monotonic'] == 'Y')),
    '\nTotal Cutoffs with Valid Bin Sizes:',
    sum((arr_resptime_sum_object.cutoff_universe_df['is_valid_bin_size'] == 'Y'))
)


# %% Define ranking columns for shortlisting the best curves
print('Ranking using good vs bad')
arr_resptime_sum_object.define_ranking(
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'],
    ranking_formula = 'good_v_bad',
    ranking_column = 'good_v_bad_rank',
    ranking_method = 'dense',
    rounding_factor = 3
)

print('Ranking using dif between extremes')
arr_resptime_sum_object.define_ranking(
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'],
    ranking_formula = 'dif_in_extremes',
    ranking_column = 'dif_in_extremes_rank',
    ranking_method = 'dense',
    rounding_factor = 4
)

print('Ranking using pairwise ratio')
arr_resptime_sum_object.define_ranking(
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'],
    ranking_formula = 'pairwise_ratio',
    ranking_column = 'pairwise_ratio_rank',
    ranking_method = 'dense',
    rounding_factor = 1
)

print('Ranking using sum of square errors')
arr_resptime_sum_object.define_ranking(
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'],
    ranking_formula = 'sum_square_errors',
    ranking_column = 'sum_square_errors_rank',
    ranking_method = 'dense',
    rounding_factor = None      # Does not round the squared errors
)

# %% Saving curve dataframe to csv for inspection and manual curve selection
arr_resptime_sum_object.cutoff_universe_df.sort_values(
    by = [
        'is_monotonic',
        'is_valid_bin_size',
        'pairwise_ratio_rank',
        'dif_in_extremes_rank',
        'sum_square_errors_rank'
    ],
    ascending = [
        False,
        False,
        True,
        True,
        True
    ],
    inplace = True
)

# Save sorted dataframe as csv
arr_resptime_sum_object.cutoff_universe_df.to_csv('chatlp_arr_resptime_sum_cutoffs.csv')

# %% Establish best threshold (pick final candidate)
# Top row is the winner
best_arr_resptime_sum_curve = arr_resptime_sum_object.cutoff_universe_df.iloc[0,:]

#%%
# Assign cutoffs from the winner to object attributes
arr_resptime_sum_object.best_threshold = best_arr_resptime_sum_curve['threshold'] * (-1)
print('threshold:', arr_resptime_sum_object.best_threshold)

arr_resptime_sum_object.best_cutoff_1 =  best_arr_resptime_sum_curve['cutoff_1'] * (-1)
print('cutoff 1:', arr_resptime_sum_object.best_cutoff_1)

arr_resptime_sum_object.best_cutoff_2 =  best_arr_resptime_sum_curve['cutoff_2'] * (-1)
print('cutoff 2:', arr_resptime_sum_object.best_cutoff_2)

arr_resptime_sum_object.best_cutoff_4 =  best_arr_resptime_sum_curve['cutoff_4'] * (-1)
print('cutoff 4:', arr_resptime_sum_object.best_cutoff_4)

arr_resptime_sum_object.best_cutoff_5 =  best_arr_resptime_sum_curve['cutoff_5'] * (-1)
print('cutoff 5:', arr_resptime_sum_object.best_cutoff_5)

# ============= art_resptime_sum ==============
df = pd.read_csv('chatlp_raw_data.csv')
# Filter for a single metric
df = df.loc[df.metricname == 'art_resptime_sum']

# Filter for the appropriate typology
df = df.loc[df.primary_high_level_reason == 'all']

# Rename the main column
df.rename(columns={'value':'art_resptime_sum'}, inplace=True)

# Remove nulls
print(len(df))
df = df.loc[df.art_resptime_sum.notnull()]
print(len(df))

# %% Instantiate Metadata (class object for threshold build)
art_resptime_sum_object = tbs.Threshold_Build(
    metricname = 'art_resptime_sum', 
    channel = 'chatlp', 
    input_df = df.copy(deep=True)
)

# %% Analyze exisiting relationship: Plot Bivariate
# bins = [i for i in range(-7200,-9,60)]

art_resptime_sum_object.plot_churn(
    bins = [i for i in range(0,5000)]
)

# %% Explicitly record the relationship to churn
art_resptime_sum_object.relationship_to_churn = 'positive'

art_resptime_sum_object.input_df['arr_resptime_sum'] = art_resptime_sum_object.input_df['art_resptime_sum'] * (-1)

# %% Split the input dataframe
art_resptime_sum_object.split = (None, None)

# %% Create universe of potential thresholds

art_resptime_sum_object.create_3(
    list_of_thresholds = [-i for i in range(0, 2600, 10)]
)

# %% Establish cutoffs for each potential threshold
art_resptime_sum_object.create_1_5(
    ## think of argument name for how many percentiles to loop through
)

art_resptime_sum_object.create_2_4(
    ## create argument for how many potential 2s and 4s we cycle through
)


# %% Create bins
art_resptime_sum_object.create_bins(
        input_df = art_resptime_sum_object.cutoff_universe_df,
        bin_definitions = {
                'bin_1':['1','btwn_1_2'],
                'bin_2':['2','btwn_2_3'],
                'bin_3':['3','btwn_3_4'],
                'bin_4':['4','btwn_4_5','5']
        }
)

# %% Validity Checks
art_resptime_sum_object.cutoff_validity_checks(
    num_bins = 4,
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'], 
    check_monotonic = True, 
    check_pop_size = True,
    min_pop_size = (sum(art_resptime_sum_object.input_df.events) * 0.01)
)

# Looking at the number of valid curves
print('Total Completely Valid Cutoffs:',
    sum((art_resptime_sum_object.cutoff_universe_df['is_monotonic'] == 'Y') &
    (art_resptime_sum_object.cutoff_universe_df['is_valid_bin_size'] == 'Y')),
    '\nTotal Monotonic Cutoffs:',
    sum((art_resptime_sum_object.cutoff_universe_df['is_monotonic'] == 'Y')),
    '\nTotal Cutoffs with Valid Bin Sizes:',
    sum((art_resptime_sum_object.cutoff_universe_df['is_valid_bin_size'] == 'Y'))
)


# %% Define ranking columns for shortlisting the best curves
print('Ranking using good vs bad')
art_resptime_sum_object.define_ranking(
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'],
    ranking_formula = 'good_v_bad',
    ranking_column = 'good_v_bad_rank',
    ranking_method = 'dense',
    rounding_factor = 3
)

print('Ranking using dif between extremes')
art_resptime_sum_object.define_ranking(
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'],
    ranking_formula = 'dif_in_extremes',
    ranking_column = 'dif_in_extremes_rank',
    ranking_method = 'dense',
    rounding_factor = 4
)

print('Ranking using pairwise ratio')
art_resptime_sum_object.define_ranking(
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'],
    ranking_formula = 'pairwise_ratio',
    ranking_column = 'pairwise_ratio_rank',
    ranking_method = 'dense',
    rounding_factor = 1
)

print('Ranking using sum of square errors')
art_resptime_sum_object.define_ranking(
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'],
    ranking_formula = 'sum_square_errors',
    ranking_column = 'sum_square_errors_rank',
    ranking_method = 'dense',
    rounding_factor = None      # Does not round the squared errors
)

# %% Saving curve dataframe to csv for inspection and manual curve selection
art_resptime_sum_object.cutoff_universe_df.sort_values(
    by = [
        'is_monotonic',
        'is_valid_bin_size',
        'pairwise_ratio_rank',
        'dif_in_extremes_rank',
        'sum_square_errors_rank'
    ],
    ascending = [
        False,
        False,
        True,
        True,
        True
    ],
    inplace = True
)

# Save sorted dataframe as csv
art_resptime_sum_object.cutoff_universe_df.to_csv('chatlp_art_resptime_sum_cutoffs.csv')

# %% Establish best threshold (pick final candidate)
# Top row is the winner
best_art_resptime_sum_curve = art_resptime_sum_object.cutoff_universe_df.iloc[0,:]

#%%
# Assign cutoffs from the winner to object attributes
art_resptime_sum_object.best_threshold = best_art_resptime_sum_curve['threshold'] * (-1)
print('threshold:', art_resptime_sum_object.best_threshold)

art_resptime_sum_object.best_cutoff_1 =  best_art_resptime_sum_curve['cutoff_1'] * (-1)
print('cutoff 1:', art_resptime_sum_object.best_cutoff_1)

art_resptime_sum_object.best_cutoff_2 =  best_art_resptime_sum_curve['cutoff_2'] * (-1)
print('cutoff 2:', art_resptime_sum_object.best_cutoff_2)

art_resptime_sum_object.best_cutoff_4 =  best_art_resptime_sum_curve['cutoff_4'] * (-1)
print('cutoff 4:', art_resptime_sum_object.best_cutoff_4)

art_resptime_sum_object.best_cutoff_5 =  best_art_resptime_sum_curve['cutoff_5'] * (-1)
print('cutoff 5:', art_resptime_sum_object.best_cutoff_5)

# ============= total_msg =================
df = pd.read_csv('chatlp_raw_data.csv')
# Filter for a single metric
df = df.loc[df.metricname == 'total_msg']

# Filter for the appropriate typology
df = df.loc[df.primary_high_level_reason == 'all']

# Rename the main column
df.rename(columns={'value':'total_msg'}, inplace=True)

# Remove nulls
print(len(df))
df = df.loc[df.total_msg.notnull()]
print(len(df))

# %% Instantiate Metadata (class object for threshold build)
total_msg_object = tbs.Threshold_Build(
    metricname = 'total_msg', 
    channel = 'chatlp', 
    input_df = df.copy(deep=True)
)

# %% Analyze exisiting relationship: Plot Bivariate
# bins = [i for i in range(-7200,-9,60)]

total_msg_object.plot_churn(
    bins = [i for i in range(0,150,1)]
)

# %% Explicitly record the relationship to churn
total_msg_object.relationship_to_churn = 'positive'

total_msg_object.input_df['total_msg'] = total_msg_object.input_df['total_msg'] * (-1)

# %% Split the input dataframe
total_msg_object.split = (None, None)

# %% Create universe of potential thresholds

total_msg_object.create_3(
    list_of_thresholds = [-i for i in range(0, 25, 1)]
)

# %% Establish cutoffs for each potential threshold
total_msg_object.create_1_5(
    ## think of argument name for how many percentiles to loop through
)

total_msg_object.create_2_4(
    ## create argument for how many potential 2s and 4s we cycle through
)


# %% Create bins
total_msg_object.create_bins(
        input_df = total_msg_object.cutoff_universe_df,
        bin_definitions = {
                'bin_1':['1','btwn_1_2'],
                'bin_2':['2','btwn_2_3'],
                'bin_3':['3','btwn_3_4'],
                'bin_4':['4','btwn_4_5','5']
        }
)

# %% Validity Checks
total_msg_object.cutoff_validity_checks(
    num_bins = 4,
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'], 
    check_monotonic = True, 
    check_pop_size = True,
    min_pop_size = (sum(total_msg_object.input_df.events) * 0.01)
)

# Looking at the number of valid curves
print('Total Completely Valid Cutoffs:',
    sum((total_msg_object.cutoff_universe_df['is_monotonic'] == 'Y') &
    (total_msg_object.cutoff_universe_df['is_valid_bin_size'] == 'Y')),
    '\nTotal Monotonic Cutoffs:',
    sum((total_msg_object.cutoff_universe_df['is_monotonic'] == 'Y')),
    '\nTotal Cutoffs with Valid Bin Sizes:',
    sum((total_msg_object.cutoff_universe_df['is_valid_bin_size'] == 'Y'))
)


# %% Define ranking columns for shortlisting the best curves
print('Ranking using good vs bad')
total_msg_object.define_ranking(
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'],
    ranking_formula = 'good_v_bad',
    ranking_column = 'good_v_bad_rank',
    ranking_method = 'dense',
    rounding_factor = 3
)

print('Ranking using dif between extremes')
total_msg_object.define_ranking(
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'],
    ranking_formula = 'dif_in_extremes',
    ranking_column = 'dif_in_extremes_rank',
    ranking_method = 'dense',
    rounding_factor = 4
)

print('Ranking using pairwise ratio')
total_msg_object.define_ranking(
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'],
    ranking_formula = 'pairwise_ratio',
    ranking_column = 'pairwise_ratio_rank',
    ranking_method = 'dense',
    rounding_factor = 1
)

print('Ranking using sum of square errors')
total_msg_object.define_ranking(
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'],
    ranking_formula = 'sum_square_errors',
    ranking_column = 'sum_square_errors_rank',
    ranking_method = 'dense',
    rounding_factor = None      # Does not round the squared errors
)

# %% Saving curve dataframe to csv for inspection and manual curve selection
total_msg_object.cutoff_universe_df.sort_values(
    by = [
        'is_monotonic',
        'is_valid_bin_size',
        'pairwise_ratio_rank',
        'dif_in_extremes_rank',
        'sum_square_errors_rank'
    ],
    ascending = [
        False,
        False,
        True,
        True,
        True
    ],
    inplace = True
)

# Save sorted dataframe as csv
total_msg_object.cutoff_universe_df.to_csv('chatlp_total_msg_cutoffs.csv')

# %% Establish best threshold (pick final candidate)

# Top row is the winner
best_total_msg_curve = total_msg_object.cutoff_universe_df.iloc[0,:]

#%%
# Assign cutoffs from the winner to object attributes
total_msg_object.best_threshold = best_total_msg_curve['threshold'] * (-1)
print('threshold:', total_msg_object.best_threshold)

total_msg_object.best_cutoff_1 =  best_total_msg_curve['cutoff_1'] * (-1)
print('cutoff 1:', total_msg_object.best_cutoff_1)

total_msg_object.best_cutoff_2 =  best_total_msg_curve['cutoff_2'] * (-1)
print('cutoff 2:', total_msg_object.best_cutoff_2)

total_msg_object.best_cutoff_4 =  best_total_msg_curve['cutoff_4'] * (-1)
print('cutoff 4:', total_msg_object.best_cutoff_4)

total_msg_object.best_cutoff_5 =  best_total_msg_curve['cutoff_5'] * (-1)
print('cutoff 5:', total_msg_object.best_cutoff_5)
