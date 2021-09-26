# %% Changing Directories and Importing Modules 
import os
import sys
os.chdir('/vzwhome/sringpr/qes-data/chatlp')
os.getcwd()
print('Working in: ',os.getcwd())
import pandas as pd
import numpy as np
from pyhive import hive
sys.path.append(os.path.abspath(os.path.join('/vzwhome/sringpr', 'qesmetric/cross_channel_features/thresholds_scripts')))
import threshold_build_service as tbs

# %% Initialize parameters

# Variable to control print statements in the script
verbose = True

# %% Data Preprocessing
# Read csv data
metricname = 'nature_score'
tbl = 'chat_nature_scores'
metricquery = 'round(a.nature_score * 100,0)'
file_name = 'chat_nature_score.csv'

df = pd.read_csv('chatlp_nature_score.csv')
df = df.loc[df[metricname].notnull(),:]

# %% Instantiate threshold build object to calculate thresholds
nature = tbs.Threshold_Build(
    channel = 'chatlp',
    metricname = 'nature_score',
    input_df = df.copy(deep=True)
)

nature.typology = 'all'

# If the object is a curve that is split in two, define the (lower bound, upper bound) in the tuple below. If not, both None
nature.split = (None, None)
# %% Plot existing relationship to churn
nature.plot_churn(
    bins = [i for i in range(-300,200,1)]
)
nature.relationship_to_churn = 'negative'

# %% Defining universe of potential thresholds
uniques = nature.input_df['nature_score'].unique()

valids = uniques[np.isfinite(uniques)]

inliers = valids[(valids >= -60) & (valids <= 50)] 

values_to_test = np.sort(inliers)[::1]

if verbose: print('Here are the thresholds to test: \n',values_to_test)

nature.create_3(
    list_of_thresholds = values_to_test
)

# %% Establish cutoffs for each potential threshold
nature.create_1_5(
    ## think of argument name for how many percentiles to loop through
)
nature.create_2_4(
    ## create argument for how many potential 2s and 4s we cycle through
)

# %% Create bins
nature.create_bins(
        input_df = nature.cutoff_universe_df,
        bin_definitions = {
                'bin_1':['1','btwn_1_2'],
                'bin_2':['2','btwn_2_3'],
                'bin_3':['3','btwn_3_4'],
                'bin_4':['4','btwn_4_5','5']
        }
)

# %% Validity Checks
nature.cutoff_validity_checks(
    num_bins = 4,
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'], 
    check_monotonic = True, 
    check_pop_size = True,
    min_pop_size = int(sum(nature.input_df.events) * 0.05)
)

# Looking at the number of valid curves
print(
    sum((nature.cutoff_universe_df['is_monotonic'] == 'Y') &
    (nature.cutoff_universe_df['is_valid_bin_size'] == 'Y'))
)



# %% Define ranking columns for shortlisting the best curves
# print('Ranking using good vs bad')
# nature.define_ranking(
#     ordered_bin_suffixes = [
#         'bin_1',
#         'bin_2',
#         'bin_3',
#         'bin_4'],
#     ranking_formula = 'good_v_bad',
#     ranking_column = 'good_v_bad_rank',
#     ranking_method = 'dense',
#     rounding_factor = 3
# )

print('Ranking using dif between extremes')
nature.define_ranking(
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
nature.define_ranking(
    ordered_bin_suffixes = [
        'bin_1',
        'bin_2',
        'bin_3',
        'bin_4'],
    ranking_formula = 'pairwise_ratio',
    ranking_column = 'pairwise_ratio_rank',
    ranking_method = 'dense',
    rounding_factor = 8
)

print('Ranking using sum of square errors')
nature.define_ranking(
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
nature.cutoff_universe_df.sort_values(
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

best_nature_curve = nature.cutoff_universe_df.iloc[0,:]
ten_thousand = nature.cutoff_universe_df.iloc[0:10000,:]

nature.cutoff_universe_df.to_csv('chatlp_customer_nature_cutoffs.csv')
ten_thousand.to_csv('chatlp_customer_nature_cutoffs_10000.csv')
# %% Establish best threshold (pick final candidate)

# Top row is the winner
best_nature_curve = nature.cutoff_universe_df.iloc[0,:]

# Assign cutoffs from the winner to object attributes
nature.best_threshold = best_nature_curve['threshold']
print('threshold:', nature.best_threshold)

nature.best_cutoff_1 =  best_nature_curve['cutoff_1']
print('cutoff 1:', nature.best_cutoff_1)

nature.best_cutoff_2 =  best_nature_curve['cutoff_2']
print('cutoff 2:', nature.best_cutoff_2)

nature.best_cutoff_4 =  best_nature_curve['cutoff_4']
print('cutoff 4:', nature.best_cutoff_4)

nature.best_cutoff_5 =  best_nature_curve['cutoff_5']
print('cutoff 5:', nature.best_cutoff_5)

# %% Save the output (object) in a binary file
nature.save_object('chatlp_nature_qes_score.obj')

# %% Example code to load object
nature1 = tbs.load_object('chatlp_nature_qes_score.obj')

# %% Storing object in the overall channel binary file
chatlp = tbs.load_object('chatlp_channel_overall.obj')

chatlp.nature_score = nature

chatlp.save_object('chatlp_channel_overall.obj')

#%% Calculate 70_2 result

# =IF(OR(AC2>SUM(AB2:AE2)*0.7,AE2<SUM(AB2:AE2)*0.02),0,1)
ax = pd.read_csv('chatlp_customer_nature_cutoffs.csv')


filter_70 = ax[ax['events_bin_2'] <
                                 (ax['events_bin_1'] +
                                  ax['events_bin_2'] + 
                                  ax['events_bin_3'] +
                                  ax['events_bin_4'] )*0.7]

filter_2 = filter_70[(filter_70['events_bin_1'] > 
                                 (filter_70['events_bin_1'] +
                                  filter_70['events_bin_2'] + 
                                  filter_70['events_bin_3'] +
                                  filter_70['events_bin_4'] )*0.02)
                        &
                        (filter_70['events_bin_3'] > 
                                 (filter_70['events_bin_1'] +
                                  filter_70['events_bin_2'] + 
                                  filter_70['events_bin_3'] +
                                  filter_70['events_bin_4'] )*0.02)
                        &
                        (filter_70['events_bin_4'] > 
                                 (filter_70['events_bin_1'] +
                                  filter_70['events_bin_2'] + 
                                  filter_70['events_bin_3'] +
                                  filter_70['events_bin_4'] )*0.02)]

print(filter_2.iloc[0,:])
#%%
