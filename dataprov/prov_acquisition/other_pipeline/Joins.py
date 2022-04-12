# Necessary packages
from dataprov.prov_acquisition.prov_libraries import ProvenanceTracker
import pandas as pd

# n.b
# If both key columns contain rows where the key is a null value, those rows will be matched against each other.
# This is different from usual SQL join behaviour and can lead to unexpected results.

df_a = pd.read_csv('Datasets\\table_a.csv')
df_b = pd.read_csv('Datasets\\table_b.csv')

#left join 
tracker=ProvenanceTracker.ProvenanceTracker(df_a, '', 'left_join')
tracker.set_description("Left Join on Column B")
tracker.add_second_df(df_b)
tracker.df = tracker.df.merge(df_b, on='B', how='left')

#right join
tracker=ProvenanceTracker.ProvenanceTracker(df_a, '', 'right_join')
tracker.set_description("Right Join on Column B")
tracker.add_second_df(df_b)
tracker.df = tracker.df.merge(df_b, on='B', how='right')

#inner join
tracker=ProvenanceTracker.ProvenanceTracker(df_a, '', 'inner_join')
tracker.set_description("Inner Join on Column B")
tracker.add_second_df(df_b)
tracker.df = tracker.df.merge(df_b, on='B', how='inner')

#full outer join
tracker=ProvenanceTracker.ProvenanceTracker(df_a, '', 'full_outer_join')
tracker.set_description("Full Outer Join")
tracker.add_second_df(df_b)
tracker.df = tracker.df.merge(df_b, how='outer')

#cross join
tracker=ProvenanceTracker.ProvenanceTracker(df_a, '', 'cross_join')
tracker.set_description("Cross Join")
tracker.add_second_df(df_b)
tracker.df = tracker.df.merge(df_b, how='cross')