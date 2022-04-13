# Necessary packages
from dataprov.prov_acquisition.prov_libraries import ProvenanceTracker
import pandas as pd
import os
from dataprov.new_queries import create_mongo_pandas

# n.b
# If both key columns contain rows where the key is a null value, those rows will be matched against each other.
# This is different from usual SQL join behaviour and can lead to unexpected results.

if __name__ == '__main__':
    prog_dir = os.path.dirname(os.path.realpath(__file__))
    savepath = os.path.join(prog_dir, 'Results\\joins')
    df_a = pd.read_csv(os.path.join(prog_dir,'Datasets\\table_a.csv'))
    df_b = pd.read_csv(os.path.join(prog_dir,'Datasets\\table_b.csv'))

    #left join 
    tracker=ProvenanceTracker.ProvenanceTracker(df_a, '', os.path.join(savepath, 'left_join'))
    tracker.set_description("Left Join on Column B")
    tracker.add_second_df(df_b)
    tracker.df = tracker.df.merge(df_b, on='B', how='left')
    create_mongo_pandas.main('left_join_py_db', savepath)

    #right join
    tracker=ProvenanceTracker.ProvenanceTracker(df_a, '', os.path.join(savepath, 'right_join'))
    tracker.set_description("Right Join on Column B")
    tracker.add_second_df(df_b)
    tracker.df = tracker.df.merge(df_b, on='B', how='right')
    create_mongo_pandas.main('right_join_py_db', savepath)

    #inner join
    tracker=ProvenanceTracker.ProvenanceTracker(df_a, '', os.path.join(savepath, 'inner_join'))
    tracker.set_description("Inner Join on Column B")
    tracker.add_second_df(df_b)
    tracker.df = tracker.df.merge(df_b, on='B', how='inner')
    create_mongo_pandas.main('inner_join_py_db', savepath)

    #full outer join
    tracker=ProvenanceTracker.ProvenanceTracker(df_a, '', os.path.join(savepath, 'full_outer_join'))
    tracker.set_description("Full Outer Join")
    tracker.add_second_df(df_b)
    tracker.df = tracker.df.merge(df_b, how='outer')
    create_mongo_pandas.main('oute_join_py_db', savepath)

    #cross join
    tracker=ProvenanceTracker.ProvenanceTracker(df_a, '', os.path.join(savepath, 'cross_join'))
    tracker.set_description("Cross Join")
    tracker.add_second_df(df_b)
    tracker.df = tracker.df.merge(df_b, how='cross')
    create_mongo_pandas.main('cross_join_py_db', savepath)