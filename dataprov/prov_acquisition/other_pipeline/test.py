import pandas as pd
from dataprov.prov_acquisition.prov_libraries import ProvenanceTracker

db_name = ''
savepath = 'D:\\My Documents\\1. SYNCTHING\\University Work\\3rd-year-project\\dataprov\\dataprov\\prov_acquisition\\real_world_pipeline\\testfolder'
table = pd.read_csv('Datasets\\test.csv')
tracker=ProvenanceTracker.ProvenanceTracker(table, db_name, savepath)
tracker.set_description("Drop column A")
tracker.df = tracker.df.drop('A', axis =1)
tracker.set_description("Drop first row")
tracker.df = tracker.df.drop(index=tracker.df.index[0], axis =0)