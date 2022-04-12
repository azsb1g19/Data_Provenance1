# Necessary packages
from dataprov.prov_acquisition.prov_libraries import ProvenanceTracker
import pandas as pd
import time
import argparse
import os
from dataprov.new_queries import create_mongo_pandas

def main(opt):
	prog_dir = os.path.dirname(os.path.realpath(__file__))
	input_path = os.path.join(prog_dir, 'Datasets\\compas.csv')
	savepath = os.path.join(prog_dir, 'Results\\compas') 

	df = pd.read_csv(input_path, header=0)

	# create provenance tracker
	tracker=ProvenanceTracker.ProvenanceTracker(df, '', savepath)

	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Initialization')
	
	# OPERATION O
	tracker.set_description('select relevant columns')
	tracker.df = tracker.df[['age', 'c_charge_degree', 'race', 'sex', 'priors_count', 'days_b_screening_arrest', 'two_year_recid', 'c_jail_in', 'c_jail_out']]

	# OPERATION 1
	tracker.set_description('Remove missing values')
	tracker.df = tracker.df.dropna()

	# OPERATION 2
	tracker.set_description('Make race binary')
	tracker.df.race = [0 if r != 'Caucasian' else 1 for r in tracker.df.race]

	# OPERATION 3
	tracker.set_description('Make two_year_recid the label')
	df = tracker.df
	df = df.rename({'two_year_recid': 'label'}, axis=1)
	df.label = [0 if l == 1 else 1 for l in df.label] # reverse label for consistency with function defs: 1 means no recid (good), 0 means recid (bad)
	tracker.df = df

	# OPERATION 4
	tracker.set_description('convert jailtime to days')
	tracker.df['jailtime'] = (pd.to_datetime(tracker.df.c_jail_out) - pd.to_datetime(tracker.df.c_jail_in)).dt.days

	# OPERATION 5
	tracker.set_description('drop jail in and out dates')
	tracker.df = tracker.df.drop(['c_jail_in', 'c_jail_out'], axis=1)
	
	# OPERATION 6
	tracker.set_description('M: misconduct, F: felony')
	tracker.df.c_charge_degree = [0 if s == 'M' else 1 for s in tracker.df.c_charge_degree]

	#Save to DB
	create_mongo_pandas.main('compas_py_db', savepath)

	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Prov saved')

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-op', dest='opt', action='store_true', help='Use the optimized capture')
	args = parser.parse_args()
	main(args.opt)