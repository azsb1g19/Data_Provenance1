# Necessary packages
from dataprov.prov_acquisition.prov_libraries import ProvenanceTracker
import pandas as pd
import numpy as np
import time
import argparse
import os
from dataprov.new_queries import create_mongo_pandas

def main(opt):
	prog_dir = os.path.dirname(os.path.realpath(__file__))
	input_path = os.path.join(prog_dir, 'Datasets\\census.csv')
	savepath = os.path.join(prog_dir, 'Results\\census')

	df = pd.read_csv(input_path)

	# Assign names to columns
	names = ['age', 'workclass', 'fnlwgt', 'education', 'education-num', 'marital-status', 'occupation', 'relationship', 'race', 'sex', 'capital-gain', 'capital-loss', 'hours-per-week', 'native-country', 'label']

	df.columns = names

	# create provenance tracker
	tracker=ProvenanceTracker.ProvenanceTracker(df, '', savepath)
	tracker.df.to_csv(os.path.join(prog_dir, 'temp.csv'))

	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Initialization')
	
	#OPERATION 0
	tracker.set_description('Cleanup names from spaces')
	col = ['workclass', 'education', 'marital-status', 'occupation', 'relationship', 'race', 'sex', 'native-country', 'label']

	df = tracker.df.copy()
	for c in col:
		df[c] = df[c].map(str.strip)
	tracker.df = df

	#OPERATION 1
	#tracker.set_description('Replace ? character for NaN value')
	#tracker.df = tracker.df.replace('?', np.nan)

	#OPERATION 2-3
	tracker.set_description('One-hot encode categorical variables')
	dummies = []
	dummies.append(pd.get_dummies(pd.DataFrame(tracker.df, columns=['workclass', 'education', 'marital-status', 'occupation', 'relationship', 'race', 'native-country'])))
	df_dummies = pd.concat(dummies, axis=1)
	tracker.df = pd.concat((tracker.df, df_dummies), axis=1)
	tracker.stop_space_prov(['workclass', 'education', 'marital-status', 'occupation', 'relationship', 'race', 'native-country'])

	#OPERATION 4
	print("4")
	tracker.set_description('Assign sex and label binary values 0 and 1')
	df = tracker.df.copy()
	df.sex = df.sex.replace('Male', 1)
	df.sex = df.sex.replace('Female', 0)
	df.label = df.label.replace('<=50K', 0)
	df.label = df.label.replace('>50K', 1)
	tracker.df = df

	#OPERATION 5
	tracker.set_description('Drop fnlwgt variable')
	tracker.df = tracker.df.drop(columns=['fnlwgt'], axis=1)

	#Save to DB
	print("Save to DB")
	create_mongo_pandas.main('census_py_db', savepath)

	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Prov saved')

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-op', dest='opt', action='store_true', help='Use the optimized capture')
	args = parser.parse_args()
	main(args.opt)