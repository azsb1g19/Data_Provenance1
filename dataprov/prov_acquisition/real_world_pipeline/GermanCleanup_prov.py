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
	input_path = os.path.join(prog_dir, 'Datasets\\german.csv')
	savepath = os.path.join(prog_dir, 'Results\\german') 

	df = pd.read_csv(input_path, header=0)

	# create provenance tracker
	tracker=ProvenanceTracker.ProvenanceTracker(df, '', savepath)

	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Initialization')
	
	#OPERATION 0
	tracker.set_description('Turn criptic values into interpretable form')
	tracker.df = tracker.df.replace({'checking': {'A11': 'check_low', 'A12': 'check_mid', 'A13': 'check_high',
	                              'A14': 'check_none'},
	                 'credit_history': {'A30': 'debt_none', 'A31': 'debt_noneBank',
	                                    'A32': 'debt_onSchedule','A33': 'debt_delay',
	                                    'A34': 'debt_critical'},
	                 'purpose': {'A40': 'pur_newCar', 'A41': 'pur_usedCar',
	                             'A42': 'pur_furniture', 'A43': 'pur_tv',
	                             'A44': 'pur_appliance', 'A45': 'pur_repairs',
	                             'A46': 'pur_education', 'A47': 'pur_vacation',
	                             'A48': 'pur_retraining', 'A49': 'pur_business',
	                             'A410': 'pur_other'},
	                 'savings': {'A61': 'sav_small', 'A62': 'sav_medium', 'A63': 'sav_large',
	                             'A64': 'sav_xlarge', 'A65': 'sav_none'},
	                 'employment': {'A71': 'emp_unemployed', 'A72': 'emp_lessOne',
	                                'A73': 'emp_lessFour', 'A74': 'emp_lessSeven',
	                                'A75': 'emp_moreSeven'},
	                 'other_debtors': {'A101': 'debtor_none', 'A102': 'debtor_coApp',
	                                   'A103': 'debtor_guarantor'},
	                 'property': {'A121': 'prop_realEstate', 'A122': 'prop_agreement',
	                              'A123': 'prop_car', 'A124': 'prop_none'},
	                 'other_inst': {'A141': 'oi_bank', 'A142': 'oi_stores', 'A143': 'oi_none'},
	                 'housing': {'A151': 'hous_rent', 'A152': 'hous_own', 'A153': 'hous_free'},
	                 'job': {'A171': 'job_unskilledNR', 'A172': 'job_unskilledR',
	                         'A173': 'job_skilled', 'A174': 'job_highSkill'},
	                 'phone': {'A191': 0, 'A192': 1},
	                 'foreigner': {'A201': 1, 'A202': 0},
	                 'label': {2: 0}})

	#OPERATION 1
	tracker.set_description('More criptic values translating')
	df = tracker.df
	df['status'] = np.where(df.personal_status == 'A91', 'divorced',
	                        np.where(df.personal_status == 'A92', 'divorced', 
	                                 np.where(df.personal_status == 'A93', 'single',
	                                          np.where(df.personal_status == 'A95', 'single',
	                                                   'married'))))

	tracker.set_description('Translate gender values')
	df['gender'] = np.where(df.personal_status == 'A92', 0,
	                        np.where(df.personal_status == 'A95', 0,
	                                 1))
	tracker.df = df

	#OPERATION 2
	tracker.set_description('Drop personal_status column')
	tracker.df = tracker.df.drop(['personal_status'], axis=1)

	#OPERATION 3-13
	tracker.set_description('One-hot encode categorical columns')
	dummies = []
	dummies.append(pd.get_dummies(pd.DataFrame(tracker.df, columns=['checking', 'credit_history', 'purpose', 'savings', 'employment', 'other_debtors', 'property', 'other_inst', 'housing', 'job', 'status'])))
	df_dummies = pd.concat(dummies, axis=1)
	tracker.df = pd.concat((tracker.df, df_dummies), axis=1)
	tracker.stop_space_prov(['checking', 'credit_history', 'purpose', 'savings', 'employment', 'other_debtors', 'property', 'other_inst', 'housing', 'job', 'status'])
	print('[' + time.strftime("%d/%m-%H:%M:%S") +'] Prov saved')

	#Save to DB
	create_mongo_pandas.main('german_py_db', savepath)

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-op', dest='opt', action='store_true', help='Use the optimized capture')
	args = parser.parse_args()
	main(args.opt)