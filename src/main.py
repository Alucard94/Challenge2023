from utils import generate_dataframe_from_daily_file, start_glue
import argparse

parser = argparse.ArgumentParser(description='TEST INGESTION')
parser.add_argument('--loadMethod', help='type of load', required=True)

args = vars(parser.parse_args())

loadMethod = str(args['loadMethod'])

if loadMethod == 'INCREMENTAL':
	generate_dataframe_from_daily_file('data')
	start_glue("incremental_load")

if loadMethod == 'FULL':
	print("TO BE DONE")
