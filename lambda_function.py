import sys, os, json
from pathlib import Path

print(f'Importing my code...')
sys.path.append("/function/ngen_forcing")
print(help('modules'))
from prep_hydrofab_forcings_ngen import prep_ngen_data

def handler(event, context):

    print(f'Made it to handler!')

    # load template config
    conf = json.load(open('./ngen_forcing/ngen_forcings_lambda.json'))

    # get date from event

    # modify config

    # call function
    prep_ngen_data(conf)

    print('TRY v9' + sys.version + '!')
    return 'TRY v9' + sys.version + '!'
