import sys, json
# from aws_lambda_powertools.utilities import parameters

def handler(event, context):

    # load template config
    conf = json.load(open('/function/ngen_forcing/ngen_forcings_lambda.json'))

    # get date from event

    # call function
    from ngen_forcing import prep_hydrofab_forcings_ngen
    prep_hydrofab_forcings_ngen.prep_ngen_data(conf)

    return 'Done!'
