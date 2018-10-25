"__author__ = 'Samuel Kozuch'"
"__credits__ = 'Keboola 2018'"
"__project__ = 'ex-looker'"

"""
Python 3 environment 
"""

#import pip
#pip.main(['install', '--disable-pip-version-check', '--no-cache-dir', 'logging_gelf'])

import sys
import os
import logging
import json
import pandas as pd
import requests
import re
import logging_gelf.handlers
import logging_gelf.formatters
from keboola import docker



### Environment setup
abspath = os.path.abspath(__file__)
script_path = os.path.dirname(abspath)
os.chdir(script_path)
sys.tracebacklimit = 3

### Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)-8s : [line:%(lineno)3s] %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S")


logger = logging.getLogger()
logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(
    host=os.getenv('KBC_LOGGER_ADDR'),
    port=int(os.getenv('KBC_LOGGER_PORT'))
    )
logging_gelf_handler.setFormatter(logging_gelf.formatters.GELFFormatter(null_character=True))
logger.addHandler(logging_gelf_handler)

# removes the initial stdout logging
logger.removeHandler(logger.handlers[0])


### Access the supplied rules
cfg = docker.Config('/data/')
params = cfg.get_parameters()
url = params['url']
token = params['#token']
objects = params['objects']

logging.debug("Parameters:" + str(params))
logging.info("Successfully fetched all parameters.")

#logging.debug("Fetched parameters are :" + str(params))

### Get proper list of tables
cfg = docker.Config('/data/')
in_tables = cfg.get_input_tables()
out_tables = cfg.get_expected_output_tables()
logging.info("IN tables mapped: "+str(in_tables))
logging.info("OUT tables mapped: "+str(out_tables))

### destination to fetch and output files
DEFAULT_FILE_INPUT = "/data/in/tables/"
DEFAULT_TABLE_DESTINATION = "/data/out/tables/"

def fullmatch_re(pattern, string):
    match = re.fullmatch(pattern, string)

    if match:
        return True
    else:
        return False

def fetch_data(url, token, endpoint):
    """
    Function fetching the data
    """

    head = {'key': token}
    response = requests.get(url + endpoint, headers=head)
    
    if response.status_code == 200:
        return response.json()
    else:
        msg1 = "Could not download data. The process exited with:"
        msg2 = "%s %s" % (response.status_code, response.reason)
        logging.error(" ".join([msg1, msg2]))
        sys.exit(1)

def create_manifest(file_name, destination, primary_key, incremental):
    """
    Function for manifest creation.
    """

    file = '/data/out/tables/' + str(file_name) + '.manifest'

    manifest_template = {
                         "destination": str(destination),
                         "incremental": incremental,
                         "primary_key": primary_key
                        }

    manifest = manifest_template

    try:
        with open(file, 'w') as file_out:
            json.dump(manifest, file_out)
            logging.info("Output %s manifest file produced." % file_name)
    except Exception as e:
        logging.warn("Could not produce %s output file manifest." % file_name)
        logging.warn(e)

def json_to_csv(data, output_path, inconsistent=True):
    """
    Function, that will write json to csv if json is inconsistent
    """

    if inconsistent:
        #columns = list(data[next(iter(data))].keys())
        #clean_data = pd.DataFrame([], columns=columns)
        """
        for key in data.keys():
            row = data[key]
            logging.info(row)
            clean_data = pd.concat(
                pd.DataFrame([data[]])
            )
        """
        
        clean_data = pd.concat(
            [pd.DataFrame(dict(
                zip(data[key].keys(), [[value] for value in data[key].values()])
            )) for key in data.keys()],
            ignore_index=True
        )
        
        clean_data.to_csv(output_path, index=False)
        logging.info("Data was written to csv.")
        
    else:
        logging.info("Writing csv file.")
        pd.io.json.json_normalize(data).to_csv(output_path, index=False)
        
def main():
    for o in objects:
        endpoint = o['endpoint']
        destination = o['destination']
        pk = o['pk']
        incremental = o['incremental']

        pk = [col.strip() for col in pk.split(',')]

        bool = fullmatch_re(r'^(in|out)\.(c-)\w*\.[\w\-]*', destination)

        if bool:
            logging.debug("The table with endpoint {0} will be saved to {1}.".format(endpoint, destination))
        elif bool == False and len(destination) == 0:
            destination = "in.c-ecomail.data_%s" % endpoint
            logging.debug("The table with id {0} will be saved to {1}.".format(endpoint, destination))
        else:
            msg1 = "The name of the table %s contains unsupported chatacters." % destination
            msg2 = "Please provide a valid name with bucket and table name."
            logging.critical(" ".join([msg1, msg2]))
            sys.exit(1)

        data = fetch_data(url, token, endpoint)
        file_name = "data_%s.csv" % endpoint
        output_path = DEFAULT_TABLE_DESTINATION + file_name


        for key in pk:
            if (str(key) in list(data) and \
            key != ''):
                logging.info("%s will be used as primary key." % key)
                pk[pk.index(key)] = key.replace('.', '_')
            elif key == '':
                pk.remove(key)
            else:
                msg1 = "%s column is not in table columns. The column will be ommited as primary key." % key
                msg2 = "Available columns to be used as primary key are %s." % str(list(data))
                logging.warn(" ".join([msg1, msg2]))
                pk.remove(key)

        json_to_csv(data, output_path, True)
        create_manifest(file_name, destination, pk, incremental)

if __name__ == '__main__':
    main()

    logging.info("Script finished.")