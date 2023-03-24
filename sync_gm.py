from boto3sts import credentials as creds
import boto3
import subprocess
import configparser
import logging 

config = configparser.ConfigParser()
config.read('cvmfs.cfg')

OIDC_PROFILE_NAME = config.get('database','OIDC_PROFILE_NAME')
ENDPOINT_URL = config.get('database','ENDPOINT_URL')
USER_NAME = config.get('database','USER_NAME')
HOST = config.get('database','HOST')

root_logger= logging.getLogger()
root_logger.setLevel(logging.WARNING)
handler = logging.FileHandler('/home/ubuntu/logs_cvmfs/%s.log' %USER_NAME, 'w', 'utf-8') 
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
root_logger.addHandler(handler)

#Getting your refreshble credentials session with the oidc-agent profile named infncloud
credentials = creds.s3_session_credentials(OIDC_PROFILE_NAME, endpoint=ENDPOINT_URL, verify=True)
ACCESS_KEY = credentials["access_key"]
SECRET_KEY = credentials["secret_key"]
TOKEN = credentials["token"]

#Starting the transaction in the repo
print('Starting transaction..')
cmd = 'cvmfs_server transaction %s.infn.it' %USER_NAME
p=subprocess.run(cmd, shell=True)
if p.returncode != 0:
	logging.error('Unable to start transaction')
    raise

#Downloading from the bucket only new or moidified files 
cmd = 's3cmd --access_key=%s --secret_key=%s --access_token=%s --host=%s --host-bucket=%s sync s3://%s/cvmfs/ /cvmfs/%s.infn.it/' % (ACCESS_KEY,SECRET_KEY,TOKEN,HOST,HOST,USER_NAME,USER_NAME)
p=subprocess.run(cmd, shell=True)
if p.returncode != 0:
    logging.warning('Problem in synchronizing') 

#Publishing changes in the repo, in case of data corruption abort transaction 
cmd= 'cvmfs_server publish %s.infn.it' %USER_NAME
p=subprocess.run(cmd, shell=True)
if p.returncode != 0:
    logging.warning('Unable to publish, aborting transaction..')
    cmd='cvmfs_server abort -f %s.infn.it' %USER_NAME
    p=subprocess.run(cmd, shell=True)
    if p.returncode != 0:
        logging.error('Unable to abort, repo still in trasansaction')
else:

    logging.info('Transaction, synchronization, publication succeded')