from boto3sts import credentials as creds
import boto3
import subprocess
import os
from dotenv import load_dotenv

#Import from configuration file
load_dotenv()

OIDC_PROFILE_NAME = os.getenv('OIDC_PROFILE_NAME')
ENDPOINT_URL = os.getenv('ENDPOINT_URL')
USER_NAME = os.getenv('USER_NAME')
HOST = os.getenv('HOST')

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
	raise Exception( f'Unable to start transaction: { p.returncode }' )

#Downloading from the bucket only new or moidified files 
cmd = 's3cmd --access_key=%s --secret_key=%s --access_token=%s --host=%s --host-bucket=%s sync s3://%s/cvmfs/ /cvmfs/%s.infn.it/' % (ACCESS_KEY,SECRET_KEY,TOKEN,HOST,HOST,USER_NAME,USER_NAME)
p=subprocess.run(cmd, shell=True)
if p.returncode != 0:
	raise Exception( f'Error in sync: { p.returncode }' )

#Publishing changes in the repo, in case of data corruption abort transaction 
cmd= 'cvmfs_server publish %s.infn.it' %USER_NAME
p=subprocess.run(cmd, shell=True)
if p.returncode != 0:
    print('Unable to publish, aborting transaction..')
    cmd='cvmfs_server abort -f %s.infn.it' %USER_NAME
    p=subprocess.run(cmd, shell=True)
    if p.returncode != 0:
	    raise Exception( f'Unable to abort: { p.returncode }' )

else:
    print('Published')