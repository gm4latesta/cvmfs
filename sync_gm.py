from boto3sts import credentials as creds
import boto3
import subprocess

#Getting your refreshble credentials session with the oidc-agent profile named infncloud
credentials = creds.s3_session_credentials("infncloud", endpoint="https://minio.cloud.infn.it/", verify=True)
ACCESS_KEY = credentials["access_key"]
SECRET_KEY = credentials["secret_key"]
TOKEN = credentials["token"]

#Starting the transaction in the repo
print('Starting transaction..')
cmd = 'cvmfs_server transaction gmalatesta.infn.it'
p=subprocess.run(cmd, shell=True)
if p.returncode != 0:
	raise Exception( f'Unable to start transaction: { p.returncode }' )

#Downloading from the bucket only new or moidified files 
cmd = 's3cmd --access_key=%s --secret_key=%s --access_token=%s --host=minio.cloud.infn.it --host-bucket=minio.cloud.infn.it sync s3://gmalatesta/cvmfs/ /cvmfs/gmalatesta.infn.it/' % (ACCESS_KEY,SECRET_KEY,TOKEN)
p=subprocess.run(cmd, shell=True)
if p.returncode != 0:
	raise Exception( f'Error in sync: { p.returncode }' )

#Publishing changes in the repo, in case of data corruption abort transaction 
cmd= 'cvmfs_server publish gmalatesta.infn.it'
p=subprocess.run(cmd, shell=True)
if p.returncode != 0:
    print('Unable to publish, aborting transaction..')
    cmd='cvmfs_server abort -f gmalatesta.infn.it'
    p=subprocess.run(cmd, shell=True)
    if p.returncode != 0:
	    raise Exception( f'Unable to abort: { p.returncode }' )

else:
    print('Published')