from boto3sts import credentials as creds
import boto3
import subprocess
import configparser
import logging 


def get_names(OIDC_PROFILE_NAME,ENDPOINT_URL) :

    '''This functions returns the names of the buckets with cvmfs/ area and the credentials needed to download their content  '''
    
    #Getting refreshble credentials session
    credentials = creds.s3_session_credentials(OIDC_PROFILE_NAME, endpoint=ENDPOINT_URL, verify=True)
    aws_session = creds.assumed_session(OIDC_PROFILE_NAME)
    s3 = aws_session.client('s3', endpoint_url=ENDPOINT_URL, 
                                config=boto3.session.Config(signature_version='s3v4'),
                                verify=True)

    names=[]
    for bkt in s3.list_buckets()['Buckets']: 
        for obj in s3.list_objects(Bucket=bkt['Name'])['Contents']:
            if 'cvmfs/' in obj['Key']:
                names.append(bkt['Name'])   #questo non funziona se il nome è dentro scratch e va estratto da obj['Key']
            break 

    return names , credentials 


def transaction(bucket):

    '''This function starts a transaction in the repositery in stratum-0'''

    print('Starting transaction..')
    cmd = 'cvmfs_server transaction %s.infn.it'  %bucket   
    p=subprocess.run(cmd, shell=True)
    if p.returncode != 0:
	    return False 
    return True 
        

def sync_repo(credentials,bucket,HOST):

    '''This functions syncronizes the repo in stratum-0 with the s3 bucket'''

    ACCESS_KEY = credentials["access_key"]
    SECRET_KEY = credentials["secret_key"]
    TOKEN = credentials["token"]

    cmd = 's3cmd --access_key=%s --secret_key=%s --access_token=%s --host=%s --host-bucket=%s sync s3://%s/cvmfs/ /cvmfs/%s.infn.it/' % (bucket,ACCESS_KEY,SECRET_KEY,TOKEN,HOST,HOST,bucket,bucket)
    p=subprocess.run(cmd, shell=True)
    if p.returncode != 0:
	    logging.warning('Synchronization not succeded')


def publish(bucket):

    cmd= 'cvmfs_server publish %s.infn.it' %bucket
    p=subprocess.run(cmd, shell=True)
    if p.returncode != 0:
        logging.warning('Unable to publish, aborting transaction..')
        cmd='cvmfs_server abort -f %s.infn.it' %bucket
        p=subprocess.run(cmd, shell=True)
        if p.returncode != 0:
    	    logging.error('Unable to abort, the repo remains in transaction')
        



if __name__ == '__main__' :

    config = configparser.ConfigParser()
    config.read('cvmfs.cfg')

    OIDC_PROFILE_NAME = config.get('database','OIDC_PROFILE_NAME')
    ENDPOINT_URL = config.get('database','ENDPOINT_URL')
    USER_NAME = config.get('database','USER_NAME')
    HOST = config.get('database','HOST')


    #Get buckets names 
    bkt_names,credentials=get_names(OIDC_PROFILE_NAME,ENDPOINT_URL)

    #Sync alle the repo in cvmfs stratum-0 with the s3 buckets and publish the changes 
    for bkt in bkt_names:
        logging.basicConfig(filename="/home/ubuntu/logs_cvmfs/%s.log", filemode="w", format="%(asctime)s - %(levelname)s : %(message)s") %bkt
        tr=transaction(bkt)
        if tr==False:
            logging.error('Unable to start transaction') 
        else:
            sync_repo(credentials,bkt,HOST)
            publish(bkt)

        