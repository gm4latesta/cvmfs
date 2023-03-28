import boto3
import subprocess
import configparser
import logging 


def get_names(ACCESS_KEY,SECRET_KEY,ENDPOINT_URL) :

    '''This functions returns the names of the buckets with cvmfs/ area and the credentials needed to download their content  '''
    
    s3 = boto3.client('s3', endpoint_url=ENDPOINT_URL, 
                        config=boto3.session.Config(signature_version='s3v4'),
                        verify=True , 
                        aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY)

    names=[]
    for bkt in s3.list_buckets()['Buckets']: 
        if 'Contents' in s3.list_objects(Bucket=bkt['Name']).keys():
            for obj in s3.list_objects(Bucket=bkt['Name'])['Contents']:
                if 'cvmfs/' in obj['Key']:
                    names.append(bkt['Name'])   
                    break
        else: continue  

    return names  


def transaction(bucket):

    '''This function starts a transaction in the repositery in stratum-0'''

    print('Starting transaction...')
    cmd = 'cvmfs_server transaction %s.infn.it'  %bucket   
    p=subprocess.run(cmd, shell=True)
    if p.returncode != 0:
	    return False 
    return True 
        

def sync_repo(ACCESS_KEY,SECRET_KEY,bucket):

    '''This functions syncronizes the repo in stratum-0 with the s3 bucket'''

    cmd = 's3cmd -c s3_cvmfs.cfg sync s3://%s/cvmfs/ /cvmfs/%s.infn.it/' % (bucket,bucket)
    p=subprocess.run(cmd, shell=True)
    if p.returncode != 0:
	    logging.warning('Synchronization not succeded')


def publish(bucket):

    cmd= 'cvmfs_server publish %s.infn.it' %bucket
    p=subprocess.run(cmd, shell=True)
    if p.returncode != 0:
        logging.warning('Unable to publish, aborting transaction...')
        cmd='cvmfs_server abort -f %s.infn.it' %bucket
        p=subprocess.run(cmd, shell=True)
        if p.returncode != 0:
    	    logging.error('Unable to abort, the repo remains in transaction')
        



if __name__ == '__main__' :

    config = configparser.ConfigParser()
    config.read('s3_cvmfs.cfg')

    root_logger= logging.getLogger()
    root_logger.setLevel(logging.WARNING)

    ENDPOINT_URL = config.get('database','ENDPOINT_URL')
    ACCESS_KEY = config.get('default','access_key')
    SECRET_KEY = config.get('default','secret_key')

    #Get buckets names 
    bkt_names=get_names(ACCESS_KEY,SECRET_KEY,ENDPOINT_URL)

    #Sync alle the repo in cvmfs stratum-0 with the s3 buckets and publish the changes 
    for bkt in bkt_names:
        handler = logging.FileHandler('/home/ubuntu/logs_cvmfs/%s.log' %USER_NAME, mode='w', encoding='utf-8', delay=True) 
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(handler)
        tr = transaction(bkt)
        if tr==False:
            logging.error('Unable to start transaction...') 
            continue
        else:
            sync_repo(ACCESS_KEY,SECRET_KEY,bkt)
            publish(bkt)

        