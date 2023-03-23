from boto3sts import credentials as creds
import boto3
import subprocess



def get_names() :

    '''This functions returns the names of the buckets with cvmfs/ area and the credentials needed to download their content  '''
    
    #Getting refreshble credentials session
    credentials = creds.s3_session_credentials("infncloud", endpoint="https://minio.cloud.infn.it/", verify=True)
    aws_session = creds.assumed_session("infncloud")
    s3 = aws_session.client('s3', endpoint_url="https://minio.cloud.infn.it/", 
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
	    raise Exception( f'Unable to start transaction: { p.returncode }' )
        #return 



def sync_repo(credentials,bucket):

    '''This functions syncronizes the repo in stratum-0 with the s3 bucket'''

    ACCESS_KEY = credentials["access_key"]
    SECRET_KEY = credentials["secret_key"]
    TOKEN = credentials["token"]

    cmd = 's3cmd --access_key=%s --secret_key=%s --access_token=%s --host=minio.cloud.infn.it --host-bucket=minio.cloud.infn.it sync s3://%s/cvmfs/ /cvmfs/%s.infn.it/' % (bucket,ACCESS_KEY,SECRET_KEY,TOKEN,bucket,bucket)
    p=subprocess.run(cmd, shell=True)
    if p.returncode != 0:
	    raise Exception( f'Error in sync: { p.returncode }' )
        #return 


def publish(bucket):

    cmd= 'cvmfs_server publish %s.infn.it' %bucket
    p=subprocess.run(cmd, shell=True)
    if p.returncode != 0:
        print('Unable to publish, aborting transaction..')
        cmd='cvmfs_server abort -f gmalatesta.infn.it'
        p=subprocess.run(cmd, shell=True)
        if p.returncode != 0:
    	    raise Exception( f'Unable to abort: { p.returncode }' )
        



if __name__ == '__main__' :

    #Get buckets names 
    bkt_names,credentials=get_names()

    #Sync alle the repo in cvmfs stratum-0 with the s3 buckets and publish the changes 
    for bkt in bkt_names:
        transaction(bkt)
        sync_repo(credentials,bkt)
        publish(bkt)