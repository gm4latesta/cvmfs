#!/usr/bin/python3

import time
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
        

def sync_repo(bucket):

    '''This functions syncronizes the repo in stratum-0 with the s3 bucket'''

    cmd = 's3cmd -c /home/ubuntu/s3_cvmfs.cfg sync --delete-removed --exclude "/cvmfs/%s.infn.it/*/" s3://%s/cvmfs/ /cvmfs/%s.infn.it/' % (bucket,bucket,bucket) 
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

    start_time = time.time()

    config = configparser.ConfigParser()
    config.read('/home/ubuntu/s3_cvmfs.cfg')

    root_logger= logging.getLogger()
    root_logger.setLevel(logging.WARNING)

    ENDPOINT_URL = config.get('database','ENDPOINT_URL')
    ACCESS_KEY = config.get('default','access_key')
    SECRET_KEY = config.get('default','secret_key')

    #Get buckets names 
    bkt_names=get_names(ACCESS_KEY,SECRET_KEY,ENDPOINT_URL)

    #Sync alle the repo in cvmfs stratum-0 with the s3 buckets and publish the changes 
    for bkt in bkt_names:
        handler = logging.FileHandler('/home/ubuntu/logs_cvmfs/%s.log' %bkt, mode='w', encoding='utf-8', delay=True) 
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(handler)
        tr = transaction(bkt)
        if tr==False:
            logging.error('Unable to start transaction...') 
            continue
        else:
            sync_repo(bkt)
            publish(bkt)

            #Check if in the cvmfs repo there is a tarball file 
            for entry in os.scandir('/cvmfs/%s.infn.it' %bkt) :

                if entry.name.endswith('.tar') :

                    #Check if the there is the correspondend configuration file and in case write the error log 
                    if '%s_%s.cfg' % (bkt,entry.name.split('.')[0]) in os.listdir('/cvmfs/%s.infn.it' %bkt):
                        #Check if there are the correct variables
                        config.read('/cvmfs/%s.infn.it/%s_%s.cfg' % (bkt,bkt,entry.name.split('.')[0]))
                        try:
                            publish = config.get('default','publish')
                            base_dir = config.get('default','base_dir')
                            #Check if the software need to be distributed 
                            if publish == 'yes':
                                #Check if it is not already been distributed 
                                if '%s/' %entry.name.split('.')[0] not in  os.listdir('/cvmfs/%s.infn.it' %bkt) :
                                    #Distribute the software using the native function of cvmfs (cvmfs_server ingest)
                                    cmd='cvmfs_server ingest --tar_file /cvmfs/%s.infn.it/%s --base_dir %s/ %s.infn.it' %(bkt,entry.name,base_dir,bkt)
                                    p=subprocess.run(cmd, shell=True)
                                    if p.returncode != 0:
                                        logging.error('Unable to publish the server %s' %entry.name )

                        except Exception as ex:
                            logging.warning(ex)
                            logging.warning('Some configuration info for %s_%s.cfg file are missing' %(bkt,entry.name.split('.')[0]) )
                    
                    else:
                        logging.warning('The configuration file for the tarball %s is missing, please write %s_%s.cfg to manage the tarball' %(entry.name,bkt,entry.name.split('.')[0]))
            
         
    end_time = time.time()
    print("Execution time:", end_time - start_time, "seconds")