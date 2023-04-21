#!/usr/bin/python3

import time
import boto3
import subprocess
import configparser
import logging 
import os 


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

    print('Starting transaction for %s.infn.it repository...' %bucket)
    cmd = 'cvmfs_server transaction %s.infn.it'  %bucket   
    p=subprocess.run(cmd, shell=True)
    if p.returncode != 0:
	    return False 
    return True 
        

def sync_repo(bucket):

    '''This functions syncronizes the repo in stratum-0 with the s3 bucket'''

    cmd = "s3cmd -c /home/ubuntu/s3_cvmfs.cfg sync s3://%s/cvmfs/ /cvmfs/%s.infn.it/" % (bucket,bucket) #--delete-removed options 
    p=subprocess.run(cmd, shell=True)
    if p.returncode != 0:
	    logging.warning('Synchronization not succeded\n', p.returncode)


def publish(bucket):

    '''This function publishes (closes a transaction) in the repositery, in case of some errors (e.g. data corruption)
        it aborts the transaction'''

    cmd= 'cvmfs_server publish %s.infn.it' %bucket
    p=subprocess.run(cmd, shell=True)
    if p.returncode != 0:
        logging.warning('Unable to publish, aborting transaction...\n' , p.returncode)
        cmd='cvmfs_server abort -f %s.infn.it' %bucket
        p=subprocess.run(cmd, shell=True)
        if p.returncode != 0:
    	    logging.error('Unable to abort, the repo remains in transaction\n' , p.returncode)
        

def distribute_software(bucket):

    '''This function looks for a tar file in the repository and for the correspondent cfg file.
        If the users use the value 'yes' for the variable 'publish', the software will be distributed 
        and published in the specififed 'base_dir' variable'''

    for entry in os.scandir('/cvmfs/%s.infn.it' %bucket) :
        if not entry.name.endswith('.tar') : 
            continue 

        if '%s_%s.cfg' % (bucket,entry.name.split('.')[0]) not in os.listdir('/cvmfs/%s.infn.it' %bucket):
            logging.warning('The configuration file for the tarball %s is missing, please write %s_%s.cfg to manage the tarball' %(entry.name,bucket,entry.name.split('.')[0])) 
            continue    
    
        if '%s' %entry.name.split('.')[0] not in os.listdir('/cvmfs/%s.infn.it' %bucket):
            config.read('/cvmfs/%s.infn.it/%s_%s.cfg' % (bucket,bucket,entry.name.split('.')[0]))
            try:
                publish = config.get('default','publish')
                base_dir = config.get('default','base_dir')
                if publish == 'yes':
                    cmd='cvmfs_server ingest --tar_file /cvmfs/%s.infn.it/%s --base_dir %s/ %s.infn.it' %(bucket,entry.name,base_dir,bucket)
                    p=subprocess.run(cmd, shell=True)
                    if p.returncode != 0:
                        logging.error('Unable to publish the server %s' %entry.name )
            except Exception as ex:
                logging.warning('Some configuration info for %s_%s.cfg file are missing\n' %(bucket,entry.name.split('.')[0]), ex )

        #else:
            #Prova con, se è stato spacchettato controlla le date (se tar più recente dello spacchettato allora ridistribuisci) 
            #data in bit (se è più grande è più nuovo --> controlla la data della directory  ) 
            #se non funziona la data usa md5sum e salvalo in un file 
            #Devo salvare md5sum del tar appena viene scaricato e poi confrontarlo ogni volta per valutare se ridstribuirlo o no


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
        handler = logging.FileHandler('/home/ubuntu/logs_cvmfs/%s.log' %bkt, mode='a', encoding='utf-8', delay=True) 
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(handler)
        tr = transaction(bkt)
        if tr==False:
            logging.error('Unable to start transaction...') 
            continue 
        
        sync_repo(bkt)
        publish(bkt)
        #Software distribution 
        distribute_software(bkt)


    end_time = time.time()
    print("Execution time:", end_time - start_time, "seconds")