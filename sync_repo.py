#!/usr/bin/python3

import time
import boto3
import subprocess
import configparser
import logging 
import os 


def get_names(ACCESS_KEY,SECRET_KEY,ENDPOINT_URL) :

    '''This function returns the names of the buckets with cvmfs/ area and the credentials needed to download their content  '''
    
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


def mkdir(bucket):

    '''This function creates the folder /cvmfs/<username>.infn.it/tarballs_to_be_extracted if it does not exist'''

    if 'tarballs_to_be_extracted' not in os.listdir('/cvmfs/%s.infn.it' %bucket):
        cmd = 'mkdir /cvmfs/%s.infn.it/tarballs_to_be_extracted' %bucket
        p=subprocess.run(cmd, shell=True)
    return


def sync_repo(bucket):

    '''This function syncronizes the cvmfs/ area in the s3 bucket with the repo in stratum-0,
        and the cvmfs/tarballs_to_be_extracted area in s3 bucket with /tmp/sofwtare directory in stratum 0. '''

    #Synchronization of the cvmfs/ area of the bucket with the /cvmfs repo
    cmd = "s3cmd -c /home/ubuntu/s3_cvmfs.cfg sync --exclude 'tarballs_to_be_extracted/*' --delete-removed s3://%s/cvmfs/ /cvmfs/%s.infn.it/" % (bucket,bucket) 
    p=subprocess.run(cmd, shell=True)
    if p.returncode != 0:
	    logging.warning('Bucket and cvmfs repo synchronization not succeded\n', p.returncode)

    #Synchronization of the cvmfs/tarballs_to_be_extracted/ area of the bucket with /tmp/software directory of stratum 0 
    cmd = "s3cmd -c /home/ubuntu/s3_cvmfs.cfg sync --exclude '*' --include '*.tar' s3://%s/cvmfs/tarballs_to_be_extracted/ /tmp/software/" % bucket
    p=subprocess.run(cmd, shell=True)
    if p.returncode != 0:
	    logging.warning('Bucket and /tmp/software dir in stratum 0 synchronization not succeded\n', p.returncode)


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
        return False 
    return True 
        

def distribute_software(bucket):

    '''This function looks for the software.cfg file in the repository, scans all its sections 
        and for each software it look at the 'publish' variable. If the value is 'yes', it takes the software in .tar format
        from /tmp/software directory and distributes it using cvmfs method 'cvmfs_server ingest' in the
        directory specified in the 'base_dir' variable of the .cfg file'''

    if '%s_software.cfg' %bucket in os.listdir('/home/ubuntu/software'):
        config.read('/home/ubuntu/software/%s_software.cfg' %bucket)

        for section in config.sections():

            try: 
                base_dir = config.get(section,'base_dir')
                new = config.get(section, 'new')

                #The software has never be distributed 
                if section not in os.listdir('/cvmfs/%s.infn.it/tarballs_to_be_extracted' %bucket):
                    cmd = 'cvmfs_server ingest --tar_file /home/ubuntu/software/%s.tar --base_dir tarballs_to_be_extracted/%s/ %s.infn.it' %(section, base_dir, bucket)
                    p=subprocess.run(cmd, shell=True)
                    if p.returncode != 0:
                        logging.error('Unable to publish the server %s' %section )

                #The software has already been distributed but there is a new version, the previous one is renamed as _old
                elif section in os.listdir('/cvmfs/%s.infn.it/tarballs_to_be_extracted' %bucket) and new==True:
                    tr=transaction(bucket)
                    if tr==False:
                        continue 
                    cmd = 'mv /cvmfs/%s.infn.it/tarballs_to_be_extracted/%s /cvmfs/%s.infn.it/tarballs_to_be_extracted/%s_old' %(bucket,section,bucket,section)
                    p=subprocess.run(cmd, shell=True)
                    if p.returncode != 0:
                        logging.error(p.returncode)
                    pb=publish(bucket)
                    if pb == False:
                        continue 
                    cmd = 'cvmfs_server ingest --tar_file /home/ubuntu/software/%s.tar --base_dir tarballs_to_be_extracted/%s/ %s.infn.it' %(section, base_dir, bucket)
                    p=subprocess.run(cmd, shell=True)
                    if p.returncode != 0:
                        logging.error('Unable to publish the server %s' %section )
                    
            except Exception as ex:
                logging.warning('Missing configuration info for %s in %s_software.cfg\n' %(section,bucket) , ex )
            



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
        mkdir(bkt)
        sync_repo(bkt)
        publish(bkt)
        #Software distribution 
        distribute_software(bkt)


    end_time = time.time()
    print("Execution time:", end_time - start_time, "seconds")