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


def fill_md5(md5_dict,bucket):

    os.system('for tar in /home/ubuntu/software/%s/*.tar ; do md5sum "$tar" ; done >> /home/ubuntu/software/%s/md5.txt' %(bucket,bucket))

    with open('/home/ubuntu/software/%s/md5.txt' %bucket , 'r') as file:
        for line in file:             
            if '.tar' not in line:
                continue 

            if len(md5_dict)==0: #if the dict is empty, add as key the name of the tar file and as value the correspondent md5sum
                md5_dict[line.split()[1].split('/')[-1]]=[line.split()[0]]

            elif len(md5_dict)!=0: #if it is not, add a second value (another md5sum) to the correspondent key (tar file name)
                md5_dict[line.split()[1].split('/')[-1]].append(line.split()[0])
        
    os.remove('/home/ubuntu/software/%s/md5.txt' %bucket)
    return md5_dict 
            

def sync_sw(bucket):

    '''This function syncronizes the cvmfs/software/ folder in the s3 bucket of the user with /home/ubuntu/software
    folder in stratum 0'''

    cmd = "s3cmd -c /home/ubuntu/s3_cvmfs.cfg sync --exclude '*' --include '*.tar' --include '*.cfg' --delete-removed s3://%s/cvmfs/software/ /home/ubuntu/software/%s/" % (bucket,bucket)
    p=subprocess.run(cmd, shell=True)
    if p.returncode != 0:
	    logging.warning('Bucket and /home/ubuntu/software dir in stratum 0 synchronization not succeded\n', p.returncode)


def transaction(bucket):

    '''This function starts a transaction in the repositery in stratum-0'''

    print('Starting transaction for %s.infn.it repository...' %bucket)
    cmd = 'cvmfs_server transaction %s.infn.it'  %bucket   
    p=subprocess.run(cmd, shell=True)
    if p.returncode != 0:
	    return False 
    return True 


def sync_repo(bucket):

    '''This function syncronizes the cvmfs/ folder in the s3 bucket with the repo in stratum-0,
        and the cvmfs/software area in s3 bucket with /home/ubuntu/sofwtare directory in stratum 0. '''

    #Synchronization of the cvmfs/ area of the bucket with the /cvmfs repo
    cmd = "s3cmd -c /home/ubuntu/s3_cvmfs.cfg sync --exclude 'software/*' --delete-removed s3://%s/cvmfs/ /cvmfs/%s.infn.it/" % (bucket,bucket) 
    p=subprocess.run(cmd, shell=True)
    if p.returncode != 0:
	    logging.warning('Bucket and cvmfs repo synchronization not succeded\n', p.returncode)    


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
        

def distribute_software(bucket,md5_dict):

    '''This function looks for the software.cfg file in the repository, scans all its sections 
        and for each software it looks for the variable needed for the distribution'''

    if '%s_software.cfg' %bucket in os.listdir('/home/ubuntu/software/%s' %bucket):
        config = configparser.ConfigParser()
        config.read('/home/ubuntu/software/%s/%s_software.cfg' %(bucket,bucket) )

        for section in config.sections():

            try: 
                base_dir = config.get(section,'base_dir')

                #The software has never be distributed 
                if section not in os.listdir('/cvmfs/%s.infn.it/software' %bucket):
                    cmd = 'cvmfs_server ingest --tar_file /home/ubuntu/software/%s/%s.tar --base_dir software/%s/ %s.infn.it' %(section, base_dir, bucket)
                    p=subprocess.run(cmd, shell=True)
                    if p.returncode != 0:
                        logging.error('Unable to publish the server %s' %section )

                #The software has already been distributed, check at the md5sum to distribute again if there is a new version
                else:
                    for tar in md5_dict:
                        if len(md5_dict[tar])==1 : #there are no md5sum to compare
                            continue 
                        elif md5_dict[tar][0]==md5_dict[tar][1] : #the 2 md5sum are the same, no need to distribute again
                            continue 
                        elif md5_dict[tar][0]!=md5_dict[tar][1] : #the 2 md5sum are different, distribute the software again adding the suffix "_old" to the old version 
                            tr=transaction(bucket)
                            if tr==False:
                                continue 
                            cmd = 'mv /cvmfs/%s.infn.it/software/%s /cvmfs/%s.infn.it/software/%s_old' %(bucket,section,bucket,section)
                            p=subprocess.run(cmd, shell=True)
                            if p.returncode != 0:
                                logging.error(p.returncode)
                            pb=publish(bucket)
                            if pb == False:
                                continue
                            cmd = 'cvmfs_server ingest --tar_file /home/ubuntu/software/%s/%s.tar --base_dir software/%s/ %s.infn.it' %(bucket,section, base_dir, bucket)
                            p=subprocess.run(cmd, shell=True)
                            if p.returncode != 0:
                                logging.error('Unable to publish the server %s' %section )
            
                    
            except Exception as ex:
                return "error"
    else:
        return "no cfg" 



if __name__ == '__main__' :

    start_time = time.time()

    config = configparser.ConfigParser()
    config.read('/home/ubuntu/s3_cvmfs.cfg')

    ENDPOINT_URL = config.get('database','ENDPOINT_URL')
    ACCESS_KEY = config.get('default','access_key')
    SECRET_KEY = config.get('default','secret_key')

    #Get buckets names 
    bkt_names=get_names(ACCESS_KEY,SECRET_KEY,ENDPOINT_URL)

    #Create sofware directory in /home/ubuntu for storing .tar  and .cfg files (software to be distributed)
    if 'software' not in os.listdir('/home/ubuntu'):
        os.mkdir('/home/ubuntu/software')
    #Create logs_cvmfs directory for storing logs 
    if 'logs_cvmfs' not in os.listdir('/home/ubuntu'):
        os.mkdir('/home/ubuntu/logs_cvmfs')
        
    #Sync alle the repo in cvmfs stratum-0 with the s3 buckets and publish the changes 
    for bkt in bkt_names:
        handler = logging.FileHandler('/home/ubuntu/logs_cvmfs/%s.log' %bkt, mode='a', encoding='utf-8', delay=True) 
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger= logging.getLogger()
        root_logger.addHandler(handler)

        #Create user-specific folder in /home/ubuntu/software
        if '%s' %bkt not in os.listdir('/home/ubuntu/software'):
            os.mkdir('/home/ubuntu/software/%s' %bkt)

        #Create a dictonary for comparing md5sum of all the tar files (software) uploaded by the user 
        md5_empty={}
        md5=fill_md5(md5_empty,bkt)

        #Synchronization of the software/ folder in the user bucket with /home/ubuntu/software/<username>/ folder in stratum 0
        sync_sw(bkt) #sync /home/ubuntu/software/<username> with s3://<username>/cvmfs/software/
        
        md5_final=fill_md5(md5,bkt)   

        #Start transaction for the /cvmfs user repo 
        tr = transaction(bkt)
        if tr==False:
            logging.error('Unable to start transaction...') 
            continue 
        
        #Create software/ folder in the /cvmfs user repo 
        if 'software' not in os.listdir('/cvmfs/%s.infn.it' %bkt):
            os.system('mkdir /cvmfs/%s.infn.it/software' %bkt)

        #Synchronization of the user bucket with the correspondent /cvmfs repo
        sync_repo(bkt)
        #Publish
        publish(bkt)

        #Software distribution 
        sw=distribute_software(bkt,md5_final)

        if sw=="no cfg":
            tr=transaction(bkt)
            if tr==False:
                continue 
            with open('/cvmfs/%s.infn.it/software/info_log.txt' %bkt , 'w') as file:
                file.write('Missing configuration file for software distribution (<username>_software.cfg), write it in the correct format if software distribution is needed\n')
            publish(bkt)
        
        elif sw=="error":
            tr=transaction(bkt)
            if tr==False:
                continue 
            with open('/cvmfs/%s.infn.it/software/info_log.txt' %bkt , 'w') as file:
                file.write('Missing base_dir variable in the configuration file (<username>_software.cfg), see documentation\n')
            publish(bkt)


    end_time = time.time()
    print("Execution time:", end_time - start_time, "seconds")