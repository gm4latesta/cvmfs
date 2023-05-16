#!/usr/bin/python3

"""This code implements the distribution of the bucket content of the user with the cvmfs repo"""

import time
import subprocess
import configparser
import logging
import os
import argparse
import boto3


def get_names(access_k,secret_k,end_url) :

    '''This function returns the names of the buckets with cvmfs/ area
        and the credentials needed to download their content'''

    s_3 = boto3.client('s3', endpoint_url=end_url,
                        config=boto3.session.Config(signature_version='s3v4'),
                        verify=True,
                        aws_access_key_id=access_k,
                        aws_secret_access_key=secret_k)

    names=[]
    for bucket in s_3.list_buckets()['Buckets']:
        if 'Contents' in s_3.list_objects(Bucket=bucket['Name']).keys():
            for obj in s_3.list_objects(Bucket=bucket['Name'])['Contents']:
                if 'cvmfs/' in obj['Key']:
                    names.append(bucket['Name'])
                    break
        else: continue

    return names


def fill_md_5(md_5_dict,bucket,o_s):

    '''This function creates a dictory for storing md5sum'''

    proc=subprocess.run(f'for tar in /home/{o_s}/software/{bucket}/*.tar ; do md5sum "$tar" ; done',
                     shell=True,check=False,capture_output=True) 
    #change capture_output=True with stdout=subprocess.PIPE
    list_md_5=proc.stdout.decode().split('\n')

    if len(md_5_dict)==0:
        for tar_el in list_md_5:
            if tar_el!='':
                md_5_dict[tar_el.split('/')[-1]]=[tar_el.split()[0]]
    else:
        for tar_el in list_md_5:
            if tar_el!='':
                md_5_dict[tar_el.split('/')[-1]].append(tar_el.split()[0])
    return md_5_dict


def sync_sw(bucket,o_s,cfg):

    '''This function syncronizes the cvmfs/software/ folder in the s3 bucket of the user with
    /home/<os>/software/<username>/ folder in stratum 0'''

    cmd = f"s3cmd -c /home/{o_s}/{cfg} sync --exclude '*' --include '*.tar' --include '*.cfg' \
        --delete-removed s3://{bucket}/cvmfs/software/ /home/{o_s}/software/{bucket}/"
    proc=subprocess.run(cmd,shell=True,check=False)
    if proc.returncode != 0:
        logging.warning('Not able to sync software')


def transaction(bucket):

    '''This function starts a transaction in the repositery in stratum-0'''

    print(f'Starting transaction for {bucket}.infn.it repository...')
    cmd = f'cvmfs_server transaction {bucket}.infn.it'
    proc=subprocess.run(cmd,shell=True,check=False)
    if proc.returncode != 0:
        return False
    return True


def sync_repo(bucket,o_s,cfg):

    '''This function syncronizes the cvmfs/ folder in the s3 bucket with the repo in stratum-0'''

    #Synchronization of the cvmfs/ area of the bucket with the /cvmfs repo
    cmd = f"s3cmd -c /home/{o_s}/{cfg} sync --exclude 'software/*' --delete-removed \
        s3://{bucket}/cvmfs/ /cvmfs/{bucket}.infn.it/"
    proc=subprocess.run(cmd,shell=True,check=False)
    if proc.returncode != 0:
        logging.warning('Not able to sync repo')


def publish(bucket):

    '''This function publishes (closes a transaction) in the repositery, in case of some errors
    (e.g. data corruption) it aborts the transaction'''

    cmd= f'cvmfs_server publish {bucket}.infn.it'
    proc=subprocess.run(cmd,shell=True,check=False)
    if proc.returncode != 0:
        logging.warning('Unable to publish, aborting transaction...')
        cmd=f'cvmfs_server abort -f {bucket}.infn.it'
        proc=subprocess.run(cmd,shell=True,check=False)
        if proc.returncode != 0:
            logging.error('Unable to abort, the repo remains in transaction')

        return False
    return True


def distribute_software(bucket,md5_dict,o_s):

    '''This function looks for the software.cfg file in the repository, scans all its sections 
        and for each software it looks for the md5sum and variable needed for the distribution'''

    if f'{bucket}_software.cfg' in os.listdir(f'/home/{o_s}/software/{bucket}'):
        conf = configparser.ConfigParser()
        conf.read(f'/home/{o_s}/software/{bucket}/{bucket}_software.cfg')

        for section in conf.sections():

            try:
                base_dir = conf.get(section,'base_dir')

                #The software has never be distributed
                if base_dir not in os.listdir(f'/cvmfs/{bucket}.infn.it/software'):
                    cmd=f'cvmfs_server ingest --tar_file \
                        /home/{o_s}/software/{bucket}/{section}.tar \
                            --base_dir software/{base_dir}/ {bucket}.infn.it'
                    proc=subprocess.run(cmd,shell=True,check=False)
                    if proc.returncode != 0:
                        logging.error(f'Unable to publish server {section}')
                    time.sleep(5)
                #The software has already been distributed, check the md5sum to distribute again
                elif base_dir in os.listdir(f'/cvmfs/{bucket}.infn.it/software'):
                    for tar in md5_dict:
                        if len(md5_dict[tar])==1: #there are no md5sum to compare
                            continue
                        if md5_dict[tar][0]==md5_dict[tar][1]:
                            continue
                        tran=transaction(bucket)
                        if tran is False:
                            continue
                        time.sleep(5)
                        cmd = f'mv /cvmfs/{bucket}.infn.it/software/{base_dir} \
                        /cvmfs/{bucket}.infn.it/software/{base_dir}_old'
                        proc=subprocess.run(cmd,shell=True,check=False)
                        if proc.returncode != 0:
                            logging.error('Unable to rename base_dir')
                        time.sleep(5)
                        publ=publish(bucket)
                        if publ is False:
                            continue
                        time.sleep(5)
                        cmd = f'cvmfs_server ingest --tar_file \
                            /home/{o_s}/software/{bucket}/{section}.tar \
                                --base_dir software/{base_dir}/ {bucket}.infn.it'
                        proc=subprocess.run(cmd,shell=True,check=False)
                        if proc.returncode != 0:
                            logging.error(f'Unable to distribute software {section}')
                        time.sleep(5)

            except KeyError:
                return "error"
            except SyntaxError:
                return "error"
    else:
        return "no cfg"



if __name__ == '__main__' :

    start_time = time.time()

    parser = argparse.ArgumentParser()
    parser.add_argument('-os','--operating_sys',dest='o_s',help='operating system')
    parser.add_argument('-cfg','--conf_file',dest='cfg',help='configuration file')
    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(f'/home/{args.o_s}/{args.cfg}')

    endpoint_url = config.get('database','endpoint_url')
    access_key = config.get('default','access_key')
    secret_key = config.get('default','secret_key')

    #Get buckets names
    bkt_names=get_names(access_key,secret_key,endpoint_url)

    #Create sofware directory in /home/<os> for storing .tar  and .cfg files of the users
    if 'software' not in os.listdir(f'/home/{args.o_s}'):
        os.mkdir(f'/home/{args.o_s}/software')
    #Create logs_cvmfs directory for storing logs
    if 'logs_cvmfs' not in os.listdir(f'/home/{args.o_s}'):
        os.mkdir(f'/home/{args.o_s}/logs_cvmfs')

    #Distribution of configurations, files, static libraries and software
    for bkt in bkt_names:
        root_logger= logging.getLogger()
        handler = logging.FileHandler(f'/home/{args.o_s}/logs_cvmfs/{bkt}.log',
                                      mode='a',encoding='utf-8',delay=True)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(handler)

        #Create user folder in /home/<os>/software for storing tar and cfg files
        if bkt not in os.listdir(f'/home/{args.o_s}/software'):
            os.mkdir(f'/home/{args.o_s}/software/{bkt}')

        #Create a dictonary for comparing md5sum of all the tar files uploaded by the user
        md_5_empty={}
        md_5=fill_md_5(md_5_empty,bkt,args.o_s)

        #Synchronization of the software/ folder in the user bucket with folder in stratum 0
        sync_sw(bkt,args.o_s,args.cfg)

        md_5_final=fill_md_5(md_5,bkt,args.o_s)

        #Start transaction for the /cvmfs user repo
        TRANSAC = transaction(bkt)
        if TRANSAC is False:
            logging.error('Unable to start transaction...')
            continue
        time.sleep(5)
        #Create software/ folder in the /cvmfs user repo
        if 'software' not in os.listdir(f'/cvmfs/{bkt}.infn.it'):
            os.system(f'mkdir /cvmfs/{bkt}.infn.it/software')

        #Synchronization of the user bucket with the correspondent /cvmfs repo
        sync_repo(bkt,args.o_s,args.cfg)
        time.sleep(5)
        #Publish
        publish(bkt)
        time.sleep(5)
        #Software distribution
        SW=distribute_software(bkt,md_5_final,args.o_s)

        if SW=="no cfg" and '.tar' in os.listdir(f'/home/{args.o_s}/software/{bkt}'):
            TRANSAC=transaction(bkt)
            if TRANSAC is False:
                continue
            time.sleep(5)
            with open(f'/cvmfs/{bkt}.infn.it/info_log.txt','w',encoding='utf-8') as file:
                file.write(f'Missing <username>_software.cfg for software distribution. \
                           Write it in the correct format if software distribution is needed')
            time.sleep(5)
            publish(bkt)
            time.sleep(5)
        elif SW=="error":
            TRANSAC=transaction(bkt)
            if TRANSAC is False:
                continue
            time.sleep(5)
            with open(f'/cvmfs/{bkt}.infn.it/info_log.txt','w',encoding='utf-8') as file:
                file.write('Missing base_dir key in <username>_software.cfg, see documentation')
            time.sleep(5)
            publish(bkt)
            time.sleep(5)


    end_time = time.time()
    print("Execution time:", end_time - start_time, "seconds")
