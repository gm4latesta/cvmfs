# cvmfs

This repo contains the code for implementing an automated pipeline for synchronizing an s3 bucket with cvmfs repository. 

### cvmfs basic commands
The basic cvmfs commands are:
```
cvmfs_server transaction <name_of_the_repo> 
cd /cvmfs/<name_of_the_repo>
#make some changes and type 
cvmfs_server publish <name_of_the_repo>
#cvmfs command for software distribution 
cvmfs_server ingest --tar_file /some/path/file.tar --base_dir some/path <name_of_the_repo>
```

### sync_gm.py
In this script is implemeted the automatization for synchronizing my /cvmfs repo (/cvmfs/gmalatesta.infn.it/) with my bucket 
In the automatization everything is executed by sudo with the steps written in the code. 
With a cronjob this is repeated every hour so that the repo is kept updated. 

### sync_repo.py
In this script is implemented the automatization for synchronizing all the buckets having a cvmfs/ area with te correspondent /cvmfs repo. 
All files excluded those in cvmfs/software/ repo are synchonized. 
Those files (.tar) in cvmfs/software/ are downloaded in /tmp/software/ dir in Startum 0 and distributed using 'cvmfs_server ingest' according to the variables in software.cfg

### Configuration file 
config_write.py contains some variables needed for the synchronization, it writes a .cfg file from which these variables are extracted and used

software.cfg contains different sections (each section has the software name to be distributed) with 'publish' and 'base_dir' variables needed for software publication when using 'cvmfs_server ingest'
