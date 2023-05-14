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

### sync_s3_cvmfs.py
In this script is implemented the automated synchronization of all the buckets having a cvmfs/ area with the correspondent /cvmfs repo. 
All files, excluded those in cvmfs/software/ folder of the bucket, are synchronized. 
Those files (.tar) in cvmfs/software/ are downloaded in another directory of the Startum 0 and are distributed using 'cvmfs_server ingest' according to the variables in <username>_software.cfg

### Configuration file 
config_write.py contains some variables needed for the synchronization, it writes a .cfg file from which these variables are extracted and used,
<username>_software.cfg contains different sections (each section has the software name to be distributed) with the 'base_dir' variable needed for software publication when using 'cvmfs_server ingest'.
