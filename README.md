# cvmfs

This repo contains the code for implementing an automated pipeline for synchronizing an s3 bucket with cvmfs repository. 

The basic cvmfs commands are:
```
cvmfs_server transaction <name_of_the_repo> 
cd /cvmfs/<name_of_the_repo>
#make some changes and type 
cvmfs_server publish <name_of_the_repo>
```

In the automatization everything is executed by sudo with the steps written in the code. 
With a cronjob this is repeated every hour so that the repo is kept updated. 
