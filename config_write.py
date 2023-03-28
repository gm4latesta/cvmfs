import configparser

config = configparser.ConfigParser()

config['database'] = {
    'OIDC_PROFILE_NAME' : 'infncloud',
    'ENDPOINT_URL' : 'https://minio.cloud.infn.it/',
    'USER_NAME' : 'gmalatesta',
    'HOST' : 'minio.cloud.infn.it'
}

config['default'] = {
    'host_base' : 'minio.cloud.infn.it' ,
    'host_bucket' : 'minio.cloud.infn.it' ,
    'use_https' : 'true' , 
    'access_key' : '' , 
    'secret_key' : ''

}

with open('s3_cvmfs.cfg', 'w') as config_file:
    config.write(config_file)

