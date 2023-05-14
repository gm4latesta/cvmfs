import configparser

config = configparser.ConfigParser()

config['database'] = {
    'oidc_profile_name' : 'infncloud',
    'endpoint_url' : 'https://minio.cloud.infn.it/',
    'user_name' : 'gmalatesta',
    'host' : 'minio.cloud.infn.it'
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

