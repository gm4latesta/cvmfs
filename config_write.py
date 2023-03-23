import configparser

config = configparser.ConfigParser()

config['database'] = {
    'OIDC_PROFILE_NAME': 'infncloud',
    'ENDPOINT_URL': 'https://minio.cloud.infn.it/',
    'USER_NAME': 'gmalatesta',
    'HOST': 'minio.cloud.infn.it'
}

with open('cvmfs.cfg', 'w') as config_file:
    config.write(config_file)

