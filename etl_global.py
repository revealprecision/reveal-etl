"""Global configs"""

import configparser

config = configparser.ConfigParser()
config.read('./etl_processor.conf')

element = config['process']['element']
function = config['process']['function']
data_pull_interval = config['process']['data_pull_interval']
drop_views_and_recreate = config['process']['drop_views_and_recreate']
