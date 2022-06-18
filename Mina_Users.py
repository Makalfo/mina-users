# import libraries
import os
import sys
import pandas as pd
import pandas.io.sql as sqlio
import json
import configparser
import psycopg2
import numpy as np
import warnings
import logging
import requests
import base58
import time
import urllib
import random
from psycopg2.extensions import register_adapter, AsIs
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
warnings.filterwarnings("ignore")

logging.getLogger(__name__).addHandler(logging.StreamHandler(sys.stdout))

class MinaAnalysis:

    def __init__( self, mode='nominal'):
        self.sleep_time = 3600

        # read config file
        self.config = self.read_config( )

        # log levels and mode
        self.mode = os.getenv('MODE')
        if self.mode == None:
            self.mode = 'nominal'

        if self.mode == 'nominal' :
            log_level = logging.INFO
        else:
            log_level = logging.DEBUG

        logging.basicConfig( format = '%(asctime)s.%(msecs)03d %(levelname)s: %(message)s',
            level = log_level )

        # save the logger
        self.logger = logging
        self.logger.info( 'Starting Up Mina Users' )
        self.logger.info( f'Operation Mode: {self.mode}' )

        # connect to the analysis database
        self.analysis = self.connect_db( {
            'database':  os.getenv('ANALYSIS_DATABASE'),
            'host': os.getenv('ANALYSIS_HOST'),
            'port': os.getenv('ANALYSIS_PORT'),
            'user': os.getenv('ANALYSIS_USER'),
            'password': os.getenv('ANALYSIS_PASSWORD'),
        } )
        self.analysis_cursor = self.analysis.cursor()

        # loop
        while True:

            # update the users
            providers = self.get_providers()
            for key in providers.keys():
                if key != "" and key.startswith("B62") and len( key ) == 55 and len(providers[key])> 3:
                    self.update_names( key, providers[key].replace("'","") )

            # Sleep after each cycle
            self.logger.info( f"Sleeping for { self.sleep_time } Seconds" )
            time.sleep( int( self.sleep_time ) )

            # break if in debug mode
            if self.mode == "debug":
                break

    def read_config( self ):
        '''read the config file'''
        config = configparser.ConfigParser(interpolation=None)
        config.read( 'config.ini' )
        return config

    def connect_db( self, info ):
        '''establish the postgres'''
        self.logger.info( f"Connecting to {info[ 'database' ]} at {info[ 'host' ]}:{info[ 'port' ]}")
        # connect
        conn = psycopg2.connect(
            database =  info[ 'database' ],
            user =      info[ 'user' ],
            password =  info[ 'password' ],
            host =      info[ 'host' ],
            port =      info[ 'port' ] )
        # set isolation level
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT);
        return conn

    def get_csv_url( self, url ):
        '''return the csv as a list'''
        req = urllib.request.Request( url )
        req.add_header('User-Agent', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:77.0) Gecko/20100101 Firefox/77.0')
        content = urllib.request.urlopen(req)
        data = pd.read_csv(content, header=None)
        return list( data[0] ) 

    def get_url_json( self, url_address ):
        '''return the json'''
        with urllib.request.urlopen( url_address ) as url:
            return json.loads(url.read().decode())

    def get_providers( self ):
        '''get providers'''
        output = dict()
        
        # staketab providers
        data = self.get_url_json( self.config['URLS']['staketab'] )
        for provider in data['staking_providers']:
            output[ provider['provider_address'] ] = provider['provider_title'] 

        # Mina Foundation
        mf_data = self.get_csv_url( self.config['URLS']['mina_foundation'] )
        for idx, address in enumerate(mf_data):
            output[ address ] = f'Mina Foundation {idx}'

        # O1 Labs
        mf_data = self.get_csv_url( self.config['URLS']['o1_labs'] )
        for idx, address in enumerate(mf_data):
            output[ address ] = f'O1 Labs {idx}'

        return output 

    def update_names( self, public_key, name ):
        '''add / update name address'''
        self.logger.info( f"Inserting / Updating {public_key} - {name}")
        cmd = f"""INSERT INTO names (
            public_key,
            name
            ) VALUES ('{public_key}', '{name}')
            ON CONFLICT (public_key) DO UPDATE SET name = '{name}'"""
        self.analysis_cursor.execute( cmd )


database = MinaAnalysis( )