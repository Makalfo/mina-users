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
        self.dec_multiplier = 1000000000
        self.sleep_time = 30

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

        # add genesis if users table is empty
        self.genesis = self.get_url_json( self.config['URLS']['genesis'] )
        if self.get_num_names() == 0:
            self.parse_genesis()

        # loop
        while True:

            # update the users
            print( self.get_providers() )

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

    def get_providers( self ):
        '''get providers'''
        output = { 'providers': dict(),
                   'delegation_program': dict(),
                   'exchanges': dict() }
        
        # staketab providers
        data = self.get_url_json( self.config['URLS']['staketab'] )
        for provider in data['staking_providers']:
            output[ 'providers' ][ provider['provider_address'] ] = provider['provider_title'] 
            # check if it is an exchange
            if " Wallet" in provider['provider_title'] and provider['provider_address'] != '':
                output[ 'exchanges' ][ provider['provider_address'] ] = provider['provider_title'] 

        # Mina Foundation
        mf_data = self.get_csv_url( self.config['URLS']['mina_foundation'] )
        for idx, address in enumerate(mf_data):
            output[ 'delegation_program' ][ address ] = f'Mina Foundation {idx}'

        # O1 Labs
        mf_data = self.get_csv_url( self.config['URLS']['o1_labs'] )
        for idx, address in enumerate(mf_data):
            output[ 'delegation_program' ][ address ] = f'O1 Labs {idx}'

        return output 

    def add_creator( self, data ):
        '''add the creator for the receiver'''
        cmd = """INSERT INTO users (
            public_key,
            creator,
            first_active,
            genesis,
            foundation,
            exchange,
            scammer
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING"""
        if self.mode in [ 'nominal', 'test', 'random' ]:
            self.analysis_cursor.execute( cmd, data )
        else:
            self.logger.debug( f'Creator Not Inserted with Mode: {self.mode}' )


database = MinaAnalysis( )