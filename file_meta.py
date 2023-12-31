import os
import logging
logging.basicConfig(filename='file_meta.log', encoding='utf-8', level=logging.DEBUG)

import time
import redis
from redis.commands.graph import Graph, Node, Edge
from pathlib import Path

from metadatum.commands import Commands
from metadatum.utils import Utils as utl
from metadatum.vocabulary import Vocabulary as voc
from metadatum.bootstrap import Bootstrap

utl.importConfig()
import config as cnf


def run(props: dict = None):                     
       
        pool = redis.ConnectionPool(host=cnf.settings.redis.host, port = cnf.settings.redis.port, db = 0)
        r = redis.Redis(connection_pool = pool)
        cmd = Commands()

        reg, idx, schema_sha_id = cmd.buildIndex(r, cnf.settings.indices.dir_user, 'schemas/file.yaml', 'file_meta')

        key_list = idx.get(voc.KEYS)

        # list all files with ext in directory
        file_list = utl.listAllFiles(props.get('dir'), props.get('file_type'))

        # Process each file
        for file in file_list:
            print(file)
            _dir, _file = os.path.split(os.path.abspath(file))
            # extract file extention from file name
            ext = Path(file).suffix
            map: dict = {
                'schema_id': schema_sha_id,
                'parent_id': _dir,
                'url': file,
                'file_type': ext,
                'size': os.path.getsize(file),
                'doc': ' ',
                'commit_id': 'und',
                'commit_status': 'und'
            } 
            f_prefix = idx.get(voc.PREFIX)
            f_sha_id = cmd._updateRecordHash(r, f_prefix, key_list, map) 
            cmd.txCreate(r, 'file_meta', schema_sha_id, f_sha_id, f_prefix, file, voc.WAITING)           

        return props