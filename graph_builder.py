from metadatum.utils import Utils as utl
from metadatum.commands import Commands
from metadatum.vocabulary import Vocabulary as voc
import redis
from redis.commands.graph.edge import Edge
from redis.commands.graph.node import Node
import logging
logging.basicConfig(filename='file_meta.log', encoding='utf-8', level=logging.DEBUG)

utl.importConfig()
import config as cnf

'''
    This processor is taking record from transaction index with the references to
    resources that it should process. In our case resource is the edge index
    in Redisearch. We are using RedisGraph to build graph of the resources.
'''
def run(t_list:list = None, props:dict = None) -> dict|None:
    pool = redis.ConnectionPool(host=cnf.settings.redis.host, port = cnf.settings.redis.port, db = 0)
    rs = redis.Redis(connection_pool = pool)
    cmd = Commands()
    reg, idx, schema_sha_id = cmd.buildIndex(rs, cnf.settings.indices.dir_core, 'schemas/edge.yaml', 'graph_builder')
   
    if t_list == None or len(t_list) == 0:
        return {'result':'ERROR', 'message':'No files to process'}   
        
    '''
        Create graph nodes and edges
    '''
    graph_name = props.get('graph_name')
    graph = rs.graph(graph_name)
    graph_list = utl.decodeList(graph.list_keys()) 

    if graph_name in graph_list and str(props.get('rebuild')).lower() == 'yes':
            graph.delete()

    args = []
    # Create nodes and add nodes to the graph
    for t_doc in t_list:        
        args.append(utl.normId(t_doc.id))
        '''
            Here we are working with transaction index. We need to get the resource id's
        '''
        t_props:dict = cmd.getRedisHash(rs, t_doc.id)
        n_id = t_props.get(voc.ITEM_ID)
        n_label = t_props.get(voc.ITEM_PREFIX)
        n_props:dict = cmd.getRedisHash(rs, utl.fullId(n_label, n_id))
        node = Node(label=n_label, properties=n_props)        
        try:          
            graph.merge(node)
        except:
            graph.add_node(node)

        cmd.txStatus(rs, 'graph_builder', '', str(t_doc.id), voc.COMPLETE)
    '''
        Create edge index and add edges to the graph
    '''
    keys = [schema_sha_id, '', voc.WAITING, props.get('l_threshold'), props.get('r_threshold'), props.get('m_threshold')]
    cmd.updateEdgeIndex(rs, keys, args, True)
    edges = cmd.selectBatch(rs, 'edge', str(props.get('e_query')), 1000)
    for e_doc in edges.docs:
        '''
            Here we are working with edge index
        '''
        id = e_doc.id
        e_props:dict = cmd.getRedisHash(rs, id)
        if float(e_props.get('l_match')) < props.get('l_threshold'): 
            continue
       
        label = e_props.get(voc.LABEL)
        print(label)
        left = utl.getIdShaPart(e_props.get(voc.ID_1))        
        label_left = utl.getIdShaPart(e_props.get(voc.LABEL_1))        
        right = utl.getIdShaPart(e_props.get(voc.ID_2))        
        label_right = utl.getIdShaPart(e_props.get(voc.LABEL_2))        
        dict_left = {voc.ID: left}
        dict_right = {voc.ID: right}
        edge = cmd.mergeEdgeQuery(label, label_left, dict_left, label_right, dict_right, e_props)
        graph.query(str(edge))
        '''
            Update transaction index. Attention! Here we are working with transaction index,
            so, we need to use transaction id
        '''
        t_sha1= utl.getIdShaPart(id)
        cmd.txCreate(rs, 'graph_builder', schema_sha_id, t_sha1, voc.EDGE, '', voc.COMPLETE)

    graph.commit()

    return {'result':'OK'}    