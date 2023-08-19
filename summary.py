import time
import textract
from transformers import pipeline, AutoModelForTokenClassification, AutoTokenizer

from metadatum.commands import Commands
from metadatum.vocabulary import Vocabulary as voc

import redis
import config as cnf
from metadatum.utils import Utils as utl

utl.importConfig()
import config as cnf


from transformers import BartForConditionalGeneration, BartTokenizer

def run(t_list:list = None, props:dict = None) -> dict|None:

        t1 = time.perf_counter()

        model = BartForConditionalGeneration.from_pretrained(props.get('model'))
        print('model', props.get('model'))
        tokenizer = BartTokenizer.from_pretrained(props.get('tokenizer'))
        print('tokenizer', props.get('tokenizer'))

        cmd = Commands()
        pool = redis.ConnectionPool(host=cnf.settings.redis.host, port = cnf.settings.redis.port, db = 0)
        rs = redis.Redis(connection_pool = pool)
        
        query = props.get(voc.QUERY)
        # print('query', query)
        resources = cmd.selectBatch(rs, voc.EDGE, query, props.get(voc.LIMIT))

        file_set = set()
        # print(resources.docs, '\n')
        for doc in resources.docs:
                doc_hash = cmd.getRedisHash(rs, doc.id)
                # print(doc_hash)
                file_set.add(doc_hash.get(voc.ID_1))
                file_set.add(doc_hash.get(voc.ID_2))

        sum_list = {}
        i = 0
        for file in file_set:
                i = i + 1
                file_hash = cmd.getRedisHash(rs, utl.denormId(file))
                file_path = file_hash.get(voc.URL)
                # print('\n', file_path)
                text = textract.process(file_path, encoding='utf-8')
                # Read first 1000 characters from the string
                text = text[:3000]
                # print(f'\nText: {text}\n')
                summarizer = pipeline("summarization", 
                                model=model, 
                                tokenizer=tokenizer, 
                                framework="pt",
                                # do_sample=True,
                                top_k=0, 
                                # top_k=50, 
                                top_p=0.95, 
                                early_stopping=True
                                )                
                output_text = summarizer(str(text.decode()))
                print_text = output_text[0]['summary_text']
                # print(f'\nSummary: {print_text}\n')
                key ='summary_' + str(i)
                sum_list.update({key:print_text})

        print(f'=== Execution time: {time.perf_counter() - t1}')

        return sum_list

if __name__ == '__main__':
        run()
