#!/usr/bin/env python

# -*- coding: utf-8 -*-
# elastichandler.py
#
# Matthew Seyer, mseyer@g-cpartners.com
# Copyright 2015 G-C Partners, LLC
#
# G-C Partners licenses this file to you under the Apache License, Version
# 2.0 (the "License"); you may not use this file except in compliance with the
# License.  You may obtain a copy of the License at:
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import logging
import os
import json
import csv
MAX_SIZE = int(sys.maxint)
csv.field_size_limit(MAX_SIZE)
import md5
import datetime
import copy
import re
import pickle
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk as es_bulk
from elasticsearch import helpers
from argparse import ArgumentParser

__VERSION__ = '1.0'
VERSION = __VERSION__

logging.getLogger("elasticsearch").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)

log_fmt = '%(module)s:%(funcName)s:%(lineno)d %(message)s'
logging.basicConfig(
    level = logging.DEBUG,
    format=log_fmt
)

def GetOptions():
    '''Get needed options for processesing'''
    #Options:
    #evidence_name,index,report,config
    
    usage = ''''''
    options = ArgumentParser(
        description=(usage)
    )
    
    options.add_argument(
        '--host',
        dest='host',
        action="store",
        type=str,
        default=None,
        required=True,
        help='Elastic Host IP'
    )
    
    options.add_argument(
        '--evidence_name',
        dest='evidence_name',
        action="store",
        type=str,
        default=None,
        help='evidence name'
    )
    
    options.add_argument(
        '--index',
        dest='index',
        action="store",
        type=str,
        default=None,
        help='index name'
    )
    
    options.add_argument(
        '--report',
        dest='report',
        action="store",
        type=str,
        default=None,
        help='report to index'
    )
    
    options.add_argument(
        '--config',
        dest='config',
        action="store",
        type=str,
        default=None,
        help='json config file'
    )

    return options

def Main():
    ###GET OPTIONS###
    arguements = GetOptions()
    options = arguements.parse_args()
    
    fileIndexer = FileIndexer(
        options
    )
    
    fileIndexer.IndexFile()
    
    sys.exit(0)
    
def GetJsonContent(filename):
    try:
        with open(filename,'rb') as json_fh:
            content = json.load(json_fh)
            
        return content
    except Exception as e:
        msg = 'Failed to load JSON file: {} \n[{}]'.format(filename,e.message)
        raise Exception(msg)

class FileIndexer():
    BULK_GROUP_SIZE = 100000
    def __init__(self,options):
        self.options = options
        
        self.config = GetJsonContent(options.config)
        self.index_mapping = GetJsonContent(self.config['map_file'])
        
        self.esConfig = EsConfig(
            host=self.options.host
        )
        
        if 'index_suffix' in self.config:
            self.options.index += self.config['index_suffix']
    
    @staticmethod
    def GetIndexName(index_name):
        index_name = index_name.lower()
        
        return index_name
    
    def IndexFile(self):
        
        esHandler = EsHandler(
            self.esConfig
        )
        
        result = esHandler.CheckForIndex(
            self.options.index
        )
        
        if result == False:
            esHandler.InitializeIndex(
                index=self.options.index
            )
            
        #Check if mapping exists#
        result = esHandler.CheckForMapping(
            self.config['type'],
            index=self.options.index
        )
        
        #Create mapping if not exists#
        if result == False:
            esHandler.InitializeMapping(
                self.config['type'],
                self.index_mapping,
                index=self.options.index
            )
        
        if 'report_format' in self.config:
            if self.config['report_format'] is not None:
                if self.config['report_format'] == 'json':
                    self._IndexJsonReport(
                        esHandler
                    )
                elif self.config['report_format'] == 'txt' or self.config['report_format'] == 'csv':
                    self._IndexCsvReport(
                        esHandler
                    )
            else:
                raise Exception('report_format should not be None. Use \'csv\' or \'json\'')
        else:
            self._IndexCsvReport(
                esHandler
            )
        
        pass
    
    def _IndexJsonReport(self,esHandler):
        with open(self.options.report, 'rb') as jsonfile:
            json_report = json.load(jsonfile)
            
        if 'record_key' in self.config:
            if self.config['record_key'] is not None:
                records = json_report[self.config['record_key']]
            else:
                raise Exception('record_key cannot be None')
            
        records_to_insert = []
        for row in records:
            record = row
            
            record.update({
                'Source':self.options.report,
                'Source Base Name':os.path.basename(self.options.report),
                'Evidence Name':self.options.evidence_name
            })
            
            #Add Timestamp#
            timestamp = datetime.datetime.now()
            
            record.update({
                'index_timestamp': timestamp.strftime("%m/%d/%Y %H:%M:%S.%f")
            })
            
            #Create hash of our record to be the id#
            m = md5.new()
            
            record_string = pickle.dumps(record) #causing speed issues
            m.update(record_string)
            hash_id = m.hexdigest()
            
            action = {
                "_index": self.options.index,
                "_type": self.config['type'],
                "_id": hash_id,
                "_source": record
            }
            
            records_to_insert.append(action)
            
            if len(records_to_insert) >= FileIndexer.BULK_GROUP_SIZE:
                esHandler.BulkIndexRecords(
                    records_to_insert
                )
                
                records_to_insert = []
        
        if len(records_to_insert) > 0:
            esHandler.BulkIndexRecords(
                records_to_insert
            )
            
    def _GetQuoteType(self,value):
        if value == "QUOTE_NONE":
            return csv.QUOTE_NONE
        elif value == "QUOTE_ALL":
            return csv.QUOTE_ALL
        else:
            return csv.QUOTE_NONE
    
    def _IndexCsvReport(self,esHandler):
        with open(self.options.report, 'rb') as csvfile:
            
            if 'quoting' in self.config:
                quoting = self._GetQuoteType(self.config['quoting'])
            else:
                # Default quoting
                quoting = csv.QUOTE_NONE
            
            rReader = csv.DictReader(
                csvfile,
                fieldnames=self.config['columns'],
                delimiter=str(self.config['delimiter']),
                quoting=quoting
            )
            
            rReader.line_num = self.config['start_line']
            
            records_to_insert = []
            for row in rReader:
                if rReader.line_num < int(self.config['start_line']):
                    continue
                
                record = {}
                
                for k, v in row.iteritems():
                    if v is None:
                        continue
                    v = v.strip()
                    if (v != "" and v != " " and v != "na" and v != r'N/A'):
                        if isinstance(v,str):
                            v = v.decode("utf-8", "replace")
                        record[k] = v
                    else:
                        record[k] = None
                
                ###Add Extra Columns###
                extra_columns = self.AddColumns(
                    row
                )
                record.update(
                    extra_columns
                )
                
                ###Parse Sub Records###
                subfields = self.ParseSubFields(
                    record
                )
                record.update(
                    subfields
                )
                
                record.update({
                    'Source':self.options.report,
                    'Source Base Name':os.path.basename(self.options.report),
                    'Evidence Name':self.options.evidence_name
                })
                
                #Add Timestamp#
                timestamp = datetime.datetime.now()
                
                record.update({
                    'index_timestamp': timestamp.strftime("%m/%d/%Y %H:%M:%S.%f")
                })
                
                #Create hash of our record to be the id#
                m = md5.new()
                #record_string = json.dumps(record)
                record_string = pickle.dumps(record)
                m.update(record_string)
                hash_id = m.hexdigest()
                
                action = {
                    "_index": self.options.index,
                    "_type": self.config['type'],
                    "_id": hash_id,
                    "_source": record
                }
                
                records_to_insert.append(action)
                
                if len(records_to_insert) >= FileIndexer.BULK_GROUP_SIZE:
                    esHandler.BulkIndexRecords(
                        records_to_insert
                    )
                    
                    records_to_insert = []
            
            if len(records_to_insert) > 0:
                esHandler.BulkIndexRecords(
                    records_to_insert
                )
        
        pass
    
    def ParseSubFields(self,record):
        subfields = {}
        if self.config.has_key('sub_record_columns'):
            for sub_column in self.config['sub_record_columns']:
                if record.has_key(sub_column):
                    if record[sub_column] is not None:
                        extra = {}
                        
                        extra = self._GetSubFields(
                            record[sub_column]
                        )
                        
                        subfields.update(extra)
                    
        return subfields
    
    def _GetSubFields(self,sub_record):
        info = {}
        
        value_array = sub_record.split(';')
        for item in value_array:
            try:
                item = item.strip()
                
                key,value = item.split(': ',1)
                key = key.strip('[]')
                
                info[key] = value
            except:
                continue
        
        return info
    
    def AddColumns(self,row):
        extra = {}
        num_of_columns = self.config['add_columns']
        if len(num_of_columns) > 0:
            add_columns = self.config['add_columns']
            for column_name in add_columns.keys():
                column_options = add_columns[column_name]
                
                ###Column Functions Here###
                if column_options['type'] == 'append':
                    extra.update(
                        self._FuncAppendColumns(column_options,row,column_name)
                    )
                elif column_options['type'] == 'append_list':
                    extra.update(
                        self._FuncAppendList(column_options,row,column_name)
                    )
                elif column_options['type'] == 'get_from_path':
                    extra.update(
                        self._FuncGetEntriesFromPath(column_options,row,column_name)
                    )
                elif column_options['type'] == 'get_filename':
                    extra.update(
                        self._FuncGetFileName(column_options,row,column_name)
                    )
                elif column_options['type'] == 'get_ext':
                    extra.update(
                        self._FuncGetFileExt(column_options,row,column_name)
                    )
                elif column_options['type'] == 'from_regex':
                    extra.update(
                        self._FuncFromRegex(column_options,row,column_name)
                    )
                    
                pass
            
        return extra
    
    def _FuncFromRegex(self,column_options,row,column_name):
        extra = {}
        sources = column_options['source']
        
        for source in sources:
            pass
        
        if 'options' in column_options:
            if 'regex' in column_options['options'] and 'value' in column_options['options']:
                regexp = column_options['options']['regex']
                value = column_options['options']['value']
            else:
                logging.warn('Function "from_regex" requires options "regex" and "value"')
                
        for source in sources:
            field = source.format(**row)
            reg_match = re.search(regexp,field)
            
            if reg_match is not None:
                str_matches = reg_match.groups()
                extra[column_name] = value.format(*str_matches)
                pass
        
        return extra
    
    def _FuncGetFileExt(self,column_options,row,column_name):
        extra = {}
        sources = column_options['source']
        
        sep = '\\'
        
        if 'options' in column_options:
            if 'sep' in column_options['options']:
                sep = column_options['options']['sep']
                
        extra[column_name] = ''
        
        for source in sources:
            fullname = source.format(**row).strip(sep)
            
            extra['FileExt'] = os.path.splitext(fullname)[1].lstrip('.')
            break
            
        return extra
    
    def _FuncGetFileName(self,column_options,row,column_name):
        extra = {}
        sources = column_options['source']
        
        sep = '\\'
        
        if 'options' in column_options:
            if 'sep' in column_options['options']:
                sep = column_options['options']['sep']
                
        extra[column_name] = ''
        
        for source in sources:
            try:
                fullname = source.format(**row)
                fullname = fullname.strip(sep)
            except UnicodeDecodeError as error:
                logging.warn('File {}; Error: {}'.format(self.options.report,error))
                fullname = '<UNICODE_ERROR_DURRING_INDEXING>'
            
            entries = fullname.split(sep)
            
            if len(entries) < 2:
                continue
            
            extra[column_name] = os.path.basename(fullname)
            extra['FileExt'] = os.path.splitext(fullname)[1].lstrip('.')
            break
            
        return extra
    
    def _FuncGetEntriesFromPath(self,column_options,row,column_name):
        extra = {}
        sources = column_options['source']
        
        sep = '\\'
        
        if 'options' in column_options:
            if 'sep' in column_options['options']:
                sep = column_options['options']['sep']
        
        extra[column_name] = []
        
        for source in sources:
            try:
                fullname = source.format(**row)
                fullname = fullname.strip(sep)
            except UnicodeDecodeError as error:
                logging.warn('File {}; Error: {}'.format(self.options.report,error))
                fullname = '<UNICODE_ERROR_DURING_INDEXING>'
            
            entries = fullname.split(sep)
            
            if len(entries) < 2:
                continue
            
            extra[column_name] += entries
        
        return extra
    
    def _FuncAppendColumns(self,column_options,row,column_name):
        extra = {}
        
        sources = column_options['source']
        
        fline = ''
        for line in sources:
            fline += line
            
        extra[column_name] = fline.format(**row)
        
        return extra
    
    def _FuncAppendList(self,column_options,row,column_name):
        extra = {}
        
        sources = column_options['source']
        
        fline = []
        for line in sources:
            fline.append(line.format(**row))
            
        extra[column_name] = fline
        
        return extra

class EsConfig():
    def __init__(self,host=None):
        self.host = host
        
    def GetEsHandler(self):
        esHandler = EsHandler(
            self
        )
        
        return esHandler
    
class EsHandler():
    
    def __init__(self,esConfig):
        self.current_index = None
        self.esh = Elasticsearch(
            esConfig.host
        )
        
    def CheckForIndex(self,index_name):
        return self.esh.indices.exists(index_name)
    
    def IndexRecord(self,index,doc_type,record):
        '''
        Index a single record
        IN
            self: EsHandler
            index: the index name
            doc_type: the document type to index as
            record: The dictionary record to be indexed
        '''
        #Create hash of our record to be the id#
        m = md5.new()
        m.update(json.dumps(record))
        hash_id = m.hexdigest()
        
        #Index the record#
        res = self.esh.index(
            ignore=[400], #This will ignore fields if the field doesnt match the mapping type (important for fields where timestamp is blank)
            index=index,
            doc_type=doc_type,
            id=hash_id,
            body=record
        )
        
    def BulkIndexRecords(self,records):
        '''
        Bulk Index Records
        IN
            self: EsHandler
            records: a list of records to bulk index
        '''
        logging.debug('[starting] Indexing Bulk Records')
        success_count,failed_items = es_bulk(
            self.esh,
            records,
            chunk_size=10000,
            raise_on_error=False
        )
        
        if len(failed_items) > 0:
            logging.error('{} index errors'.format(len(failed_items)))
            index_error_file = open("IndexErrors.txt", "a+")
            index_error_file.write(str(failed_items)+"\n")
            index_error_file.close()
        
        logging.debug('[finished] Indexing Bulk Records')
        
    def CheckForMapping(self,doc_type,index=None):
        '''
        Check if a mapping exists for a given document type
        IN
            self: EsHandler
            index: the name of the index
            doc_type: the document type
        OUT
            True - Mapping exists for doc_type in index
            False - Mapping does not exists for doc_type in index
        '''
        index = self._SetIndex(index)
        
        mapping = self.esh.indices.get_mapping(
            index = index,
            doc_type = doc_type
        )
        
        count = len(mapping.keys())
        
        if count > 0:
            return True
        
        return False
    
    def InitializeMapping(self,doc_type,mapping,index=None):
        '''
        Create mapping for a document type
        IN
            self: EsHandler
            index: the name of the index
            doc_type: the document type
            mapping: The dictionary mapping (not a json string)
        '''
        index = self._SetIndex(index)
        
        self.esh.indices.put_mapping(
            doc_type=doc_type,
            index=index,
            body=mapping['mappings']
        )
    
    def InitializeIndex(self,index=None):
        '''
        Create an index
        IN
            self: EsHandler
            index: the name of the index to create
        '''
        index = self._SetIndex(index)
        
        request_body = {
            "settings" : {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                'analysis': {
                    'analyzer': {
                        'file_path': {
                            'type': 'custom',
                            'tokenizer': 'path_hierarchy',
                            'filter': ['lowercase']
                        }
                    }
                }
            },
        }
        
        res = self.esh.indices.create(
            index = index,
            body = request_body
        )
    
    def GetRecordsFromFile_Result(self,query_file,index=None):
        ''' **NEEDS WORK - NOT COMPLETE**
        Return results based off of a query from a json file
        IN
            self: EsHandler
            index: the index name
            query_file: the file that contains a query
        OUT
            None: This returns none because this function is not complete
        '''
        index = self._SetIndex(index)
        
        with open(query_file,'rb') as qfh:
            query = json.load(qfh)
            
        qfh.close
        
        result = self.esh.search(
            index=index,
            scroll='60s',
            size=1000,
            body=query
        )
        
        total_hits = result['hits']['total']
        
        scroll_size = total_hits
        
        while (scroll_size > 0):
            scroll_id = result['_scroll_id']
            
            result = self.esh.scroll(
                scroll_id=scroll_id,
                scroll='60s'
            )
            records = result['hits']['hits']
            
            for hit in records:
                yield hit
            scroll_size -= len(records)
    
    def FetchRecordsFromQuery(self,query,index=None):
        '''
        Yield hits based off of a query from a json str
        IN
            self: EsHandler
            query: the query (can be dictionary or json str)
            index: the index name
        OUT
            hit: Yields hits for the query
        '''
        #If query is a string, load from json#
        if isinstance(query,str) or isinstance(query,unicode):
            query = json.loads(query)
            
        index = self._SetIndex(index)
        
        result = self.esh.search(
            index=index,
            scroll='60s',
            size=1000,
            body=query
        )
        
        total_hits = result['hits']['total']
        
        scroll_size = total_hits
        
        while (scroll_size > 0):
            scroll_id = result['_scroll_id']
            
            for hit in result['hits']['hits']:
                yield hit
                
            scroll_size -= len(result['hits']['hits'])
            
            result = self.esh.scroll(
                scroll_id=scroll_id,
                scroll='60s'
            )
    
    def GetRecordsFromQueryStr_Dict(self,json_str,mapping,index=None):
        '''
        Return dictionary of results based off of mapping list. The last item in the
        mapping list should be unique, otherwise reocrds can overwrite records.
        
        This function attempts to emulate perl dbi's fetchall_hashref([key,key,...]).
        
        IN
            self: EsHandler
            json_str: query
            mapping: list of mapping keys
            index: The index to search. default=None (if None, will use self.current_index)
        OUT
            record_dict: dictionary of hits based off of mapping
        '''
        query = json.loads(json_str)
        record_dict = {}
        
        if index == None:
            if self.current_index == None:
                msg = 'No index given, and no current index specified. Pass in index=INDEX or use EsHandler.SetCurrentIndex(INDEX) first'
                raise Exception(msg)
            else:
                index = self.current_index
        
        result = self.esh.search(
            index=index,
            scroll='60s',
            size=1000,
            body=query
        )
        
        scroll_size = result['hits']['total']
        
        while (scroll_size > 0):
            scroll_id = result['_scroll_id']
            
            for hit in result['hits']['hits']:
                #eumerated mapping#
                emapping = []
                #for each key in mapping, enumerate the value#
                for key in mapping:
                    emapping.append(
                        hit['_source'][key]
                    )
                
                #Set current level#
                current_level = record_dict
                
                #set markers#
                c = 1
                lp = len(emapping)
                
                #create dictionary keys based off of enumerated mapping#
                for key in emapping:
                    if key not in current_level:
                        if lp == c:
                            current_level[key] = hit
                        else:
                            current_level[key] = {}
                    current_level = current_level[key]
                    c += 1
            #update scroll size#
            scroll_size -= len(result['hits']['hits'])
            
            #update result#
            result = self.esh.scroll(
                scroll_id=scroll_id,
                scroll='60s'
            )
            
        return record_dict
    
    def SetCurrentIndex(self,index_name):
        '''
        Set the current index to index_name.
        IN
            self: EsHandler
            index_name: name of the index
        '''
        self.current_index = index_name
        
    def _SetIndex(self,index):
        if index == None:
            if self.current_index == None:
                msg = 'No index given, and no current index specified. Pass in index=INDEX or use EsHandler.SetCurrentIndex(INDEX) first'
                raise Exception(msg)
            else:
                index = self.current_index
        
        return index
    
if __name__ == '__main__':
    Main()
