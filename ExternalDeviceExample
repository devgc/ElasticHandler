import elasticsearch
import json
import sys
import codecs
import xlsxwriter

codecs.register(lambda name: codecs.lookup('utf-8') if name == 'cp65001' else None)
#reload(sys)
#sys.setdefaultencoding('utf8')
workbook = xlsxwriter.Workbook('output.xlsx')
w=workbook.add_worksheet('USB Devices')
es = elasticsearch.Elasticsearch()  # use default of localhost, port 9200

title_header_format  = workbook.add_format({'font_color': 'white', 'bg_color': 'gray', 'bold': 'true'})
center_header_format = workbook.add_format({'font_color': 'black', 'align': 'center',  'bg_color': 'gray', 'bold': 'true'})
header_format        = workbook.add_format({'font_color': 'black', 'bg_color': 'gray', 'bold': 'true'})
black_type_format    = workbook.add_format({'font_color': 'black', 'align': 'left'})
black_date_format    = workbook.add_format({'font_color': 'black', 'num_format': 'yyyy-mm-dd hh:mm:ss.000'})
black_url_format     = workbook.add_format({'font_color': 'black', 'align': 'left'})
black_field_format   = workbook.add_format({'font_color': 'black', 'align': 'left'})
black_value_format   = workbook.add_format({'font_color': 'black', 'align': 'left',   'num_format': '0'})
black_flag_format    = workbook.add_format({'font_color': 'black', 'align': 'center'})
black_trans_format   = workbook.add_format({'font_color': 'black', 'align': 'left'})
gray_type_format     = workbook.add_format({'font_color': 'gray',  'align': 'left'})
gray_date_format     = workbook.add_format({'font_color': 'gray',  'num_format': 'yyyy-mm-dd hh:mm:ss.000'})
gray_url_format      = workbook.add_format({'font_color': 'gray',  'align': 'left'})
gray_field_format    = workbook.add_format({'font_color': 'gray',  'align': 'left'})
gray_value_format    = workbook.add_format({'font_color': 'gray',  'align': 'left', 'num_format': '0'})
red_type_format      = workbook.add_format({'font_color': 'red',   'align': 'left'})
red_date_format      = workbook.add_format({'font_color': 'red',   'num_format': 'yyyy-mm-dd hh:mm:ss.000'})
red_url_format       = workbook.add_format({'font_color': 'red',   'align': 'left'})
red_field_format     = workbook.add_format({'font_color': 'red',   'align': 'right'})
red_value_format     = workbook.add_format({'font_color': 'red',   'align': 'left', 'num_format': '0'})
green_type_format    = workbook.add_format({'font_color': 'green', 'align': 'left'})
green_date_format    = workbook.add_format({'font_color': 'green', 'num_format': 'yyyy-mm-dd hh:mm:ss.000'})
green_url_format     = workbook.add_format({'font_color': 'green', 'align': 'left'})
green_field_format   = workbook.add_format({'font_color': 'green', 'align': 'left'})
green_value_format   = workbook.add_format({'font_color': 'green', 'align': 'left'})
usbdevices = es.search(index='sbe.test', body={"query": {"term": {"_type": "tz_usp"}}})

usbdeviceset = usbdevices['hits']['hits']
rownumber=1
for usb in usbdeviceset:
    #print json.dumps(usb, indent=4)
    columnnumber=0
    usbd=usb['_source']
    for us in usbd: 
        w.write_string(rownumber, columnnumber, str(usbd[us]), black_type_format)
        columnnumber += 1
    print ""
    rownumber += 1
w.freeze_panes(2, 0)                # Freeze top row
w.autofilter(1, 0, rownumber, 16)  # Add autofilter

result = es.search(index='sbe.test', body={"size": 0,"aggs": {"serials": { "terms" :{"field" : "Volume Serial"}}}})
#print json.dumps(result, indent=4)
hits = result['aggregations']['serials']['buckets']
volume_serials = []
for hit in hits:
    #volume_serials.append(hit["key"])
    #volume_serials.append(hit["doc_count"])
    #print volume_serials
    volume=str(hit["key"])
    records = es.search(index='sbe.test', q=volume)
    record_set = records['hits']['hits']
    q=workbook.add_worksheet(volume)
    rownumber=1
    for record in record_set:
        #print record['_type'],",",record['_id'],
        
        sources=record['_source']
        columnnumber=0
        for source in sources:
            
            #print ",",sources[source],
            q.write_string(rownumber, columnnumber, str(sources[source]), black_type_format)
            columnnumber += 1
        print ""
        rownumber += 1
q.freeze_panes(2, 0)                # Freeze top row
q.autofilter(1, 0, rownumber, 16)
workbook.close()
