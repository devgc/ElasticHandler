# ElasticHandler
Assorted classes and methods for indexing reports and retrieving information from an elastic index.

Indexing a file:
```
elastichandler.py --host 127.0.0.1 --index sbe.test --config etc\sbe_config.json --report report_examples\sbe.donald.usrclass.dat.tsv
```

## Config Files
When indexing a file with the elastichandler, you must pass in a configuration file. This file tells the handler how to index the report.

http://binaryforay.blogspot.com/p/software.html
Example for Eric Zimmerman's SBECmd.exe version 0.6.1.0 report:
```
{
	#delimiter of report columns#
	"delimiter":"\t",
	
	#line to start indexing from#
	"start_line":"2",
	
	#name of document type#
	"type":"sbe",
	
	#Mapping to create for the type#
	"map_file":"etc\\sbe_0.6.1.0.mapping",
	
	#Column order and names#
	"columns":[
		"BagPath",
		"Slot",
		"NodeSlot",
		"MRUPosition",
		"AbsolutePath",
		"ShellType",
		"Value",
		"ChildBags",
		"CreatedOn",
		"ModifiedOn",
		"AccessedOn",
		"LastWriteTime",
		"MFTEntry",
		"MFTSequenceNumber",
		"ExtensionBlockCount",
		"FirstExplored",
		"LastExplored",
		"Miscellaneous"
	],
	
	#Extra columns to create#
	"add_columns":{
		
	}
}
```

### Adding Columns
## Mapping Files
The *map_file* attribute points to a json file that is used to apply the document mapping to the document type specified by the *type* attribute.