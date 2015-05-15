 nagios-zookeeper
A tool to check znodes status on zookeeper, using Nagios Plugin API.

Usage.
```
 json_val_check.py 
 -h | --hosts              Zookeeper hosts seperated by comma(,)
 -p | --path               Path
 -k | --json-key           Json key to check
 -v | --json-value         Json value to check
 -m | --match-num          lowerbound:upperbound Inclusive bounds of allowed number of matched nodes 
 -d | --mismatch-num       lowerbound:upperbound Inclusive bounds of allowed number of mismatched nodes  
 Eg. zookeeper_checking.py -h server_1:2181,server_2:2181,server_3:2181
                           -p /foo
                           -k bar
                           -v baz
                           -m 0:3
                           -d 0:3
```
