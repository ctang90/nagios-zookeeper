from kazoo.client import KazooClient
from sets import Set
import os
import json
import sys
from copy import copy
from optparse import Option, OptionValueError, OptionParser

def check_bound(option, opt, value):
	try:
		bound = {}
		delim = value.index(':')
		if delim > 0:
			bound['lower'] = int(float(value[:delim]))
		if delim < (len(value) - 1):
			bound['upper'] = int(float(value[delim+1:]))
		return bound
	except ValueError:
		raise OptionValueError(
			'option %s: invalid bound value: %r' %(opt, value))

class MyOption(Option):
	TYPES = Option.TYPES + ("bound",)
	TYPE_CHECKER = copy(Option.TYPE_CHECKER)
	TYPE_CHECKER["bound"] = check_bound

# Input args: 
# -h | --hosts              Zookeeper hosts seperated by comma(,)
# -p | --path               Path
# -k | --json-key           Json key to check
# -v | --json-value         Json value to check
# -m | --match-num          lowerbound:upperbound Inclusive bounds of allowed number of matched nodes 
# -d | --mismatch-num       lowerbound:upperbound Inclusive bounds of allowed number of mismatched nodes  
# Eg. zookeeper_checking.py -h server_1:2181,server_2:2181,server_3:2181
#                           -p /foo
#                           -k bar
#                           -v baz
#                           -m 0:3
#                           -d 0:3
def check_config(config_dict):
	valid_config = True
	required_field=['server', 'path', 'json_key', 'json_val', 'match', 'mismatch']
	for field in required_field:
		if config_dict[field] == None:
			print 'Need to specify', field, 'in command line!'
			valid_config = False
	return valid_config

# OK = 0
# Only if can connect to hosts 
# AND       Path exist
# AND       Json key exist
# AND       matched value is within the bound
# AND       mismatched value is within the bound

# WARNING = 1
# Only if cannot connect to hosts
# OR the specified path doesn't exist
# OR JSON key doesn't exist

# CRITICAL = 2
# Only if matched value is outof the bound
# OR mismatched value is outof the bound

# UNKNOWN = 3
OK = 0
WARNING = 1
CRITICAL = 2
def zk_check(config_dict):
	zk_client = KazooClient(hosts = config_dict['server'])
	try:
		zk_client.start()
	except timeout_exception:
		print 'Cannot connect to', config_dict['server']
		return WARNING
	if zk_client.exists(config_dict['path']) == None:
		print 'Specified path', config_dict['path'], 'does not exist'
		zk_client.stop()
		zk_client.close()
		return WARNING
	try:
		children = zk_client.get_children(config_dict['path'])
	except ZookeeperError:
		print 'An error happens when retrieving information of path', \
			   config_dict['path']

		zk_client.stop()
		zk_client.close()
		return WARNING
	matched_num = 0
	mismatch_num = 0
	for child in children:
		child_path = os.path.join(config_dict['path'], child)
		try:
			values = zk_client.get(child_path)
		except ZookeeperError:
			print 'An error happens when retrieving information of path', \
				   child_path
			zk_client.stop()
			zk_client.close()
			return WARNING

		loads = json.loads(values[0])
		if config_dict['json_key'] not in loads.keys():
			print 'Cannot find', config_dict['json_key'], 'in values of', \
				   child_path, ':', values
			zk_client.stop()
			zk_client.close()
			return WARNING
		if loads[config_dict['json_key']] == config_dict['json_val']:
			matched_num = matched_num + 1
		else:
			mismatch_num = mismatch_num + 1

	valid = True
	if 'lower' in config_dict['match'] and \
		matched_num < config_dict['match']['lower']:
		print 'znodes meeting requirement number:', matched_num, \
			  'less than expected:', str(config_dict['match']['lower'])
		valid = False

	if 'upper' in config_dict['match'] and \
		matched_num > config_dict['match']['upper']:
		print 'znodes meeting requirement number:', matched_num, \
			  'more than expected:', str(config_dict['match']['upper'])
		valid = False

	if 'lower' in config_dict['mismatch'] and \
		matched_num < config_dict['mismatch']['lower']:
		print 'znodes not meeting requirement number:', mismatch_num, \
			  'less than expected:', str(config_dict['mismatch']['lower'])
		valid = False

	if 'upper' in config_dict['mismatch'] and \
		matched_num > config_dict['mismatch']['upper']:
		print 'znodes meeting requirement number:', mismatch_num, \
			  'more than expected:', str(config_dict['mismatch']['upper'])
		valid = False

	zk_client.stop()
	zk_client.close()
	if valid == False:
		return CRITICAL
	else:
		return OK


def main():
	usage = 'usage: %prog [options] arg'
	parser = OptionParser(usage = usage, option_class=MyOption)
	parser.add_option('-s', '--server', dest='server', 
					  help='Zookeeper servers seperated by comma(,)')
	parser.add_option('-p', '--path', dest='path',
					  help='Zookeeper path to check')
	parser.add_option('-k', '--json-key', dest='json_key',
					  help='Json key to check')
	parser.add_option('-v', '--json-value', dest='json_val',
					  help='Json value to check')
	parser.add_option('-m', '--match-num', dest='match', type='bound',
					  help='lowerbound:upperbound \
					  Inclusive bounds of allowed number of matched nodes')
	parser.add_option('-d', '--mismatch-num', dest='mismatch', type='bound',
					  help='lowerbound:upperbound \
					  Inclusive bounds of allowed number of mismatched nodes')
	(options, args) = parser.parse_args()
	if check_config(options.__dict__) == True:
		sys.exit(zk_check(options.__dict__))
	sys.exit(UNKNOWN)

if __name__ == '__main__':
	main()
