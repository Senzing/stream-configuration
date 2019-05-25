#! /usr/bin/env python

# -----------------------------------------------------------------------------
# stream-configuration.py Loader for streaming input.
# -----------------------------------------------------------------------------

import argparse
import configparser
import json
import logging
import os
import signal
import sys
import time
import confluent_kafka

# import pika

# Python 2 / 3 migration.

try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

# Import Senzing libraries.

try:
#     from G2ConfigTables import G2ConfigTables
#     from G2Engine import G2Engine
    import G2Exception
    from G2Database import G2Database

#     from G2Product import G2Product
#     from G2Project import G2Project
#     from G2Diagnostic import G2Diagnostic
except ImportError:
    pass

from flask import Flask
from flask import json
from flask import request as flask_request

app = Flask(__name__)

__all__ = []
__version__ = 1.0
__date__ = '2019-05-23'
__updated__ = '2019-05-25'

SENZING_PRODUCT_ID = "5004"  # See https://github.com/Senzing/knowledge-base/blob/master/lists/senzing-product-ids.md
log_format = '%(asctime)s %(message)s'

# Working with bytes.

KILOBYTES = 1024
MEGABYTES = 1024 * KILOBYTES
GIGABYTES = 1024 * MEGABYTES

# The "configuration_locator" describes where configuration variables are in:
# 1) Command line options, 2) Environment variables, 3) Configuration files, 4) Default values

config = {}
configuration_locator = {
    "debug": {
        "default": False,
        "env": "SENZING_DEBUG",
        "cli": "debug"
    },
    "g2_database_url": {
        "ini": {
            "section": "g2",
            "option": "G2Connection"
        }
    },
    "host": {
        "default": "0.0.0.0",
        "env": "SENZING_HOST",
        "cli": "host"
    },
    "input_url": {
        "default": None,
        "env": "SENZING_INPUT_URL",
        "cli": "input-url"
    },
    "kafka_bootstrap_server": {
        "default": "localhost:9092",
        "env": "SENZING_KAFKA_BOOTSTRAP_SERVER",
        "cli": "kafka-bootstrap-server",
    },
    "kafka_group": {
        "default": "senzing-config-kafka-group",
        "env": "SENZING_KAFKA_GROUP",
        "cli": "kafka-group"
    },
    "kafka_topic": {
        "default": "senzing-config-kafka-topic",
        "env": "SENZING_KAFKA_TOPIC",
        "cli": "kafka-topic"
    },
    "port": {
        "default": 5000,
        "env": "SENZING_PORT",
        "cli": "port"
    },
    "senzing_dir": {
        "default": "/opt/senzing",
        "env": "SENZING_DIR",
        "cli": "senzing-dir"
    },
    "subcommand": {
        "default": None,
        "env": "SENZING_SUBCOMMAND",
    }
}

# -----------------------------------------------------------------------------
# Define argument parser
# -----------------------------------------------------------------------------


def get_parser():
    '''Parse commandline arguments.'''
    parser = argparse.ArgumentParser(prog="stream-configuration.py", description="Configure Senzing metadata. For more information, see https://github.com/senzing/stream-configuration")
    subparsers = parser.add_subparsers(dest='subcommand', help='Subcommands (SENZING_SUBCOMMAND):')

    subparser_1 = subparsers.add_parser('url', help='Read JSON Lines from a URL addressable file.')
    subparser_1.add_argument("--debug", dest="debug", action="store_true", help="Enable debugging. (SENZING_DEBUG) Default: False")
    subparser_1.add_argument("--input-url", dest="input_url", metavar="SENZING_INPUT_URL", help="URL to file of JSON lines.")
    subparser_1.add_argument("--senzing-dir", dest="senzing_dir", metavar="SENZING_DIR", help="Location of Senzing. Default: /opt/senzing")

    subparser_2 = subparsers.add_parser('service', help='Receive HTTP requests.')
    subparser_2.add_argument("--debug", dest="debug", action="store_true", help="Enable debugging. (SENZING_DEBUG) Default: False")
    subparser_2.add_argument("--host", dest="host", metavar="SENZING_HOST", help="Host to listen on. Default: 0.0.0.0")
    subparser_2.add_argument("--port", dest="port", metavar="SENZING_PORT", help="Port to listen on. Default: 8080")
    subparser_2.add_argument("--senzing-dir", dest="senzing_dir", metavar="SENZING_DIR", help="Location of Senzing. Default: /opt/senzing")

    subparser_3 = subparsers.add_parser('kafka', help='Read JSON Lines from Apache Kafka topic.')
    subparser_3.add_argument("--debug", dest="debug", action="store_true", help="Enable debugging. (SENZING_DEBUG) Default: False")
    subparser_3.add_argument("--kafka-bootstrap-server", dest="kafka_bootstrap_server", metavar="SENZING_KAFKA_BOOTSTRAP_SERVER", help="Kafka bootstrap server. Default: localhost:9092")
    subparser_3.add_argument("--kafka-group", dest="kafka_group", metavar="SENZING_KAFKA_GROUP", help="Kafka group. Default: senzing-config-kafka-group")
    subparser_3.add_argument("--kafka-topic", dest="kafka_topic", metavar="SENZING_KAFKA_TOPIC", help="Kafka topic. Default: senzing-config-kafka-topic")
    subparser_3.add_argument("--senzing-dir", dest="senzing_dir", metavar="SENZING_DIR", help="Location of Senzing. Default: /opt/senzing")

    subparser_10 = subparsers.add_parser('docker-acceptance-test', help='For Docker acceptance testing.')

    return parser

# -----------------------------------------------------------------------------
# Message handling
# -----------------------------------------------------------------------------

# 1xx Informational (i.e. logging.info())
# 2xx Warning (i.e. logging.warn())
# 4xx User configuration issues (either logging.warn() or logging.err() for Client errors)
# 5xx Internal error (i.e. logging.error for Server errors)
# 9xx Debugging (i.e. logging.debug())


MESSAGE_INFO = 100
MESSAGE_WARN = 200
MESSAGE_ERROR = 400
MESSAGE_DEBUG = 900

message_dictionary = {
    "100": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}I",
    "101": "Enter {0}",
    "102": "Exit {0}",
    "110": "Successfully added CFG_DSRC.DSRC_ID: {0}.",
    "111": "Successfully deleted CFG_DSRC.DSRC_ID: {0}.",
    "112": "Successfully updated CFG_DSRC: {0}.",
    "197": "Version: {0}  Updated: {1}",
    "198": "For information on warnings and errors, see https://github.com/Senzing/stream-loader#errors",
    "199": "{0}",
    "200": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}W",
    "400": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}E",
    "401": "Cannot process request. No function for '{0}'.",
    "402": "Cannot process request. Method '{0}' not in {1}",
    "403": "Cannot process request. Key '{0}' not in request.",
    "498": "Bad SENZING_SUBCOMMAND: {0}.",
    "499": "No processing done.",
    "500": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}E",
    "501": "Error: {0} for {1}",
    "502": "Could not connect to database. URL: {0} Error type: {1} Error: {2}",
    "599": "Program terminated with error.",
    "900": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}D",
    "999": "{0}",
}


def message(index, *args):
    index_string = str(index)
    template = message_dictionary.get(index_string, "No message for index {0}.".format(index_string))
    return template.format(*args)


def message_generic(generic_index, index, *args):
    returnCode = 0
    if index >= MESSAGE_WARN:
        returnCode = index
    message_dictionary = {
        "returnCode": returnCode,
        "messageId": message(generic_index, index),
        "message":  message(index, *args),
    }
    return json.dumps(message_dictionary)


def message_info(index, *args):
    return message_generic(MESSAGE_INFO, index, *args)


def message_warn(index, *args):
    return message_generic(MESSAGE_WARN, index, *args)


def message_error(index, *args):
    return message_generic(MESSAGE_ERROR, index, *args)


def message_debug(index, *args):
    return message_generic(MESSAGE_DEBUG, index, *args)


def get_exception():
    ''' Get details about an exception. '''
    exception_type, exception_object, traceback = sys.exc_info()
    frame = traceback.tb_frame
    line_number = traceback.tb_lineno
    filename = frame.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, line_number, frame.f_globals)
    return {
        "filename": filename,
        "line_number": line_number,
        "line": line.strip(),
        "exception": exception_object,
        "type": exception_type,
        "traceback": traceback,
    }

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------


def get_g2project_ini_filename(args_dictionary):
    ''' Find the G2Project.ini file in the filesystem.'''

    # Possible locations for G2Project.ini

    filenames = [
        "{0}/g2/python/G2Project.ini".format(args_dictionary.get('senzing_dir', None)),
        "{0}/g2/python/G2Project.ini".format(os.getenv('SENZING_DIR', None)),
        "{0}/G2Project.ini".format(os.getcwd()),
        "{0}/G2Project.ini".format(os.path.dirname(os.path.realpath(__file__))),
        "{0}/G2Project.ini".format(os.path.dirname(os.path.abspath(sys.argv[0]))),
        "/etc/G2Project.ini",
        "/opt/senzing/g2/python/G2Project.ini",
    ]

    # Return first G2Project.ini found.

    for filename in filenames:
        final_filename = os.path.abspath(filename)
        if os.path.isfile(final_filename):
            return final_filename

    # If file not found, return error.

    logging.warn(message_warn(406))
    return None


def get_configuration(args):
    ''' Order of precedence: CLI, OS environment variables, INI file, default.'''
    result = {}

    # Copy default values into configuration dictionary.

    for key, value in configuration_locator.items():
        result[key] = value.get('default', None)

    # "Prime the pump" with command line args. This will be done again as the last step.

    for key, value in args.__dict__.items():
        new_key = key.format(subcommand.replace('-', '_'))
        if value:
            result[new_key] = value

    # Copy INI values into configuration dictionary.

    g2project_ini_filename = get_g2project_ini_filename(result)
    if g2project_ini_filename:

        result['g2project_ini'] = g2project_ini_filename

        config_parser = configparser.RawConfigParser()
        config_parser.read(g2project_ini_filename)

        for key, value in configuration_locator.items():
            keyword_args = value.get('ini', None)
            if keyword_args:
                try:
                    result[key] = config_parser.get(**keyword_args)
                except:
                    pass

    # Copy OS environment variables into configuration dictionary.

    for key, value in configuration_locator.items():
        os_env_var = value.get('env', None)
        if os_env_var:
            os_env_value = os.getenv(os_env_var, None)
            if os_env_value:
                result[key] = os_env_value

    # Copy 'args' into configuration dictionary.

    for key, value in args.__dict__.items():
        new_key = key.format(subcommand.replace('-', '_'))
        if value:
            result[new_key] = value

    # Special case: Remove variable of less priority.

    if result.get('project_filespec') and result.get('project_filename'):
        result.pop('project_filename')  # Remove key

    # Special case: subcommand from command-line

    if args.subcommand:
        result['subcommand'] = args.subcommand

    # Special case: Change boolean strings to booleans.

    booleans = ['debug']
    for boolean in booleans:
        boolean_value = result.get(boolean)
        if isinstance(boolean_value, str):
            boolean_value_lower_case = boolean_value.lower()
            if boolean_value_lower_case in ['true', '1', 't', 'y', 'yes']:
                result[boolean] = True
            else:
                result[boolean] = False

    # Special case: Change integer strings to integers.

    integers = ["port"]
    for integer in integers:
        integer_string = result.get(integer)
        result[integer] = int(integer_string)

    return result


def validate_configuration(config):
    '''Check aggregate configuration from commandline options, environment variables, config files, and defaults.'''

    user_warning_messages = []
    user_error_messages = []

#     if not config.get('g2_database_url'):
#         user_error_messages.append(message_error(401))

    # Perform subcommand specific checking.

    subcommand = config.get('subcommand')

    if subcommand in ['kafka', 'stdin', 'url']:
        pass

    if subcommand in ['stdin', 'url']:
        pass

    if subcommand in ['stdin']:
        pass

    if subcommand in ['kafka']:
        pass

    # Log warning messages.

    for user_warning_message in user_warning_messages:
        logging.warn(user_warning_message)

    # Log error messages.

    for user_error_message in user_error_messages:
        logging.error(user_error_message)

    # Log where to go for help.

    if len(user_warning_messages) > 0 or len(user_error_messages) > 0:
        logging.info(message_info(198))

    # If there are error messages, exit.

    if len(user_error_messages) > 0:
        exit_error(499)

# -----------------------------------------------------------------------------
# Utility functions
# -----------------------------------------------------------------------------


def create_signal_handler_function(config):
    '''Tricky code.  Uses currying technique. Create a function for signal handling.'''

    def result_function(signal_number, frame):
        stop_time = time.time()
        config['stopTime'] = stop_time
        result = {
            "returnCode": 0,
            "messageId":  message(100, 102),
            "elapsedTime": stop_time - config.get('startTime', stop_time),
            "status": "Exit via signal",
            "context": config,
        }

        # FIXME: Redact sensitive info:  Example: database password.

        logging.info(json.dumps(result, sort_keys=True))
        sys.exit(0)

    return result_function


def bootstrap_signal_handler(signal, frame):
    sys.exit(0)


def entry_template(config):
    '''Format of entry message.'''
    config['startTime'] = time.time()
    result = {
        "returnCode": 0,
        "messageId":  message(100, 101),
        "status": "Entry",
        "context": config,
    }

    # FIXME: Redact sensitive info:  Example: database password.

    return json.dumps(result, sort_keys=True)


def exit_template(config):
    '''Format of exit message.'''
    stop_time = time.time()
    config['stopTime'] = time.time()
    result = {
        "returnCode": 0,
        "messageId":  message(100, 102),
        "elapsedTime": stop_time - config.get('startTime', stop_time),
        "status": "Exit",
        "context": config,
    }

    # FIXME: Redact sensitive info:  Example: database password.

    return json.dumps(result, sort_keys=True)


def exit_error(index, *args):
    '''Log error message and exit program.'''
    logging.error(message_error(index, *args))
    logging.error(message_error(599))
    sys.exit(1)


def exit_silently():
    '''Exit program.'''
    sys.exit(1)

# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------


def common_prolog(config):

    validate_configuration(config)

    # Prolog.

    logging.info(entry_template(config))


def get_config():
    return config

# -----------------------------------------------------------------------------
#
# -----------------------------------------------------------------------------


def create_input_lines_generator_factory(config):
    '''Choose which input_lines_from_* function should be used.'''

    def input_lines_from_stdin():
        '''A generator for reading lines from STDIN.'''

        # Note: The alternative, 'for line in sys.stdin:',  suffers from a 4K buffering issue.

        reading = True
        while reading:
            line = sys.stdin.readline()
            if line:
                yield line.strip()
            else:
                reading = False  # FIXME: Not sure if this is the best method of exiting.

    def input_lines_from_file():
        '''A generator for reading lines from a local file.'''
        with open(parsed_file_name.path, 'r') as lines:
            for line in lines:
                yield line.strip()

    def input_lines_from_url():
        '''A generator for reading lined from a URL-addressable file.'''
        with urlopen(input_url) as lines:
            for line in lines:
                yield line.strip()

    result = None
    input_url = config.get('input_url')

    # If no file, input comes from STDIN.

    if not input_url:
        return input_lines_from_stdin

    # Return a function based on URI protocol.

    parsed_file_name = urlparse(input_url)
    if parsed_file_name.scheme in ['http', 'https']:
        result = input_lines_from_url
    elif parsed_file_name.scheme in ['file', '']:
        result = input_lines_from_file
    return result

# -----------------------------------------------------------------------------
# Database routines.
# -----------------------------------------------------------------------------


sql_dictionary = {

    # 11x CFG_DSRC SQL statements.

    "110": "select DSRC_ID, DSRC_CODE, DSRC_DESC, DSRC_RELY, RETENTION_LEVEL, CONVERSATIONAL from 'CFG_DSRC'",
    "111": "select * from 'CFG_DSRC' where DSRC_ID = {0}",
    "112": "insert into CFG_DSRC (DSRC_ID, DSRC_CODE, DSRC_DESC, DSRC_RELY, RETENTION_LEVEL, CONVERSATIONAL) values ({DSRC_ID}, \"{DSRC_CODE}\", \"{DSRC_DESC}\", {DSRC_RELY}, \"{RETENTION_LEVEL}\", \"{CONVERSATIONAL}\")",
    "113": "update CFG_DSRC set {0} where DSRC_ID = {1}",
    "119": "delete from CFG_DSRC where DSRC_ID = {0}",

    # Generic SQL statements.

    "801": "select max({0}) as {0} from '{1}'",
}


def get_g2_database(config):
    '''Get the G2Engine resource.'''

    g2_database_url = config.get('g2_database_url')
    try:
        result = G2Database(g2_database_url)
    except G2Exception.G2UnsupportedDatabaseType as err:
        exit_error(502, g2_database_url, "G2UnsupportedDatabaseType", err)
    except G2Exception.G2DBMNotStarted as err:
        exit_error(502, g2_database_url, "G2DBMNotStarted", err)
    except G2Exception.G2DBNotFound as err:
        exit_error(502, g2_database_url, "G2DBNotFound", err)
    except G2Exception.G2ModuleException as err:
        exit_error(502, g2_database_url, "G2ModuleException", err)
    except G2Exception.G2DBUnknownException as err:
        exit_error(502, g2_database_url, "G2DBUnknownException", err)
    except G2Exception.G2TableNoExist as err:
        exit_error(502, g2_database_url, "G2TableNoExist", err)
    except G2Exception.G2DBUniqueConstraintViolation as err:
        exit_error(502, g2_database_url, "G2DBUniqueConstraintViolation", err)
    except G2Exception.G2DBException as err:
        exit_error(502, g2_database_url, "G2DBException", err)
    except Exception as err:
        exit_error(502, g2_database_url, "Exception", err)
    return result


def database_exec(config, sql):
    g2_database = get_g2_database(config)
    return g2_database.sqlExec(sql)


def database_select(config, sql):
    g2_database = get_g2_database(config)
    sql_cursor = g2_database.sqlExec(sql)
    if not sql_cursor:
        return
    result = g2_database.fetchNext(sql_cursor)
    while result:
        yield result
        result = g2_database.fetchNext(sql_cursor)


def database_select_single_row(config, sql):
    g2_database = get_g2_database(config)
    sql_cursor = g2_database.sqlExec(sql)
    if not sql_cursor:
        return
    return g2_database.fetchNext(sql_cursor)

# -----------------------------------------------------------------------------
# Error reporting.
# -----------------------------------------------------------------------------


def missing_key(key, request):
    message_number = 403
    result = {
        'returnCode': message_number,
        'messageId': message(MESSAGE_WARN, message_number),
        'message': message(message_number, key),
        'request': request,
    }
    return result


def bad_method(request, method, methods):
    message_number = 402
    methods_string = ', '.join(methods)
    result = {
        'returnCode': message_number,
        'messageId': message(MESSAGE_WARN, message_number),
        'message': message(message_number, method, methods_string),
        'request': request,
    }
    return result


def bad_function(request, function):
    message_number = 401
    result = {
        'returnCode': message_number,
        'messageId': message(MESSAGE_WARN, message_number),
        'message': message(message_number, function),
        'request': request,
    }
    return result

# -----------------------------------------------------------------------------
# handle_* functions
# Input parameters:
#   - config is the dictionary with the configuration.
#   - message is a dictionary.
# Output: a dictionary.
# -----------------------------------------------------------------------------

# ----- datasource ------------------------------------------------------------


def handle_post_datasources(config, request):
    result = {}

    # Verify input request.

    if 'DSRC_CODE' not in request:
        return missing_key('DSRC_CODE', request)

    # Find next ID.

    sql = sql_dictionary.get('801').format('DSRC_ID', 'CFG_DSRC')
    current_id_row = database_select_single_row(config, sql)
    current_id = max(999, current_id_row.get("DSRC_ID", 0))
    next_id = current_id + 1

    # Add defaults.

    defaults = {
        "DSRC_ID": next_id,
        "DSRC_DESC": request.get('DSRC_CODE', ""),
        "DSRC_RELY": 1,
        "RETENTION_LEVEL": "Remember",
        "CONVERSATIONAL": "No",
    }

    for key, value in defaults.items():
        if key not in request:
            request[key] = value

    # Insert into database.

    sql = sql_dictionary.get('112').format(**request)
    sql_result = database_exec(config, sql)

    # Construct and return result.

    result = {
        'returnCode': 0,
        'messageId': message(MESSAGE_INFO, 110),
        'message': message(110, next_id),
        'request': request,
    }

    return result


def handle_put_datasources(config, request):
    result = {}

    # Verify input request.

    if 'DSRC_ID' not in request:
        return missing_key('DSRC_ID', request)

    # Remove DSRC_ID from request.

    id = request.pop('DSRC_ID')

    # Calculate SQL "set" clause.

    set_clause = []
    for key, value in request.items():
        if isinstance(value, str):
            value = "\"{0}\"".format(value)
        set_clause.append("{0} = {1}".format(key, value))

    # Construct and execute SQL statement.

    sql = sql_dictionary.get('113').format(", ".join(set_clause), id)
    sql_result = database_exec(config, sql)

    # Construct and return result.

    result = {
        'returnCode': 0,
        'messageId': message(MESSAGE_INFO, 112),
        'message': message(112, id),
        'request': request,
    }

    return result


def handle_get_datasources(config, request):
    result = []
    sql = sql_dictionary.get('110')
    for row in database_select(config, sql):
        result.append(row)
    return result


def handle_get_datasource(config, request):
    row_id = request.get('DSRC_ID')
    sql = sql_dictionary.get('111').format(row_id)
    for row in database_select(config, sql):
        return row


def handle_delete_datasources(config, request):
    row_id = request.get('DSRC_ID')
    sql = sql_dictionary.get('119').format(row_id)
    result = database_exec(config, sql)
    print(result)
    result = {
        'returnCode': 0,
        'messageId': message(MESSAGE_INFO, 111),
        'message': message(111, row_id),
        'request': request,
    }
    return result

# -----------------------------------------------------------------------------
# message router
# -----------------------------------------------------------------------------


def route(config, message_dictionary):

    methods = ["post", "put", "get", "delete"]

    # Parse message.

    method = message_dictionary.get("method", "").lower()
    object = message_dictionary.get("object", "").lower()
    request = message_dictionary.get("request", {})

    # Verify method.

    if method not in methods:
        return json.dumps(bad_method(message_dictionary, method, methods))

    # Create a function name.

    route_function_name = "handle_{0}_{1}".format(method.replace('-', '_'), object.replace('-', '_'),)

    # Test to see if function exists in the code.

    if route_function_name not in globals():
        return json.dumps(bad_function(message_dictionary, object))

    # Tricky code for calling function based on string.

    result = globals()[route_function_name](config, request)
    return json.dumps(result)

# -----------------------------------------------------------------------------
# Flask @app.routes
# -----------------------------------------------------------------------------


@app.route("/generic", methods=['POST'])
def http_post_generic():
    config = get_config()
    return route(config, flask_request.json)

# ----- datasources -----------------------------------------------------------


@app.route("/datasources", methods=['POST'])
def http_post_datasource():
    config = get_config()
    request = {
        "method": "post",
        "object": "datasources",
        "request": flask_request.json
    }
    return route(config, request)


@app.route("/datasources/<id>", methods=['PUT'])
def http_put_datasource(id):
    config = get_config()
    request = {
        "DSRC_ID": id,
    }
    request.update(flask_request.json)
    request = {
        "method": "put",
        "object": "datasources",
        "request": request
    }
    return route(config, request)


@app.route("/datasources", methods=['GET'])
def http_get_datasources():
    config = get_config()
    request = {
        "method": "get",
        "object": "datasources"
    }
    return route(config, request)


@app.route("/datasources/<id>", methods=['GET'])
def http_get_datasource(id):
    config = get_config()
    request = {
        "method": "get",
        "object": "datasource",
        "request": {
            "DSRC_ID": id,
        }
    }
    return route(config, request)


@app.route("/datasources/<id>", methods=['DELETE'])
def http_delete_datasources(id):
    config = get_config()
    request = {
        "method": "delete",
        "object": "datasources",
        "request": {
            "DSRC_ID": id,
        }
    }
    return route(config, request)

# -----------------------------------------------------------------------------
# do_* functions
#   Common function signature: do_XXX(args)
# -----------------------------------------------------------------------------


def do_docker_acceptance_test(config):
    '''Mock command for docker acceptance testing.'''
    logging.info(entry_template(config))
    logging.info(exit_template(config))


def do_kafka(config):
    '''Read from Kafka.'''

    # Perform common initialization tasks.

    common_prolog(config)

    # Create Kafka client.

    consumer_configuration = {
        'bootstrap.servers': config.get('kafka_bootstrap_server'),
        'group.id': config.get("kafka_group"),
        'enable.auto.commit': False,
        'auto.offset.reset': 'earliest'
        }
    consumer = confluent_kafka.Consumer(consumer_configuration)
    consumer.subscribe([config.get("kafka_topic")])

    # In a loop, get messages from Kafka.

    while True:

        # Get message from Kafka queue.
        # Timeout quickly to allow other co-routines to process.

        kafka_message = consumer.poll(1.0)

        # Handle non-standard Kafka output.

        if kafka_message is None:
            continue
        if kafka_message.error():
            if kafka_message.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
                continue
            else:
                logging.error(message_error(508, kafka_message.error()))
                continue

        # Construct and verify Kafka message.

        kafka_message_string = kafka_message.value().strip()
        if not kafka_message_string:
            continue

        input_dictionary = json.loads(kafka_message_string)
        response = route(config, input_dictionary)
        logging.info(response)

    # Epilog.

    logging.info(exit_template(config))


def do_service(config):
    '''Read from URL-addressable file.'''

    common_prolog(config)

    host = config.get('host')
    port = config.get('port')
    debug = config.get('debug')

    app.run(host=host, port=port, debug=debug)

    # Epilog.

    logging.info(exit_template(config))


def do_url(config):
    '''Read from URL-addressable file.'''

    common_prolog(config)

    # Pull values from configuration.

    input_lines = create_input_lines_generator_factory(config)

    # Iterate through file.

    for input_line in input_lines():
        if input_line:
            input_dictionary = json.loads(input_line)
            response = route(config, input_dictionary)
            logging.info(response)

    # Epilog.

    logging.info(exit_template(config))


def do_version(args):
    '''Log version information.'''

    logging.info(message_info(197, __version__, __updated__))

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


if __name__ == "__main__":

    # Configure logging. See https://docs.python.org/2/library/logging.html#levels

    log_level_map = {
        "notset": logging.NOTSET,
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "fatal": logging.FATAL,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL
    }

    log_level_parameter = os.getenv("SENZING_LOG_LEVEL", "info").lower()
    log_level = log_level_map.get(log_level_parameter, logging.INFO)
    logging.basicConfig(format=log_format, level=log_level)

    # Trap signals temporarily until args are parsed.

    signal.signal(signal.SIGTERM, bootstrap_signal_handler)
    signal.signal(signal.SIGINT, bootstrap_signal_handler)

    # Parse the command line arguments.

    subcommand = os.getenv("SENZING_SUBCOMMAND", None)
    parser = get_parser()
    if len(sys.argv) > 1:
        args = parser.parse_args()
        subcommand = args.subcommand
    elif subcommand:
        args = argparse.Namespace(subcommand=subcommand)
    else:
        parser.print_help()
        if len(os.getenv("SENZING_DOCKER_LAUNCHED", "")):
            subcommand = "sleep"
            args = argparse.Namespace(subcommand=subcommand)
            do_sleep(args)
        exit_silently()

    # Get configuration

    config = get_configuration(args)

    # Catch interrupts. Tricky code: Uses currying.

    signal_handler = create_signal_handler_function(config)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Transform subcommand from CLI parameter to function name string.

    subcommand_function_name = "do_{0}".format(subcommand.replace('-', '_'))

    # Test to see if function exists in the code.

    if subcommand_function_name not in globals():
        logging.warn(message_warn(498, subcommand))
        parser.print_help()
        exit_silently()

    # Tricky code for calling function based on string.

    globals()[subcommand_function_name](config)
