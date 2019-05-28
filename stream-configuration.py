#! /usr/bin/env python

# -----------------------------------------------------------------------------
# stream-configuration.py Loader for streaming input.
# -----------------------------------------------------------------------------

import argparse
import configparser
import json
import linecache
import logging
import os
import signal
import six
import sys
import time
import confluent_kafka
import pika
from cryptography.hazmat.primitives.asymmetric.padding import PSS

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
    import G2Exception
    from G2Database import G2Database
except ImportError:
    pass

from flask import Flask
from flask import json
from flask import request as flask_request

app = Flask(__name__)

__all__ = []
__version__ = 1.0
__date__ = '2019-05-23'
__updated__ = '2019-05-28'

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
    "rabbitmq_host": {
        "default": "localhost:5672",
        "env": "SENZING_RABBITMQ_HOST",
        "cli": "rabbitmq-host",
    },
    "rabbitmq_password": {
        "default": "bitnami",
        "env": "SENZING_RABBITMQ_PASSWORD",
        "cli": "rabbitmq-password",
    },
    "rabbitmq_queue": {
        "default": "senzing-rabbitmq-queue",
        "env": "SENZING_RABBITMQ_QUEUE",
        "cli": "rabbitmq-queue",
    },
    "rabbitmq_username": {
        "default": "user",
        "env": "SENZING_RABBITMQ_USERNAME",
        "cli": "rabbitmq-username",
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

    subparser_4 = subparsers.add_parser('rabbitmq', help='Read JSON Lines from RabbitMQ queue.')
    subparser_4.add_argument("--debug", dest="debug", action="store_true", help="Enable debugging. (SENZING_DEBUG) Default: False")
    subparser_4.add_argument("--rabbitmq-host", dest="rabbitmq_host", metavar="SENZING_rabbitmq_host", help="RabbitMQ host. Default: localhost:5672")
    subparser_4.add_argument("--rabbitmq-queue", dest="rabbitmq_queue", metavar="SENZING_RABBITMQ_QUEUE", help="RabbitMQ queue. Default: senzing-rabbitmq-queue")
    subparser_4.add_argument("--rabbitmq-username", dest="rabbitmq_username", metavar="SENZING_RABBITMQ_USERNAME", help="RabbitMQ username. Default: user")
    subparser_4.add_argument("--rabbitmq-password", dest="rabbitmq_password", metavar="SENZING_RABBITMQ_PASSWORD", help="RabbitMQ password. Default: bitnami")
    subparser_4.add_argument("--senzing-dir", dest="senzing_dir", metavar="SENZING_DIR", help="Location of Senzing. Default: /opt/senzing")

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
    "110": "Successfully added {table_name}.{id}: {id_value}",
    "111": "Successfully deleted {table_name}.{id}: {id_value}",
    "112": "Successfully updated {table_name}.{id}: {id_value}",
    "197": "Version: {0}  Updated: {1}",
    "198": "For information on warnings and errors, see https://github.com/Senzing/stream-loader#errors",
    "199": "{0}",
    "200": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}W",
    "202": "Non-fatal exception on Line {0}: {1} Error: {2}",
    "400": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}E",
    "401": "Cannot process request. No function for '{0}'.",
    "402": "Cannot process request. Method '{0}' not in {1}",
    "403": "Cannot process request. Key '{0}' not in request.",
    "418": "Could not connect to RabbitMQ host at {1}. The host name maybe wrong, it may not be ready, or your credentials are incorrect. See the RabbitMQ log for more details. Error: {0}",
    "406": "Cannot find G2Project.ini.",
    "498": "Bad SENZING_SUBCOMMAND: {0}",
    "499": "No processing done.",
    "500": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}E",
    "501": "Error: {0} for {1}",
    "502": "Could not connect to database. URL: {0} Error type: {1} Error: {2}",
    "599": "Program terminated with error.",
    "900": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}D",
    "901": "Execute SQL: {0}",
    "999": "{0}",
}


def message(index, *args):
    index_string = str(index)
    template = message_dictionary.get(index_string, "No message for index {0}.".format(index_string))
    return template.format(*args)


def message_kwargs(index, **kwargs):
    index_string = str(index)
    template = message_dictionary.get(index_string, "No message for index {0}.".format(index_string))
    return template.format(**kwargs)


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
        lines = urlopen(input_url)
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
    "112": "insert into {table_name} ({column_list}) values ({value_list})",
    "113": "update {table_name} set {update_list} where {id} = {id_value}",
    "114": "delete from {table_name} where {id} = {id_value}",
    "121": "select {column_list} from '{table_name}'",
    "122": "select * from '{table_name}' where {id} = {id_value}",
    "123": "select max({id}) as {id} from '{table_name}'",
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
    result = {}
    logging.debug(message_debug(901, sql))
    g2_database = get_g2_database(config)
    try:
        result = g2_database.sqlExec(sql)
    except:
        exception = get_exception()
        logging.warn(message_warn(202, exception.get('line_number'), exception.get('line'), exception.get('exception')))
    return result


def database_select(config, sql):
    result = {}
    logging.debug(message_debug(901, sql))
    g2_database = get_g2_database(config)
    try:
        sql_cursor = g2_database.sqlExec(sql)
    except:
        exception = get_exception()
        logging.warn(message_warn(202, exception.get('line_number'), exception.get('line'), exception.get('exception')))
        return
    if not sql_cursor:
        return
    result = g2_database.fetchNext(sql_cursor)
    while result:
        yield result
        result = g2_database.fetchNext(sql_cursor)


def database_select_single_row(config, sql):
    result = {}
    logging.debug(message_debug(901, sql))
    g2_database = get_g2_database(config)
    try:
        sql_cursor = g2_database.sqlExec(sql)
    except:
        exception = get_exception()
        logging.warn(message_warn(202, exception.get('line_number'), exception.get('line'), exception.get('exception')))
    if not sql_cursor:
        return
    return g2_database.fetchNext(sql_cursor)

# -----------------------------------------------------------------------------
# Database routines.
# database_*(config, table_metadata)
# -----------------------------------------------------------------------------


def database_delete_by_id(config, table_metadata):
    sql = sql_dictionary.get('114').format(**table_metadata)
    sql_result = database_exec(config, sql)
    result = {
        'returnCode': 0,
        'messageId': message(MESSAGE_INFO, 111),
        'message': message_kwargs(111, **table_metadata),
        'request': table_metadata.get('request', {}),
    }
    return result


def database_insert(config, table_metadata):
    request = table_metadata.get('request', {})
    defaults = table_metadata.get('defaults', {})
    id = table_metadata.get('id')

    # If ID is not a positive integer less than 1000, just remove it.

    if (id in request) and (request.get(id, 0) < 1000):
        request.pop(id)

    # Find next ID.

    max_id = database_max_id(config, table_metadata)
    defaults[id] = max(999, max_id) + 1

    # Add defaults to insert.

    defaults.update(request)

    # Calculate column and values list.

    columns = []
    values = []
    for column, value in defaults.items():
        columns.append(column)
        if isinstance(value, six.string_types):
            value = "\"{0}\"".format(value)
        elif isinstance(value, six.integer_types):
            value = str(value)
        values.append(value)

    # Update table_metadata.

    table_metadata['id_value'] = defaults.get(id)
    table_metadata['column_list'] = ', '.join(columns)
    table_metadata['value_list'] = ', '.join(values)

    # Insert into database.

    sql = sql_dictionary.get('112').format(**table_metadata)
    sql_result = database_exec(config, sql)

    # Construct and return result.

    request[id] = defaults.get(id)

    result = {
        'returnCode': 0,
        'messageId': message(MESSAGE_INFO, 110),
        'message': message_kwargs(110, **table_metadata),
        'request': request,
    }

    return result


def database_max_id(config, table_metadata):
    sql = sql_dictionary.get('123').format(**table_metadata)
    row = database_select_single_row(config, sql)
    return row.get(table_metadata.get('id'), 0)


def database_select_all(config, table_metadata):
    result = []
    sql = sql_dictionary.get('121').format(**table_metadata)
    for row in database_select(config, sql):
        result.append(row)
    return result


def database_select_by_id(config, table_metadata):
    sql = sql_dictionary.get('122').format(**table_metadata)
    for row in database_select(config, sql):
        return row


def database_update_by_id(config, table_metadata):
    request = table_metadata.get('request', {})
    id = table_metadata.get('id')

    # Verify input request.

    if id not in request:
        return missing_key(id, request)

    # Remove ID from request.

    table_metadata['id_value'] = request.pop(id)

    # Calculate SQL "set" clause.

    update_list = []
    for key, value in request.items():
        if isinstance(value, six.string_types):
            value = "\"{0}\"".format(value)
        update_list.append("{0} = {1}".format(key, value))
    table_metadata['update_list'] = ", ".join(update_list)

    # Construct and execute SQL statement.

    sql = sql_dictionary.get('113').format(**table_metadata)
    sql_result = database_exec(config, sql)

    # Construct and return result.

    result = {
        'returnCode': 0,
        'messageId': message(MESSAGE_INFO, 112),
        'message': message_kwargs(112, **table_metadata),
        'request': request,
    }

    return result

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
# get_table_metadata_*
# -----------------------------------------------------------------------------


def post_process_table_metadata(result):
    result['column_list'] = ', '.join(result.get('columns'))
    return result


def get_table_metadata_cfg_dsrc():
    result = {
        "columns": [
            "DSRC_ID",
            "DSRC_CODE",
            "DSRC_DESC",
            "DSRC_RELY",
            "RETENTION_LEVEL",
            "CONVERSATIONAL",
        ],
        "defaults": {
            "DSRC_RELY": 1,
            "RETENTION_LEVEL": "Remember",
            "CONVERSATIONAL": "No",
        },
        "id": "DSRC_ID",
        "id_value": 0,
        "table_name": "CFG_DSRC",
    }
    return post_process_table_metadata(result)


def get_table_metadata_cfg_etype():
    result = {
        "columns": [
            "ETYPE_ID",
            "ETYPE_CODE",
            "ETYPE_DESC",
            "ECLASS_ID",
        ],
        "defaults": {
            "ECLASS_ID": 1,
        },
        "id": "ETYPE_ID",
        "id_value": 0,
        "table_name": "CFG_ETYPE",
    }
    return post_process_table_metadata(result)

# -----------------------------------------------------------------------------
# handle_* functions
# Input parameters:
#   - config is the dictionary with the configuration.
#   - message is a dictionary.
# Output: a dictionary.
# -----------------------------------------------------------------------------


def handle_post(config, request, table_metadata):
    table_metadata['request'] = request
    return database_insert(config, table_metadata)


def handle_put(config, request, table_metadata):
    table_metadata['id_value'] = request.get(table_metadata.get('id'))
    table_metadata['request'] = request
    return database_update_by_id(config, table_metadata)


def handle_get(config, request, table_metadata):
    return database_select_all(config, table_metadata)


def handle_get_single(config, request, table_metadata):
    table_metadata['id_value'] = request.get(table_metadata.get('id'))
    return database_select_by_id(config, table_metadata)


def handle_delete(config, request, table_metadata):
    table_metadata['id_value'] = request.get(table_metadata.get('id'))
    return database_delete_by_id(config, table_metadata)

# ----- data-source -----------------------------------------------------------


def handle_post_data_sources(config, request):
    table_metadata = get_table_metadata_cfg_dsrc()
    table_metadata['defaults']['DSRC_DESC'] = request.get('DSRC_CODE', "")
    return handle_post(config, request, table_metadata)


def handle_put_data_sources(config, request):
    return handle_put(config, request, get_table_metadata_cfg_dsrc())


def handle_get_data_sources(config, request):
    return handle_get(config, request, get_table_metadata_cfg_dsrc())


def handle_get_data_source(config, request):
    return handle_get_single(config, request, get_table_metadata_cfg_dsrc())


def handle_delete_data_sources(config, request):
    return handle_delete(config, request, get_table_metadata_cfg_dsrc())

# ----- entity_type -----------------------------------------------------------


def handle_post_entity_types(config, request):
    table_metadata = get_table_metadata_cfg_etype()
    table_metadata['defaults']['ETYPE_DESC'] = request.get('ETYPE_CODE', "")
    return handle_post(config, request, table_metadata)


def handle_put_entity_types(config, request):
    return handle_put(config, request, get_table_metadata_cfg_etype())


def handle_get_entity_types(config, request):
    return handle_get(config, request, get_table_metadata_cfg_etype())


def handle_get_entity_type(config, request):
    return handle_get_single(config, request, get_table_metadata_cfg_etype())


def handle_delete_entity_types(config, request):
    return handle_delete(config, request, get_table_metadata_cfg_etype())

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

# ----- entity-type -----------------------------------------------------------


@app.route("/entity-types", methods=['POST'])
def http_post_entity_type():
    config = get_config()
    request = {
        "method": "post",
        "object": "entity_types",
        "request": flask_request.json
    }
    return route(config, request)


@app.route("/entity-types/<id>", methods=['PUT'])
def http_put_entity_type(id):
    config = get_config()
    request = {
        "ETYPE_ID": id,
    }
    request.update(flask_request.json)
    request = {
        "method": "put",
        "object": "entity_types",
        "request": request
    }
    return route(config, request)


@app.route("/entity-types", methods=['GET'])
def http_get_entity_types():
    config = get_config()
    request = {
        "method": "get",
        "object": "entity_types"
    }
    return route(config, request)


@app.route("/entity-types/<id>", methods=['GET'])
def http_get_entity_type(id):
    config = get_config()
    request = {
        "method": "get",
        "object": "entity_type",
        "request": {
            "ETYPE_ID": id,
        }
    }
    return route(config, request)


@app.route("/entity-types/<id>", methods=['DELETE'])
def http_delete_entity_types(id):
    config = get_config()
    request = {
        "method": "delete",
        "object": "entity_types",
        "request": {
            "ETYPE_ID": id,
        }
    }
    return route(config, request)

# ----- data-source -----------------------------------------------------------


@app.route("/data-sources", methods=['POST'])
def http_post_data_source():
    config = get_config()
    request = {
        "method": "post",
        "object": "data_sources",
        "request": flask_request.json
    }
    return route(config, request)


@app.route("/data-sources/<id>", methods=['PUT'])
def http_put_data_source(id):
    config = get_config()
    request = {
        "DSRC_ID": id,
    }
    request.update(flask_request.json)
    request = {
        "method": "put",
        "object": "data_sources",
        "request": request
    }
    return route(config, request)


@app.route("/data-sources", methods=['GET'])
def http_get_data_sources():
    config = get_config()
    request = {
        "method": "get",
        "object": "data_sources"
    }
    return route(config, request)


@app.route("/data-sources/<id>", methods=['GET'])
def http_get_data_source(id):
    config = get_config()
    request = {
        "method": "get",
        "object": "data_source",
        "request": {
            "DSRC_ID": id,
        }
    }
    return route(config, request)


@app.route("/data-sources/<id>", methods=['DELETE'])
def http_delete_data_sources(id):
    config = get_config()
    request = {
        "method": "delete",
        "object": "data_sources",
        "request": {
            "DSRC_ID": id,
        }
    }
    return route(config, request)

# -----------------------------------------------------------------------------
# RabbitMQ helpers
# -----------------------------------------------------------------------------


def create_on_callback_function(config):
    ''' Use currying technique to create a rabbitmq callback function.'''

    def on_callback(channel, method, header, body):
        message = json.loads(body)
        route(config, message)

    return on_callback

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


def do_rabbitmq(config):
    '''Read from Kafka.'''

    # Perform common initialization tasks.

    common_prolog(config)

    # Get config parameters.

    rabbitmq_queue = config.get("rabbitmq_queue")
    rabbitmq_username = config.get("rabbitmq_username")
    rabbitmq_password = config.get("rabbitmq_password")
    rabbitmq_host = config.get("rabbitmq_host")

    # Create callback function.

    on_callback = create_on_callback_function(config)

    # Connect to RabbitMQ queue.

#     try:
    credentials = pika.PlainCredentials(rabbitmq_username, rabbitmq_password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue=rabbitmq_queue)
    channel.basic_qos(prefetch_count=10)
    channel.basic_consume(queue=rabbitmq_queue, on_message_callback=on_callback)
#     except pika.exceptions.AMQPConnectionError as err:
#         exit_error(418, err, rabbitmq_host)
#     except BaseException as err:
#         exit_error(417, err)

    # Start consuming.

    try:
        channel.start_consuming()
    except pika.exceptions.ChannelClosed:
        logging.info(message_info(130, threading.current_thread().name))

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
