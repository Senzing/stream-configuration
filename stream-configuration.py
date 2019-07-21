#! /usr/bin/env python3

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
import string
import sys
import time
import confluent_kafka
import pika

from urllib.parse import urlparse, urlunparse, unquote
from urllib.request import urlopen

# Import Senzing libraries.

try:
    from G2Config import G2Config
    from G2ConfigMgr import G2ConfigMgr
    from G2Database import G2Database
    from G2Engine import G2Engine
    import G2Exception
except ImportError:
    pass

from flask import Flask
from flask import json
from flask import request as flask_request
from flask import url_for

app = Flask(__name__)

__all__ = []
__version__ = "1.0.0"  # See https://www.python.org/dev/peps/pep-0396/
__date__ = '2019-05-23'
__updated__ = '2019-07-21'

SENZING_PRODUCT_ID = "5004"  # See https://github.com/Senzing/knowledge-base/blob/master/lists/senzing-product-ids.md
log_format = '%(asctime)s %(message)s'

# Working with bytes.

KILOBYTES = 1024
MEGABYTES = 1024 * KILOBYTES
GIGABYTES = 1024 * MEGABYTES

# Lists from https://www.ietf.org/rfc/rfc1738.txt

safe_character_list = ['$', '-', '_', '.', '+', '!', '*', '(', ')', ',', '"' ] + list(string.ascii_letters)
unsafe_character_list = [ '"', '<', '>', '#', '%', '{', '}', '|', '\\', '^', '~', '[', ']', '`']
reserved_character_list = [ ';', ',', '/', '?', ':', '@', '=', '&']

# The "configuration_locator" describes where configuration variables are in:
# 1) Command line options, 2) Environment variables, 3) Configuration files, 4) Default values

config = {}
configuration_locator = {
    "config_path": {
        "default": "/opt/senzing/g2/data",
        "env": "SENZING_CONFIG_PATH",
        "cli": "config-path"
    },
    "debug": {
        "default": False,
        "env": "SENZING_DEBUG",
        "cli": "debug"
    },
    "g2_database_url_generic": {
        "default": "sqlite3://na:na@/opt/senzing/g2/sqldb/G2C.db",
        "env": "SENZING_DATABASE_URL",
        "cli": "database-url"
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
    "sleep_time_in_seconds": {
        "default": 0,
        "env": "SENZING_SLEEP_TIME_IN_SECONDS",
        "cli": "sleep-time-in-seconds"
    },
    "subcommand": {
        "default": None,
        "env": "SENZING_SUBCOMMAND",
    },
    "support_path": {
        "default": "/opt/senzing/g2/data",
        "env": "SENZING_SUPPORT_PATH",
        "cli": "support-path"
    }
}

# Enumerate keys in 'configuration_locator' that should not be printed to the log.

keys_to_redact = [
    ]

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

    subparser_8 = subparsers.add_parser('version', help='Print the version of senzing-package.py.')

    subparser_9 = subparsers.add_parser('sleep', help='Do nothing but sleep. For Docker testing.')
    subparser_9.add_argument("--sleep-time-in-seconds", dest="sleep_time_in_seconds", metavar="SENZING_SLEEP_TIME_IN_SECONDS", help="Sleep time in seconds. DEFAULT: 0 (infinite)")

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
MESSAGE_WARN = 300
MESSAGE_ERROR = 700
MESSAGE_DEBUG = 900

message_dictionary = {
    "100": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}I",
    "110": "Successfully added {table_name}.{id}: {id_value}",
    "111": "Successfully deleted {table_name}.{id}: {id_value}",
    "112": "Successfully updated {table_name}.{id}: {id_value}",
    "202": "Non-fatal exception on Line {0}: {1} Error: {2}",
    "292": "Configuration change detected.  Old: {0} New: {1}",
    "293": "For information on warnings and errors, see https://github.com/Senzing/stream-loader#errors",
    "294": "Version: {0}  Updated: {1}",
    "295": "Sleeping infinitely.",
    "296": "Sleeping {0} seconds.",
    "297": "Enter {0}",
    "298": "Exit {0}",
    "299": "{0}",
    "300": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}W",
    "499": "{0}",
    "500": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}E",
    "501": "Error: {0} for {1}",
    "502": "Could not connect to database. URL: {0} Error type: {1} Error: {2}",
    "695": "Unknown database scheme '{0}' in database url '{1}'",
    "696": "Bad SENZING_SUBCOMMAND: {0}.",
    "697": "No processing done.",
    "698": "Program terminated with error.",
    "699": "{0}",
    "700": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}E",
    "886": "G2Engine.addRecord() bad return code: {0}; JSON: {1}",
    "887": "G2Engine.addRecord() TranslateG2ModuleException: {0}; JSON: {1}",
    "888": "G2Engine.addRecord() G2ModuleNotInitialized: {0}; JSON: {1}",
    "889": "G2Engine.addRecord() G2ModuleGenericException: {0}; JSON: {1}",
    "890": "G2Engine.addRecord() Exception: {0}; JSON: {1}",
    "891": "Original and new database URLs do not match. Original URL: {0}; Reconstructed URL: {1}",
    "892": "Could not initialize G2Product with '{0}'. Error: {1}",
    "893": "Could not initialize G2Hasher with '{0}'. Error: {1}",
    "894": "Could not initialize G2Diagnostic with '{0}'. Error: {1}",
    "895": "Could not initialize G2Audit with '{0}'. Error: {1}",
    "896": "Could not initialize G2ConfigMgr with '{0}'. Error: {1}",
    "897": "Could not initialize G2Config with '{0}'. Error: {1}",
    "898": "Could not initialize G2Engine with '{0}'. Error: {1}",
    "899": "{0}",
    "900": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}D",
    "901": "Execute SQL: {0}",
    "999": "{0}",
}


def message(index, *args):
    index_string = str(index)
    template = message_dictionary.get(index_string, "No message for index {0}.".format(index_string))
    return template.format(*args)


def message_generic(generic_index, index, *args):
    index_string = str(index)
    return "{0} {1}".format(message(generic_index, index), message(index, *args))


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
# Database URL parsing
# -----------------------------------------------------------------------------


def translate(map, astring):
    new_string = str(astring)
    for key, value in map.items():
        new_string = new_string.replace(key, value)
    return new_string


def get_unsafe_characters(astring):
    result = []
    for unsafe_character in unsafe_character_list:
        if unsafe_character in astring:
            result.append(unsafe_character)
    return result


def get_safe_characters(astring):
    result = []
    for safe_character in safe_character_list:
        if safe_character not in astring:
            result.append(safe_character)
    return result


def parse_database_url(original_senzing_database_url):

    result = {}

    # Get the value of SENZING_DATABASE_URL environment variable.

    senzing_database_url = original_senzing_database_url

    # Create lists of safe and unsafe characters.

    unsafe_characters = get_unsafe_characters(senzing_database_url)
    safe_characters = get_safe_characters(senzing_database_url)

    # Detect an error condition where there are not enough safe characters.

    if len(unsafe_characters) > len(safe_characters):
        logging.error(message_error(730, unsafe_characters, safe_characters))
        return result

    # Perform translation.
    # This makes a map of safe character mapping to unsafe characters.
    # "senzing_database_url" is modified to have only safe characters.

    translation_map = {}
    safe_characters_index = 0
    for unsafe_character in unsafe_characters:
        safe_character = safe_characters[safe_characters_index]
        safe_characters_index += 1
        translation_map[safe_character] = unsafe_character
        senzing_database_url = senzing_database_url.replace(unsafe_character, safe_character)

    # Parse "translated" URL.

    parsed = urlparse(senzing_database_url)
    schema = parsed.path.strip('/')

    # Construct result.

    result = {
        'scheme': translate(translation_map, parsed.scheme),
        'netloc': translate(translation_map, parsed.netloc),
        'path': translate(translation_map, parsed.path),
        'params': translate(translation_map, parsed.params),
        'query': translate(translation_map, parsed.query),
        'fragment': translate(translation_map, parsed.fragment),
        'username': translate(translation_map, parsed.username),
        'password': translate(translation_map, parsed.password),
        'hostname': translate(translation_map, parsed.hostname),
        'port': translate(translation_map, parsed.port),
        'schema': translate(translation_map, schema),
    }

    # For safety, compare original URL with reconstructed URL.

    url_parts = [
        result.get('scheme'),
        result.get('netloc'),
        result.get('path'),
        result.get('params'),
        result.get('query'),
        result.get('fragment'),
    ]
    test_senzing_database_url = urlunparse(url_parts)
    if test_senzing_database_url != original_senzing_database_url:
        logging.warning(message_warning(891, original_senzing_database_url, test_senzing_database_url))

    # Return result.

    return result

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------


def get_g2_database_url_specific(generic_database_url):
    result = ""

    parsed_database_url = parse_database_url(generic_database_url)
    scheme = parsed_database_url.get('scheme')

    if scheme in ['mysql']:
        result = "{scheme}://{username}:{password}@{hostname}:{port}/?schema={schema}".format(**parsed_database_url)
    elif scheme in ['postgresql']:
        result = "{scheme}://{username}:{password}@{hostname}:{port}:{schema}/".format(**parsed_database_url)
    elif scheme in ['db2']:
        result = "{scheme}://{username}:{password}@{schema}".format(**parsed_database_url)
    elif scheme in ['sqlite3']:
        result = "{scheme}://{netloc}{path}".format(**parsed_database_url)
    else:
        logging.error(message_error(695, scheme, generic_database_url))

    return result



def get_configuration(args):
    ''' Order of precedence: CLI, OS environment variables, INI file, default. '''
    result = {}

    # Copy default values into configuration dictionary.

    for key, value in list(configuration_locator.items()):
        result[key] = value.get('default', None)

    # "Prime the pump" with command line args. This will be done again as the last step.

    for key, value in list(args.__dict__.items()):
        new_key = key.format(subcommand.replace('-', '_'))
        if value:
            result[new_key] = value

    # Copy OS environment variables into configuration dictionary.

    for key, value in list(configuration_locator.items()):
        os_env_var = value.get('env', None)
        if os_env_var:
            os_env_value = os.getenv(os_env_var, None)
            if os_env_value:
                result[key] = os_env_value

    # Copy 'args' into configuration dictionary.

    for key, value in list(args.__dict__.items()):
        new_key = key.format(subcommand.replace('-', '_'))
        if value:
            result[new_key] = value

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

    # Special case:  Tailored database URL

    result['g2_database_url_specific'] = get_g2_database_url_specific(result.get("g2_database_url_generic"))

    return result


def validate_configuration(config):
    ''' Check aggregate configuration from commandline options, environment variables, config files, and defaults. '''

    user_warning_messages = []
    user_error_messages = []

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
        logging.warning(user_warning_message)

    # Log error messages.

    for user_error_message in user_error_messages:
        logging.error(user_error_message)

    # Log where to go for help.

    if len(user_warning_messages) > 0 or len(user_error_messages) > 0:
        logging.info(message_info(293))

    # If there are error messages, exit.

    if len(user_error_messages) > 0:
        exit_error(597)


def redact_configuration(config):
    ''' Return a shallow copy of config with certain keys removed. '''
    result = config.copy()
    for key in keys_to_redact:
        result.pop(key)
    return result

# -----------------------------------------------------------------------------
# Utility functions
# -----------------------------------------------------------------------------


def create_signal_handler_function(args):
    ''' Tricky code.  Uses currying technique. Create a function for signal handling.
        that knows about "args".
    '''

    def result_function(signal_number, frame):
        logging.info(message_info(298, args))
        sys.exit(0)

    return result_function


def bootstrap_signal_handler(signal, frame):
    sys.exit(0)


def entry_template(config):
    ''' Format of entry message. '''
    debug = config.get("debug", False)
    config['start_time'] = time.time()
    if debug:
        final_config = config
    else:
        final_config = redact_configuration(config)
    config_json = json.dumps(final_config, sort_keys=True)
    return message_info(297, config_json)


def exit_template(config):
    ''' Format of exit message. '''
    debug = config.get("debug", False)
    stop_time = time.time()
    config['stop_time'] = stop_time
    config['elapsed_time'] = stop_time - config.get('start_time', stop_time)
    if debug:
        final_config = config
    else:
        final_config = redact_configuration(config)
    config_json = json.dumps(final_config, sort_keys=True)
    return message_info(298, config_json)


def exit_error(index, *args):
    ''' Log error message and exit program. '''
    logging.error(message_error(index, *args))
    logging.error(message_error(598))
    sys.exit(1)


def exit_silently():
    ''' Exit program. '''
    sys.exit(1)

# -----------------------------------------------------------------------------
# Utility functions
# -----------------------------------------------------------------------------

def get_config():
    return config

def common_prolog(config):
    '''Common steps for most do_* functions.'''
    validate_configuration(config)
    logging.info(entry_template(config))


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
# Senzing services.
# -----------------------------------------------------------------------------


def get_g2_configuration_dictionary(config):
    result = {
        "PIPELINE": {
            "SUPPORTPATH": config.get("support_path"),
            "CONFIGPATH": config.get("config_path")
        },
        "SQL": {
            "CONNECTION": config.get("g2_database_url_specific"),
        }
    }
    return result


def get_g2_configuration_json(config):
    return json.dumps(get_g2_configuration_dictionary(config))


def get_g2_config(config, g2_config_name="loader-G2-config"):
    '''Get the G2Config resource.'''
    try:
        g2_configuration_json = get_g2_configuration_json(config)
        result = G2Config()
        result.initV2(g2_config_name, g2_configuration_json, config.get('debug', False))
    except G2Exception.G2ModuleException as err:
        exit_error(897, g2_configuration_json, err)
    return result


def get_g2_configuration_manager(config, g2_configuration_manager_name="loader-G2-configuration-manager"):
    '''Get the G2Config resource.'''
    try:
        g2_configuration_json = get_g2_configuration_json(config)
        result = G2ConfigMgr()
        result.initV2(g2_configuration_manager_name, g2_configuration_json, config.get('debug', False))
    except G2Exception.G2ModuleException as err:
        exit_error(896, g2_configuration_json, err)
    return result


def get_g2_diagnostic(config, g2_diagnostic_name="loader-G2-diagnostic"):
    '''Get the G2Diagnostic resource.'''
    try:
        g2_configuration_json = get_g2_configuration_json(config)
        result = G2Diagnostic()
        result.initV2(g2_diagnostic_name, g2_configuration_json, config.get('debug', False))
    except G2Exception.G2ModuleException as err:
        exit_error(894, g2_configuration_json, err)
    return result


def get_g2_engine(config, g2_engine_name="loader-G2-engine"):
    '''Get the G2Engine resource.'''
    try:
        g2_configuration_json = get_g2_configuration_json(config)
        result = G2Engine()
        result.initV2(g2_engine_name, g2_configuration_json, config.get('debug', False))
        config['last_configuration_check'] = time.time()
    except G2Exception.G2ModuleException as err:
        exit_error(898, g2_configuration_json, err)
    return result


def get_g2_product(config, g2_product_name="loader-G2-product"):
    '''Get the G2Product resource.'''
    try:
        g2_configuration_json = get_g2_configuration_json(config)
        result = G2Product()
        result.initV2(g2_product_name, g2_configuration_json, config.get('debug'))
    except G2Exception.G2ModuleException as err:
        exit_error(892, config.get('g2project_ini'), err)
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

    g2_database_url = config.get('g2_database_url_specific')
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


def get_routes():
    result = {
        "links": {
            "self": "/"
        },
        "data": {}
    }
    for rule in app.url_map.iter_rules():

        methods = ', '.join(rule.methods)

        options = {}
        for arg in rule.arguments:
            options[arg] = "[{0}]".format(arg)
        url = url_for(rule.endpoint, **options)

        result["data"][rule.endpoint] = {
            "methods": methods,
            "url": unquote(url)
            }

    return result

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

# ----- root ------------------------------------------------------------------


def handle_get_root(config, request):
    return get_routes()

# ----- data-source -----------------------------------------------------------


def handle_post_data_sources(config, request):

    # Get Senzing G2 resources

    g2_engine = get_g2_engine(config)
    g2_config = get_g2_config(config)
    g2_configuration_manager = get_g2_configuration_manager(config)

    config_handle = g2_config.create()

    active_config_id = bytearray()
    g2_engine.getActiveConfigID(active_config_id)

    active_config_json = bytearray()

    # FIXME:  Hack to work around Python API inconsistency.

    active_config_id_int = int(active_config_id)

    g2_configuration_manager.getConfig(active_config_id_int, active_config_json)

    print("MJD: {0}".format(type(active_config_json)))

    active_config_handle = g2_config.load(active_config_json.decode())
    g2_config.addDataSource(config_handle, "BOB")

    new_config_json = bytearray()
    return_code = g2_config.save(config_handle, new_config_json)

    new_configuration_comments = "Add Bob"
    new_config_id  = bytearray()

    g2_configuration_manager.addConfig(new_config_json.decode(), new_configuration_comments, new_config_id)


#     return handle_post(config, request, table_metadata)
    return {}


def handle_put_data_sources(config, request):
    return handle_put(config, request, get_table_metadata_cfg_dsrc())


def handle_get_data_sources(config, request):
    g2_config = get_g2_config(config)
    config_handle = g2_config.create()
    datasources_bytearray = bytearray()
    method = "g2_config.listDataSources"
    parameters = "{0}, {1}".format(config_handle, datasources_bytearray.decode())
    try:
        return_code = g2_config.listDataSources(config_handle, datasources_bytearray)
    except G2Exception.TranslateG2ModuleException as err:
        logging.error(message_error(750, err, method, parameters))
    except G2Exception.G2ModuleNotInitialized as err:
        logging.error(message_error(751, err, method, parameters))
    except Exception as err:
        logging.error(message_error(752, err, method, parameters))
    except:
        logging.error(message_error(753, method, parameters))
    if return_code != 0:
        exit_error(754, return_code, method, parameters)

    result = json.loads(datasources_bytearray.decode())
    return result


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
        return json.dumps(bad_method(message_dictionary, method, methods), sort_keys=True)

    # Create a function name.

    route_function_name = "handle_{0}_{1}".format(method.replace('-', '_'), object.replace('-', '_'),)

    # Test to see if function exists in the code.

    if route_function_name not in globals():
        return json.dumps(bad_function(message_dictionary, object), sort_keys=True)

    # Tricky code for calling function based on string.

    result = globals()[route_function_name](config, request)
    return json.dumps(result, sort_keys=True)

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


@app.route("/", methods=['GET'])
def http_get_root():
    config = get_config()
    request = {
        "method": "get",
        "object": "root",
        "request": {
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


def do_docker_acceptance_test(args):
    ''' For use with Docker acceptance testing. '''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)

    # Prolog.

    logging.info(entry_template(config))

    # Epilog.

    logging.info(exit_template(config))



def do_service(args):
    '''Read from URL-addressable file.'''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)

    common_prolog(config)
    host = config.get('host')
    port = config.get('port')
    debug = config.get('debug')

    app.run(host=host, port=port, debug=debug)

    # Epilog.

    logging.info(exit_template(config))


def do_sleep(args):
    ''' Sleep.  Used for debugging. '''

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)

    # Prolog.

    logging.info(entry_template(config))

    # Pull values from configuration.

    sleep_time_in_seconds = config.get('sleep_time_in_seconds')

    # Sleep

    if sleep_time_in_seconds > 0:
        logging.info(message_info(296, sleep_time_in_seconds))
        time.sleep(sleep_time_in_seconds)

    else:
        sleep_time_in_seconds = 3600
        while True:
            logging.info(message_info(295))
            time.sleep(sleep_time_in_seconds)

    # Epilog.

    logging.info(exit_template(config))


def do_version(args):
    ''' Log version information. '''

    logging.info(message_info(294, __version__, __updated__))

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

    # Catch interrupts. Tricky code: Uses currying.

    signal_handler = create_signal_handler_function(args)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Set global config for use by Flask.

    config = get_configuration(args)

    # Transform subcommand from CLI parameter to function name string.

    subcommand_function_name = "do_{0}".format(subcommand.replace('-', '_'))

    # Test to see if function exists in the code.

    if subcommand_function_name not in globals():
        logging.warning(message_warning(596, subcommand))
        parser.print_help()
        exit_silently()

    # Tricky code for calling function based on string.

    globals()[subcommand_function_name](args)
