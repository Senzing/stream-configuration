#! /usr/bin/env python3

import json

try:
    from G2Config import G2Config
    from G2ConfigMgr import G2ConfigMgr
    import G2Exception
except ImportError:
    pass

def get_g2_configuration_dictionary():
    result = {
        "PIPELINE": {
            "SUPPORTPATH": "/opt/senzing/g2/data",
            "CONFIGPATH": "/opt/senzing/g2/data"
        },
        "SQL": {
            "CONNECTION": "sqlite3://na:na@/opt/senzing/g2/sqldb/G2C.db",
        }
    }
    return result


def get_g2_configuration_json():
    return json.dumps(get_g2_configuration_dictionary())


def get_g2_config(g2_config_name="loader-G2-config"):
    '''Get the G2Config resource.'''
    try:
        g2_configuration_json = get_g2_configuration_json()
        result = G2Config()
        result.initV2(g2_config_name, g2_configuration_json, True)
    except G2Exception.G2ModuleException as err:
        exit_error(505, g2_configuration_json, err)
    return result



def get_g2_configuration_manager(g2_configuration_manager_name="loader-G2-configuration-manager"):
    '''Get the G2Config resource.'''
    try:
        g2_configuration_json = get_g2_configuration_json()
        result = G2ConfigMgr()
        result.initV2(g2_configuration_manager_name, g2_configuration_json, True)
    except G2Exception.G2ModuleException as err:
        exit_error(516, g2_configuration_json, err)
    return result

### Main

default_config_id = bytearray()
g2_configuration_manager = get_g2_configuration_manager()
return_code = g2_configuration_manager.getDefaultConfigID(default_config_id)

# Assuming if return_code != 0

print("{0} {1} {2}".format(type(default_config_id), default_config_id, default_config_id.decode()))

if default_config_id:
    print("Default config id found. Done")
    exit()

# Assume default_config_id == 0

response = bytearray()
g2_config = get_g2_config()
config_handle = g2_config.create()
g2_config.save(config_handle, response)

# xxx

return_code = g2_configuration_manager.addConfig(response.decode(), "Initial configuration.", default_config_id)
g2_configuration_manager.setDefaultConfigID(default_config_id)

print("Finish")
