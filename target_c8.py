#!/usr/bin/env python3

import argparse
import io
import jsonschema
import simplejson as json
import sys
from c8 import C8Client

import singer
from jsonschema import Draft4Validator
from adjust_precision_for_schema import adjust_decimal_precision_for_schema

logger = singer.get_logger()

def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

def persist_messages(messages, target):
    state = None
    schemas = {}
    key_properties = {}
    validators = {}
    collections = []

    if target != None and target:
        if not client.has_collection(target):
            client.create_collection(name=target)
    else:
        for c in client.get_collections():
            if not c['system']:
                collections.append(c['name'])

    for message in messages:
        try:
            o = singer.parse_message(message).asdict()
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(message))
            raise

        message_type = o['type']
        if message_type == 'RECORD':
            stream = o['stream']
            if stream not in schemas:
                raise Exception(
                    "A record for stream {}"
                    "was encountered before a corresponding schema".format(stream)
                )

            try: 
                validators[stream].validate((o['record']))
            except jsonschema.ValidationError as e:
                logger.error(f"Failed parsing the json schema for stream: {stream}.")
                raise e

            if target == None or not target:
                if stream not in collections:
                    client.create_collection(name=stream)
                    collections.append(stream)
                    # Get Collecion Handle and Insert
                    coll = client.get_collection(stream)
            else:
                coll = client.get_collection(target)

            logger.info('Writing a record')
            try:
                coll.insert(o['record'])
            except TypeError as e:
                # TODO: This is temporary until json serializing issue for Decimals are fixed in pyC8
                logger.debug("pyC8 error occurred")

            state = None
        elif message_type == 'STATE':
            logger.debug('Setting state to {}'.format(o['value']))
            state = o['value']
        elif message_type == 'SCHEMA':
            stream = o['stream']
            schemas[stream] = o['schema']
            adjust_decimal_precision_for_schema(schemas[stream])
            validators[stream] = Draft4Validator((o['schema']))
            key_properties[stream] = o['key_properties']
        else:
            logger.warning("Unknown message type {} in message {}".format(o['type'], o))

    return state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input_json:
            config = json.load(input_json)
    else:
        raise Exception(
            "Required '--config' parameter was not provided"
        )
    region = config['region']
    tenant = config['tenant']
    fabric = config['fabric']
    password = config['password']
    target_collection = config['target_collection']

    print("Create C8Client Connection")
    global client
    client = C8Client(
        protocol='https',
        host=region,
        port=443,
        email=tenant,
        password=password,
        geofabric=fabric
    )

    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_messages(input_messages, target_collection)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
