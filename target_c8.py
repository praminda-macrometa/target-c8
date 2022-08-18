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
fabric = "_system"

def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

def persist_messages(
    messages
):
    state = None
    schemas = {}
    key_properties = {}
    validators = {}
    collections = []

    for c in client.get_collections():
        collections.append(c['name'])

    for message in messages:
        try:
            o = singer.parse_message(message).asdict()
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(message))
            raise

        message_type = o['type']
        if message_type == 'RECORD':
            if o['stream'] not in schemas:
                raise Exception(
                    "A record for stream {}"
                    "was encountered before a corresponding schema".format(o['stream'])
                )

            try: 
                validators[o['stream']].validate((o['record']))
            except jsonschema.ValidationError as e:
                logger.error(f"Failed parsing the json schema for stream: {o['stream']}.")
                raise e

            collname = o['stream']
            if collname not in collections:
                client.create_collection(name=collname)
                collections.append(collname)

            # Get Collecion Handle and Insert
            coll = client.get_collection(collname)
            print('Writing a record')
            try:
                coll.insert(o['record'])
            except TypeError as e:
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
    parser.add_argument('-r', '--region', help='Region ex: foo-eu-west.eng.macrometa.io')
    parser.add_argument('-t', '--tenant', help='Tenant ex: foo-eu-west.eng.macrometa.io')
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()
    
    if args.region:
        region = args.region
    else:
        region = "praminda-ap-west.eng.macrometa.io"

    if args.tenant:
        tenant = args.tenant
    else:
        tenant = "demo@macrometa.io"

    print("Create C8Client Connection...")
    global client
    client = C8Client(protocol='https', host=region, port=443, email=tenant, password='demo', geofabric=fabric)

    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_messages(
        input_messages
    )

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
