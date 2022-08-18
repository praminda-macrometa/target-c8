#!/usr/bin/env python3

import argparse
import io
import jsonschema
import simplejson as json
import os
import sys
from datetime import datetime
from pathlib import Path
from c8 import C8Client

import singer
from jsonschema import Draft4Validator, FormatChecker
from adjust_precision_for_schema import adjust_decimal_precision_for_schema

logger = singer.get_logger()

fabric = "_system"
collname = "employees"


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

    if client.has_collection(collname):
        print("Collection exists")
    else:
        client.create_collection(name=collname)
        print("Collection created")


    for message in messages:
        print("Processing msg: " + message)
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
            
            # Get Collecion Handle and Insert
            coll = client.get_collection(collname)
            print('Writing record: ', o['record']['timestamp'], ' in c8')
            coll.insert(o['record'])

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
