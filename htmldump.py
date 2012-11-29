#!/usr/bin/env python

import argparse
import cStringIO
import os
import sys
import tarfile
import time
import zlib

import pymongo


def zip_dumps(collection, docs_re, output_file):
    now_ts = time.time()
    with tarfile.open(output_file, 'w:bz2') as archive:
        spec = {}
        if docs_re is not None:
            spec['doc_id'] = {'$regex': docs_re}
        for doc in collection.find(spec):
            sn, year = doc['doc_id'].split('-')
            filename = os.path.join(year, sn, "%s.html" % doc['tab'])
            html = zlib.decompress(doc['zhtml'])
            info = tarfile.TarInfo(filename)
            info.size = len(html)
            info.mtime = now_ts
            info.uname = info.gname = 'root'
            archive.addfile(info, cStringIO.StringIO(html))


def parse_args(argv):
    parser = argparse.ArgumentParser(
            description='Dump TED documents from MongoDB to a ZIP file.')
    parser.add_argument('-H', '--host', default='localhost',
            help='the hostname of the MongoDB server (default: localhost)')
    parser.add_argument('-P', '--port', default=27017, type=int,
            help='the server port (default: 27017)')
    parser.add_argument('-d', '--database', default='opented',
            help='the database name (default: opented)')
    parser.add_argument('-c', '--collection', default='dumps',
            help='the HTML dumps collection name (default: dumps)')
    parser.add_argument('-u', '--username',
            help='the username used to authenticate to the database')
    parser.add_argument('-p', '--password',
            help='the authentication password')
    parser.add_argument('output', metavar='OUTPUT',
            help='the output bzip2 compressed tar archive file')
    parser.add_argument('docs_re', metavar='DOC-RE', nargs='?',
            help='a regular expression filtering document ids (default: all)')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args(sys.argv)
    connection = pymongo.Connection(host=args.host, port=args.port)
    db = connection.opented
    if args.username is not None:
        db.authenticate(args.username, args.password)
    collection = db.dumps
    zip_dumps(collection, args.docs_re, args.output)
