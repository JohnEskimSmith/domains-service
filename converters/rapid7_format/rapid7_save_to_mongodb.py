from ujson import loads as ujson_loads
from typing import Dict, Tuple, List, Optional, Iterator, Union
from pathlib import Path
from multiprocessing import Pool, cpu_count, Value, Lock
import time
import argparse
from pymongo import MongoClient, WriteConcern, UpdateOne
from functools import lru_cache
from tld import get_tld
from ctypes import c_float
from math import ceil
from ipaddress import IPv4Address
from datetime import datetime
import pytz
import re

GLOBAL_UTC = pytz.timezone('UTC')
LOCAL_DIRECTORY = Path(__file__).parent.absolute()
CONST_LRU_CACHE = 100000
VALID_FQDN_REGEX = re.compile(r'(?=^.{4,253}$)(^((?!-)[*a-z0-9-_]{1,63}(?<!-)\.)+[a-z0-9-]{2,63}$)', re.IGNORECASE)


@lru_cache(maxsize=1000)
def validate_domain(domain: Optional[str]) -> bool:
    if domain:
        if VALID_FQDN_REGEX.match(domain):
            return True
    return False


@lru_cache(maxsize=CONST_LRU_CACHE)
def convert_ip(value: str) -> Optional[int]:
    try:
        ip = IPv4Address(value.strip())
        return int(ip)
    except:
        pass


@lru_cache(maxsize=CONST_LRU_CACHE)
def wrap_get_fld(domain: str) -> Optional[Tuple[str, str, str, str]]:
    try:
        _domain = get_tld(domain, fix_protocol=True, as_object=True)
        if _domain.domain and _domain.tld:
            return _domain.subdomain, _domain.domain, _domain.tld, domain
    except:
        pass


def create_updateone_operation(row: str, need_date: int) -> Tuple[bool, Union[str, UpdateOne]]:
    try:
        record: Dict = ujson_loads(row.lower())
    except:
        return False, row
    else:
        need_type: Optional[str] = record.get('type')
        if need_type == 'a':
            ip4: Optional[int] = convert_ip(record.get('value'))
            if ip4:
                raw_domain_record: Optional[str] = record.get('name')
                if validate_domain(raw_domain_record):
                    raw_domain_record = raw_domain_record.lower().strip()
                    _values_domain_record: Optional[Tuple] = wrap_get_fld(raw_domain_record)
                    if not _values_domain_record:
                        return False, row
                    sub, name, tld, _ = _values_domain_record
                    top = ''
                    if '.' in tld:
                        top = tld.split('.')[-1]
                    document: Dict = {}
                    if sub:
                        document['sub'] = sub
                    if top:
                        document['top'] = top
                    document['tld'] = tld
                    document['name'] = name
                    v = {'h': raw_domain_record, 'ip4': ip4}
                    events = [need_date]
                    document['v'] = v
                    update_dict = {'$addToSet': {'ds': {'$each': events}}, '$set': document}
                    operation = UpdateOne({'v': v}, update_dict, upsert=True)
                    return True, operation
        return False, row


def create_operations(lines: List[str], need_date: int) -> Tuple[List[str], List[UpdateOne]]:
    _operation_records = [create_updateone_operation(line, need_date) for line in lines]
    operation_records: List[UpdateOne] = [o[1] for o in _operation_records if o[0]]
    errors_records: List[str] = [o[1] for o in _operation_records if not o[0]]
    return errors_records, operation_records



def chunkify_file(file_name_raw: str, file_end: int, size: int) -> Iterator[Tuple[int, int]]:
    """ Return a new chunk """
    with open(file_name_raw, 'rb') as file:
        chunk_end = file.tell()
        while True:
            chunk_start = chunk_end
            file.seek(size, 1)
            file.readline()
            chunk_end = file.tell()
            if chunk_end > file_end:
                chunk_end = file_end
                yield chunk_start, chunk_end - chunk_start
                break
            else:
                yield chunk_start, chunk_end - chunk_start


def process_wrapper_write_to_mongodb(file_name_raw: str,
                                     chunk_start: int,
                                     chunk_size: int,
                                     chunk_part: int,
                                     conn_string: str,
                                     database_name: str,
                                     collection_name: str,
                                     timestamp_int: int,
                                     filename_stats: str) -> None:
    start = time.process_time()
    client = MongoClient(conn_string, serverSelectionTimeoutMS=60)
    db = client[database_name]
    col = db.get_collection(collection_name, write_concern=WriteConcern(w=0, j=False))
    with open(file_name_raw, 'rb') as file:
        file.seek(chunk_start)
        lines_bytes: bytes = file.read(chunk_size)
        try:
            _lines: str = lines_bytes.decode('utf-8')
            lines: List[str] = _lines.splitlines()
        except:
            pass
        else:
            if lines:
                try:
                    errors, bulk_records = create_operations(lines, timestamp_int)
                    if errors:
                        filename_errors = f'{chunk_start}_{chunk_size}.errors'
                        with open(filename_errors, 'wt') as file_errors:
                            file_errors.write('\n'.join(errors)+'\n')
                    col.bulk_write(bulk_records, ordered=False)
                    del lines
                    del bulk_records
                    with chunk_sync.get_lock():
                        chunk_sync.value += chunk_part
                        duration = round((time.process_time() - start), 3)
                        print(f'done: {ceil(round(chunk_sync.value, 3))} %, duration chunk: {duration}', flush=True)

                    # region save statistics
                    lock_write_file.acquire()
                    try:
                        with open(filename_stats, 'a') as f:
                            f.write(';'.join([str(file_name_raw),
                                              str(chunk_start),
                                              str(chunk_size),
                                              str(duration)])+'\n')
                    except:
                        print(f'errors with statistics: {chunk_start};{chunk_size}')
                    finally:
                        lock_write_file.release()
                    # endregion

                except Exception as e:
                    print('error status: ', e)


def init_globals(counter, locker):
    global chunk_sync
    global lock_write_file
    chunk_sync = counter
    lock_write_file = locker


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='recreate Sonar dataset FDNS')

    parser.add_argument(
        "-f",
        "--input-file",
        dest='input_file',
        required=True,
        type=str,
        help="full path to internal format file")

    parser.add_argument(
        "--size",
        dest='chunk_size',
        type=str,
        default='10M',
        help="size of chank, default set 10M")

    parser.add_argument(
        "--connection",
        dest='connection_string',
        required=True,
        type=str,
        help="conection string MongoDB (ex.: mongodb://localhost:27017), default: mongodb://localhost:27017")

    parser.add_argument(
        "--db",
        dest='dbname',
        required=True,
        type=str,
        help="MongoDB database name")

    parser.add_argument(
        "--collection",
        dest='collection',
        required=True,
        type=str,
        help="MongoDB collection name")

    parser.add_argument(
        "--need-date",
        dest='need_date',
        required=True,
        type=str,
        help="required, date of upload, ex.: 2021-12-19")

    parser.add_argument(
        "--processes",
        dest='processes',
        type=int,
        help="cpu count, not required")

    parser.add_argument(
        "--stats",
        dest='filename_stats',
        type=str,
        default='statistics.csv',
        help="filename for out statistics, default: statistics.csv")


    args = parser.parse_args()

    if not args.processes:
        count_cpu = cpu_count()
    else:
        count_cpu = args.processes

    default_size = None
    _keys = {'k': 1024,
             'm': 1024 * 1024,
             'g': 1024 * 1024 * 1024,
             'b': 1}
    for k, value in _keys.items():
        try:
            s, k = args.chunk_size.lower().split(k)
            default_size = int(s) * value
        except:
            pass
    if not default_size:
        _check = any([k in args.chunk_size.lower() for k in _keys])
        if not _check:
            try:
                default_size = int(args.chunk_size) * 1024 * 1024
            except:
                pass
    if not default_size:
        default_size = 1024 * 1024 * 10

    try:
        input_file = Path(args.input_file)
        if not input_file.is_file():
            input_file = LOCAL_DIRECTORY / args.input_file
            if not input_file.is_file():
                print("where is input file?")
                exit(1)
    except Exception as e:
        print('errors:', e)
        exit(1)

    try:
        input_file_end = input_file.stat().st_size
    except Exception as e:
        print('errors:', e)
        exit(1)

    conn_string = args.connection_string
    mongodb_database = args.dbname
    mongodb_collection = args.collection

    need_date = args.need_date
    try:
        _timestamp = datetime.strptime(need_date, '%Y-%m-%d')
        _timestamp_aware = GLOBAL_UTC.localize(_timestamp)
        timestamp_int = int(_timestamp_aware.timestamp())

    except Exception as e:
        print(f'date:{need_date}, errors:', e)
        exit(1)

    jobs = []

    chunk_sync = Value(c_float, 0)
    lock_write_file = Lock()

    pool = Pool(processes=count_cpu, initializer=init_globals, initargs=(chunk_sync, lock_write_file, ))
    i = 0
    filename_stats = Path(args.filename_stats)
    if not filename_stats.is_file():
        filename_stats = LOCAL_DIRECTORY / args.filename_stats
        if not filename_stats.is_file():
            with filename_stats.open('a') as f:
                f.write(';'.join(['filename', 'start', 'size', 'duration']) + '\n')

    chunks = [(chunk_start, chunk_size)
              for chunk_start, chunk_size in chunkify_file(input_file, file_end=input_file_end, size=default_size)]
    all_chunks = len(chunks)
    chunk_part = round(100/all_chunks, 4)

    for chunk_start, chunk_size in chunks:
        jobs.append(pool.apply_async(process_wrapper_write_to_mongodb,
                                     (input_file,
                                      chunk_start,
                                      chunk_size,
                                      chunk_part,
                                      conn_string,
                                      mongodb_database,
                                      mongodb_collection,
                                      timestamp_int,
                                      str(filename_stats),
                                      )))

    # wait for all jobs to finish
    for job in jobs:
        job.get()

    # clean up
    pool.close()
    pool.join()
