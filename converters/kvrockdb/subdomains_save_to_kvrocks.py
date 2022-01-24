from ujson import loads as ujson_loads
from typing import Dict, Tuple, List, Optional, Iterator, Union, Set
from pathlib import Path
from multiprocessing import Pool, cpu_count, Value, Lock
import argparse
import redis
from functools import lru_cache
from tld import get_tld, get_fld
from ctypes import c_float
from math import ceil
from ipaddress import IPv4Address
import pytz
import re
import hashlib
import time
import yaml

GLOBAL_UTC = pytz.timezone('UTC')
LOCAL_DIRECTORY = Path(__file__).parent.absolute()
CONST_LRU_CACHE = 100000
VALID_FQDN_REGEX = re.compile(r'(?=^.{4,253}$)(^((?!-)[*a-z0-9-_]{1,63}(?<!-)\.)+[a-z0-9-]{2,63}$)', re.IGNORECASE)


def load_config(fname):
    with open(fname, 'rt') as f:
        data = yaml.load(f, Loader=yaml.Loader)
        return data


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
def wrap_hashing(value: str, hash_div: int) -> int:
    hex_hash_string = hashlib.md5(value.encode())
    return int.from_bytes(hex_hash_string.digest(), 'big') % hash_div


@lru_cache(maxsize=CONST_LRU_CACHE)
def wrap_get_tld(domain) -> Optional[Tuple[str, str, str, str]]:
    try:
        _domain = get_tld(domain, fix_protocol=True, as_object=True)
        value_domain = get_fld(domain, fix_protocol=True)
        return _domain.subdomain, _domain.domain, _domain.tld, value_domain
    except:
        pass


@lru_cache(maxsize=CONST_LRU_CACHE)
def wrap_get_hash_fld_tld(domain: str, hash_div: int) -> Optional[Tuple[str, str, str, str, int, str]]:
    _domain = wrap_get_tld(domain)
    if _domain:
        sub, name, tld, standart_domain = _domain
        _value = '.'.join([sub, name, tld])
        hashed_value = wrap_hashing(_value, hash_div)
        return sub, name, tld, standart_domain, hashed_value, domain


def create_dict_operation(row: str, schema_key: str, hash_div: int) -> Tuple[bool, Union[str, Tuple[str, str]]]:
    try:
        record: Dict = ujson_loads(row.lower())
    except:
        return False, row
    else:
        need_type: Optional[str] = record.get('type')
        if need_type == 'a':
            raw_domain_record: Optional[str] = record.get('name')
            if validate_domain(raw_domain_record):
                raw_domain_record = raw_domain_record.lower().strip()
                _values_domain_record: Optional[Tuple] = wrap_get_hash_fld_tld(raw_domain_record, hash_div)
                if not _values_domain_record:
                    return False, row
                sub, name, tld, standart_domain, hash_hostname, _ = _values_domain_record
                if not sub:
                    sub = '***'
                return True, ('|'.join([schema_key, tld, name, str(hash_hostname)]), sub)
        return False, row


def create_operations(lines: List[str], schema_key: str, hash_div: int, number_of_servers: int) -> Tuple[List[str], Dict]:
    _record_values = [create_dict_operation(line, schema_key, hash_div) for line in lines]
    errors_records = [_value[1] for _value in _record_values if not _value[0]]
    record_values = (_value[1] for _value in _record_values if _value[0])
    values = dict()
    result = dict()
    for key, sub in record_values:
        if key not in values:
            values[key] = [sub]
        else:
            values[key].append(sub)
    if values:
        for k in values.keys():
            values[k] = set(values[k])
        result = {n: {} for n in range(number_of_servers)}
        for key in values.keys():
            # region function get part key
            _part_key = '|'.join(key.split('|')[1:-1])
            index_server = wrap_hashing(_part_key, number_of_servers)
            # endregion
            result[index_server][key] = values[key]
    return errors_records, result


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



def init_globals(counter, locker):
    global chunk_sync
    global lock_write_file
    chunk_sync = counter
    lock_write_file = locker


def prepare_config(config: Dict, type_load: str) -> Dict:
    settings = dict()
    try:
        _kvrocks_servers = config['partitions_servers']
        settings['servers'] = {}
        for number_server, value in _kvrocks_servers.items():
            remainder_of_division = int(number_server.split('_')[1])
            settings['servers'][remainder_of_division] = {'host': value['host'],
                                                          'port': int(value['port'])}
        settings['schema_key'] = config['schema_keys'][type_load]
        settings['const_hash'] = config['const_hashs'][type_load]
        return settings
    except Exception as exc:
        print(exc)


def process_wrapper_write_to_kvrocks(file_name_raw: str,
                                     chunk_start: int,
                                     chunk_size: int,
                                     chunk_part: int,
                                     settings: Dict,
                                     filename_stats: Path):

    redis_connections = {}
    for key, value in settings['servers'].items():
        redis_connections[key] = redis.StrictRedis(host=value['host'], port=value['port'], db=0)
    schema_key = settings['schema_key']
    hash_div = settings['const_hash']
    number_of_servers = len(redis_connections.keys())
    start = time.process_time()
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
                    errors, parts_records = create_operations(lines, schema_key, hash_div, number_of_servers)
                    if errors:
                        errors_name = filename_stats.stem
                        filename_errors = f'{errors_name}.{chunk_start}_{chunk_size}.errors'
                        with open(filename_errors, 'wt') as file_errors:
                            file_errors.write('\n'.join(errors)+'\n')

                    for n, records in parts_records.items():
                        pipe = redis_connections[n].pipeline()
                        for key, values in records.items():
                            pipe.sadd(key, *values)
                        pipe.execute()
                    del lines
                    del parts_records
                    del errors
                    with chunk_sync.get_lock():
                        chunk_sync.value += chunk_part
                        duration = round((time.process_time() - start), 3)
                        value_sync = ceil(round(chunk_sync.value, 3))
                        print(f'done: {value_sync} %, duration chunk: {duration}', flush=True)

                    # region save statistics
                    lock_write_file.acquire()
                    try:
                        for _, redis_client in redis_connections.items():
                            redis_client.close()
                    except:
                        pass
                    try:
                        with filename_stats.open('a') as f:
                            f.write(';'.join([str(file_name_raw),
                                              str(chunk_start),
                                              str(chunk_size),
                                              str(value_sync),
                                              str(duration)])+'\n')
                    except:
                        print(f'errors with statistics: {chunk_start};{chunk_size}')
                    finally:
                        lock_write_file.release()
                    # endregion


def return_chunks_resume_mode(chunks: Set[Tuple], file_with_stats: Path, input_file: Path) -> Optional[Tuple[bool, Set[Tuple]]]:
    # fields = ['filename', 'start', 'size', 'percent', 'duration']
    result = set()
    print(f'statistics file: {str(file_with_stats)}')
    with file_with_stats.open('rt', encoding='utf-8') as stats:
        _rows = [row[:-1].split(';') for row in stats][1:]
        _rows = filter(lambda row: row[0] == str(input_file), _rows)
        result = set([(int(record[1]), int(record[2])) for record in _rows])
        print(f'records from statistics: {len(result)}')
    if result:
        return True, chunks.difference(result)


if __name__ == '__main__':
    t_start = time.perf_counter()
    parser = argparse.ArgumentParser(description='recreate Sonar dataset FDNS')

    parser.add_argument(
        "-c",
        "--config",
        dest='config_file',
        default='config.yaml',
        type=str,
        help="path to config, default: config.yaml")

    parser.add_argument(
        "-t",
        "--type",
        dest='type',
        required=True,
        choices=['subdomains', 'ipaddresses', 'subdomains_ipaddresses'],
        type=str,
        help="type load: subdomains, ipaddresses, subdomains_ipaddresses")

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
        help="size of chunk, default set 10M")

    parser.add_argument(
        "--processes",
        dest='processes',
        type=int,
        help="cpu count, not required")

    parser.add_argument(
        "--stats",
        dest='filename_stats',
        type=str,
        help="statistics filename, default: {filename}.statistics.csv")

    parser.add_argument(
        "--resume",
        dest='resume_file_stats',
        type=str,
        help="filename statistics(resume)")

    args = parser.parse_args()
    type_load = args.type

    config_yaml = args.config_file
    config = load_config(config_yaml)
    if not config:
        print(f'exit, {config_yaml} not found')
        exit(1)
    settings = prepare_config(config, type_load)
    if not settings:
        print(f'exit, {config_yaml} configuration structure errors')
        exit(1)

    if not args.processes:
        count_cpu = cpu_count() - 1
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

    jobs = []

    chunk_sync = Value(c_float, 0)
    lock_write_file = Lock()

    pool = Pool(processes=count_cpu, initializer=init_globals, initargs=(chunk_sync, lock_write_file, ))
    i = 0

    if not args.filename_stats:
        filename_stats_str = f'{input_file.stem}.statistics.csv'
        filename_stats = LOCAL_DIRECTORY / filename_stats_str
    else:
        filename_stats = Path(args.filename_stats)

    if not args.resume_file_stats:
        if not filename_stats.exists():
            with filename_stats.open('a') as f:
                f.write(';'.join(['filename', 'start', 'size', 'percent', 'duration']) + '\n')

    if args.resume_file_stats:
        resume_file_stats = Path(args.resume_file_stats)
        filename_stats = resume_file_stats
        chunks = [(chunk_start, chunk_size)
                  for chunk_start, chunk_size in chunkify_file(input_file, file_end=input_file_end, size=default_size)]
        len_chunks = len(chunks)
        print(f'all chunks: {len_chunks}')
        if resume_file_stats.is_file():
            _chunks = return_chunks_resume_mode(set(chunks), resume_file_stats, input_file)
            if _chunks:
                _, difference_chunks = _chunks
                chunks = sorted(difference_chunks, key=lambda value: value[0])
                print(f'difference: all {len_chunks}, need insert: {len(chunks)}')
        else:
            print(f'file statistics: {resume_file_stats} not found, exit.')
            print('exit')
            exit(1)
    else:
        chunks = [(chunk_start, chunk_size)
                  for chunk_start, chunk_size in chunkify_file(input_file, file_end=input_file_end, size=default_size)]


    if not chunks:
        print('all chunks saved (or errors with statistics), exit.')
        exit(0)
    all_chunks = len(chunks)
    chunk_part = round(100/all_chunks, 4)
    for chunk_start, chunk_size in chunks:
        jobs.append(pool.apply_async(process_wrapper_write_to_kvrocks,
                                     (input_file,
                                      chunk_start,
                                      chunk_size,
                                      chunk_part,
                                      settings,
                                      filename_stats,
                                      )))
    # wait for all jobs to finish
    for job in jobs:
        job.get()
    
    # clean up
    pool.close()
    pool.join()
    
    all_time = time.perf_counter() - t_start
    print(all_time)