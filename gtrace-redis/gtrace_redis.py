import json, logging
import logging, threading, pdb
from itertools import takewhile
import redis
from gtrace2.utils.misc import get_host, bytesToString, \
  fix_bytes, convert_stored_json, multidict
from django.conf import settings

logger = logging.getLogger(__name__)

def ensure_string(val):
  return val.decode() if isinstance(val, bytes) else val

def subscribed_message_parts(msg):
  if msg['type'] != 'message':
    return None, None
  msg = fix_bytes(msg)
  data = msg['data']
  print ('data', msg['data'])
  if data.startswith('{') or data.startswith('['):
    data = json.loads(msg['data'])  # expects a list of values
  return msg['channel'], data

class RedisInterface:
  GTRACE_REDIS_HOST=settings.REDIS_HOST

  def __init__(self, host=None):
    host = host or self.GTRACE_REDIS_HOST
    self._redis = redis.Redis(host=host)
    self._suffix = settings.GTRACE_SUFFIX
    self._global_warp = TestcaseData('global_warp', self)
    self._xy_tile_map = XYTileMap(self)
    self._soft_gwid_map = SoftWarpMap(self)

  @property
  def suffix(self):
    return self._suffix

  def remove_testcase_data(self, test_id):
    self._global_warp.remove(test_id)
    self._xy_tile_map.remove(test_id)

  @property
  def xy_tile(self):
    return self._xy_tile_map

  @property
  def soft_gwid(self):
    return self._soft_gwid_map

  @property
  def global_warp(self):
    return self._global_warp

  @property
  def root(self):
    return self._root

  def pubsub(self):
    '''
    Redis requires an intermediate object for handling subscriptions
    and listening for data published to subscribed to channel.
    Typically only one of these would be used for a client per
    channel or semantically related set of channels that are listened to
    together.
    :return: an instance of the pubsub intermediate object.
    '''
    return self._redis.pubsub()

  def subscribe(self, *channels):
    p = self._redis.pubsub()
    p.subscribe(*channels)
    return p

  def publish(self, pub_key, something=None, **data):
    if data and not something:
      something = json.dumps(data)
    logger.debug('key = %s, data = %s', pub_key, something)
    self._redis.publish(pub_key, something)

  def queue_put(self, queue_name, data):
    '''
    Push the json encoded form of the data to the end of
    a list-like structure in Redis that is treated as a FIFO
    :param queue_name: redis key of the queue object
    :param data: the data to json encode and push
    :return: what redis returns. Namely current length of the queue
    '''
    return self._redis.rpush(queue_name, json.dumps(data))

  def queue_get(self, *queue_names):
    '''
    Retrieve an item from the named queue.
    :param queue_name:
    :return: The decoded json object
    '''
    raw = self._redis.blpop(queue_names)
    data = ensure_string(raw[1])
    return json.loads(data)

  def hget(self, dict_name, key):
    return self._redis.hget(dict_name, key)

  def hset(self, dict_name, key, value):
    return self._redis.hset(dict_name, key, value)

  def hdel(self, dict_name, *keys):
    return self._redis.hdel(dict_name, *keys)

  def smembers(self, set_name):
    return self._redis.smembers(set_name)

  def remove(self, *keys):
    return self._redis.delete(*keys)

  def __getattr__(self, key):
    val = getattr(self._redis, key)
    if val is None:
      raise AttributeError
    return val

class HashMap:
  def __init__(self, hash_name, redis_interface):
    self._hash_name = hash_name
    self._redis_interface = redis_interface

  def put(self, item_id, data):
    self._redis_interface.hset(self._hash_name, item_id, json.dumps(data))

  def get(self, item_id):
    raw = self._redis_interface.hget(self._hash_name, item_id)
    res = multidict(list, 2)
    if raw:
      res.update(convert_stored_json(raw))
    return res

  def get_all(self):
    raw = self._redis_interface.hgetall(self._name)
    raw = fix_bytes(raw)
    return {k:json.loads(v) for k,v in raw.items()}

  def remove(self, test_id):
    self._redis_interface.hdel(self._name, test_id)


class SoftWarpMap(TestcaseData):
  def __init__(self, redis_interface):
    super().__init__('soft_warp', redis_interface)

  def get(self, test_id):
    raw = self._redis_interface.hget(self._name, test_id)
    return convert_stored_json(raw) if raw else {}

class RedisJSONMap:
  def __init__(self, redis_interface, map_name):
    self._redis_interface = redis_interface
    self._name = map_name

  def keys(self):
    return [bytesToString(s) for s in self._redis_interface.hkeys(self._name)]

  def put(self, key, data):
    self._redis_interface.hset(self._name, key, json.dumps(data))

  def get(self, key):
    raw = self._redis_interface.hget(self._name, bytesToString(key))
    return convert_stored_json(raw) if raw else {}

  def get_items(self, *keys):
    return [obj for obj in map(self.get, keys) if obj]

  def remove(self, key):
    self._redis_interface.hdel(self._name, key)

  def get_all(self):
    return {k: self.get(k) for k in self.keys()}


class WorkerConfig:
  def __init__(self, worker_hash=None, all=False):
    self._suffix = settings.GTRACE_SUFFIX
    self._worker_hash = 'worker_conf'
    if self._suffix:
      self._worker_hash = self._worker_hash + '_' + self._suffix
    self._redis_interface = GTraceRedis()
    self._data = fix_bytes(self._redis_interface.hgetall(self._worker_hash))
    self._data = bytesToString(self._data)
    self._data = fix_bytes(self._data)
    if not self._data:
      self._data = getattr(settings, 'GTRACE_WORKERS', {})
      local_info = self._data.get('localhost')
      if local_info and (len(self._data) == 1):
        self._data[get_host()] = self._data.pop('localhost')
        self._update_redis()

  @property
  def data(self):
    self._data = fix_bytes(self._redis_interface.hgetall(self._worker_hash))
    self._data = {k: eval(v) for k,v in self._data.items()}
    #ipdb.set_trace()
    return self._data

  def host_ids(self):
    return list(self.data.keys())

  def host_worker_ids(self, host_id):
    info = self.get_host_info(host_id)
    #ipdb.set_trace()
    return info.get('worker_ids', [])

  def get_host_info(self, host_id):
    data = self.data.get(host_id)
    #ipdb.set_trace()
    return data

  def update_host_info(self, host_id, **kwargs):
    data = self.get_host_info(host_id) or kwargs
    data.update(kwargs)
    #pdb.set_trace()
    self._redis_interface.hset(self._worker_hash, host_id, json.dumps(data))

  def add_local_worker(self, worker_id):
    local = get_host()
    data = self.get_host_info(local)
    current = data.get('worker_ids', []) if data else []
    self.update_host_info(local, worker_ids = current + [worker_id])

  def remove_local_worker(self, worker_id):
    local = get_host()
    data = self.get_host_info(local)
    ids = [x for x in data.get('working_ids', []) if x != worker_id]
    self.update_host_info(local, worker_ids = ids)
    
  def _update_redis(self):
    self._redis_interface.hmset(self._worker_hash, self._data)

  def clean(self, *hosts, all_hosts=False):
    if all_hosts:
      hosts = self.host_ids()

    data = self.data
    for host in hosts:
      info = data[host]
      info['worker_ids'] = []
    self._update_redis()
