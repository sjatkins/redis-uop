from sja_redis import generic
from uop import db_collection as dbc
from uop import database


class AssocSet:
    def __init__(self, redis, prefix):
        self._prefix = prefix
        self._redis = redis

    def _name(self, an_id):
        return f'{self._prefix}:{an_id}'

    def get_association(self, assoc_id):
        return self._redis.sget(self._name(assoc_id))

    def remove_assoc(self, assoc_id):
        return self._redis.delete(self._name(assoc_id))

    def disassociare(self, assoc_id, object_id):
        return self._redis.sremove(self._name(assoc_id), object_id)

    def associate(self, assoc_id, obj_id):
        return self._redis.sadd(self._name(assoc_id), obj_id)


class RedisTagSet(AssocSet):
    def __init__(self, redis):
        super().__init__(redis, 'tagset')

class RedisObjectTags(AssocSet):
    def __init__(self, redis):
        super().__init__(redis, 'objecttags')

class RedisGroupSet(AssocSet):
    def __init__(self, redis):
        super().__init__(redis, 'groupset')




class RedisCollection(dbc.DBCollection):
    pass



class RedisDatabase(database.Database):
    pass


