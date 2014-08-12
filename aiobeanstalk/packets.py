from operator import itemgetter


class BasePacketMeta(type):

    registry = {}

    def __new__(cls, name, bases, dic):
        token = dic['token']
        if 'fields' not in dic:
            numargs = 0
            dic['__slots__'] = ()
        else:
            numargs = len(dic['fields'])
            dic['__slots__'] = tuple(map(itemgetter(0), dic['fields']))
        self = super().__new__(cls, name, bases, dic)
        if token is not None:
            cls.registry[token.encode('ascii'), numargs] = self
        return self


class BasePacket(metaclass=BasePacketMeta):
    token = None
    fields = []

    def __init__(self, *args):
        assert len(args) == len(self.fields), (args, self.fields)
        for (k, _), v in zip(self.fields, args):
            setattr(self, k, v)

    def __eq__(self, other):
        return (self.__class__ == other.__class__ and
                all(getattr(self, a) == getattr(other, a)
                    for a, _ in self.fields))

    def __repr__(self):
        return '<{} {}>'.format(self.__class__.__name__,
            ', '.join('{}={}'.format(a, getattr(self, a))
                      for a, _ in self.fields))


class Inserted(BasePacket):
    token = "INSERTED"
    fields = [('job_id', int)]


class Buried(BasePacket):
    token = "BURIED"


class Using(BasePacket):
    token = "USING"
    fields = [('tube', str)]


class Reserved(BasePacket):
    token = "RESERVED"
    fields = [('job_id', int), ('data', bytes)]


class Deleted(BasePacket):
    token = "DELETED"


class Released(BasePacket):
    token = "RELEASED"


class Touched(BasePacket):
    token = "TOUCHED"


class Watching(BasePacket):
    token = "WATCHING"
    fields = [('count', int)]


class Found(BasePacket):
    token = "FOUND"
    fields = [('job_id', int), ('data', bytes)]


class Kicked(BasePacket):
    token = "KICKED"


class Ok(BasePacket):
    token = "OK"
    fields = [('data', bytes)]


class Paused(BasePacket):
    token = "PAUSED"
