

__all__ = ['Auth']

class Auth:

    def __init__(self, key=None):
        self.__key = key

    def __repr__(self):
        classname = self.__class__.__name__
        return '%s(key=%r)' % (classname, 'SECRET' if self.key else None)

    def is_valid(self):
        if not self.key: return False
        assert isinstance(self.key, str)
        assert len(self.key) == 40
        return True

    @property
    def key(self):
        return self.__key

