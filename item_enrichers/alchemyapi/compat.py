import sys

__all__ = ['urlencode']

if sys.version_info.major == 3:
    from urllib.parse import urlencode
else:
    from urllib import urlencode

