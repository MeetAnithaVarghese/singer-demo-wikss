import collections
import collections.abc
# The magic fix for Python 3.11
collections.MutableMapping = collections.abc.MutableMapping

from target_postgres import main
if __name__ == '__main__':
    main()