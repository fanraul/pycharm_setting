from importlib import reload

from script1 import a

import script1

print(dir(script1))

print('-------------')
print(a)
print(script1.b)


#reload(script1)

