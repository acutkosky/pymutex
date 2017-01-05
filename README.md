# pymutex

To lock a resource (say the file 'path/to/file`), use:
```
import pymutex
lockID = 'path/to/file'
pymutex.lock(lockstring)
# pymutex.lock(lockstring,20) <- lock times out after 20 seconds. Default is 10.
pymutex.locked_by_us(lockstring) #returns True

# manipulate path/to/file in some way

pymutex.unlock(lockstring)
```

