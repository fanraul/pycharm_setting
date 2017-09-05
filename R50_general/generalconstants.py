
log = True

def logprint(*args, sep=' ',  end='\n',  file=None ):
    if log:
        print(*args, sep=' ', end='\n', file=None)
