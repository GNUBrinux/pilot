
<<<<<<< HEAD
__author__    = "Andre Merzky"
=======
__author__    = "Andre Merzky, Ole Weidner"
>>>>>>> origin/titan
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


<<<<<<< HEAD
import saga.adaptors.base        as sab
import saga.attributes           as sa
from   saga.constants            import SYNC, ASYNC, TASK
from   saga.filesystem.constants import *
import saga.namespace.entry      as nsentry
import saga.session              as ss
import saga.task                 as st
import saga.url                  as surl
import saga.utils.signatures     as sus
=======
import radical.utils.signatures  as rus

import saga.adaptors.base        as sab
import saga.attributes           as sa
import saga.session              as ss
import saga.task                 as st
import saga.url                  as surl
import saga.namespace.entry      as nsentry

from   saga.filesystem.constants import *
from   saga.constants            import SYNC, ASYNC, TASK
>>>>>>> origin/titan


# ------------------------------------------------------------------------------
#
class LogicalFile (nsentry.Entry, sa.Attributes) :

    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('LogicalFile', 
                  sus.optional ((surl.Url, basestring)), 
                  sus.optional (int), 
                  sus.optional (ss.Session),
                  sus.optional (sab.Base), 
                  sus.optional (dict), 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns (sus.nothing)
=======
    @rus.takes   ('LogicalFile', 
                  rus.optional ((surl.Url, basestring)), 
                  rus.optional (int, rus.nothing), 
                  rus.optional (ss.Session),
                  rus.optional (sab.Base), 
                  rus.optional (dict), 
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns (rus.nothing)
>>>>>>> origin/titan
    def __init__ (self, url=None, flags=READ, session=None, 
                  _adaptor=None, _adaptor_state={}, _ttype=None) : 
        '''
        __init__(url=None, flags=READ, session=None)

        url:       saga.Url
        flags:     flags enum
        session:   saga.Session
        ret:       obj
        '''

        # param checks
<<<<<<< HEAD
=======
        if not flags : flags = 0
>>>>>>> origin/titan
        url = surl.Url (url)

        self._nsentry = super  (LogicalFile, self)
        self._nsentry.__init__ (url, flags, session, 
                                _adaptor, _adaptor_state, _ttype=_ttype)


    # --------------------------------------------------------------------------
    #
    @classmethod
<<<<<<< HEAD
    @sus.takes   ('LogicalFile', 
                  sus.optional ((surl.Url, basestring)), 
                  sus.optional (int), 
                  sus.optional (ss.Session),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns (st.Task)
=======
    @rus.takes   ('LogicalFile', 
                  rus.optional ((surl.Url, basestring)), 
                  rus.optional (int, rus.nothing), 
                  rus.optional (ss.Session),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns (st.Task)
>>>>>>> origin/titan
    def create (cls, url=None, flags=READ, session=None, ttype=None) :
        '''
        url:       saga.Url
        flags:     saga.replica.flags enum
        session:   saga.Session
        ttype:     saga.task.type enum
        ret:       saga.Task
        '''

<<<<<<< HEAD
=======
        if not flags : flags = 0
>>>>>>> origin/titan
        _nsentry = super (LogicalFile, cls)
        return _nsentry.create (url, flags, session, ttype=ttype)



    # ----------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('LogicalFile', 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((bool, st.Task))
=======
    @rus.takes   ('LogicalFile', 
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((bool, st.Task))
>>>>>>> origin/titan
    def is_file (self, ttype=None) :
        '''
        is_file()

        ttype:          saga.task.type enum
        ret:            bool / saga.Task
        '''
        return self.is_entry (ttype=ttype)


    # --------------------------------------------------------------------------
    #
    # replica methods
    #
<<<<<<< HEAD
    @sus.takes   ('LogicalFile', 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((int, st.Task))
=======
    @rus.takes   ('LogicalFile', 
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((int, st.Task))
>>>>>>> origin/titan
    def get_size (self, ttype=None) :
        '''
        get_size()

        Return the size of the file.

        ttype:    saga.task.type enum
        ret:      int / saga.Task
        
        Returns the size of the physical file represented by this logical file (in bytes)

           Example::

               # get a file handle
               lf = saga.replica.LogicalFile("irods://localhost/tmp/data/data.bin")
    
               # print the logical file's size
               print lf.get_size ()

        '''
        return self._adaptor.get_size_self (ttype=ttype)

  
    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('LogicalFile', 
                  sus.optional ((surl.Url, basestring)), 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.nothing, st.Task))
=======
    @rus.takes   ('LogicalFile', 
                  rus.optional ((surl.Url, basestring)), 
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.nothing, st.Task))
>>>>>>> origin/titan
    def add_location (self, name, ttype=None) :
        '''
        add_location(name)

        Add a physical location.

        name:           saga.Url
        ttype:          saga.task.type enum
        ret:            None / saga.Task
        '''
        return self._adaptor.add_location (name, ttype=ttype)


    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('LogicalFile', 
                  sus.optional ((surl.Url, basestring)), 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.nothing, st.Task))
=======
    @rus.takes   ('LogicalFile', 
                  rus.optional ((surl.Url, basestring)), 
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.nothing, st.Task))
>>>>>>> origin/titan
    def remove_location (self, name, ttype=None) :
        '''
        remove_location(name)

        Remove a physical location.

        name:           saga.Url
        ttype:          saga.task.type enum
        ret:            None / saga.Task
        '''
        return self._adaptor.remove_location (name, ttype=ttype)


    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('LogicalFile', 
                  sus.optional ((surl.Url, basestring)), 
                  sus.optional ((surl.Url, basestring)), 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.nothing, st.Task))
=======
    @rus.takes   ('LogicalFile', 
                  rus.optional ((surl.Url, basestring)), 
                  rus.optional ((surl.Url, basestring)), 
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.nothing, st.Task))
>>>>>>> origin/titan
    def update_location (self, old, new, ttype=None) :
        '''
        update_location(old, new)

        Updates a physical location.

        old:            saga.Url
        new:            saga.Url 
        ttype:          saga.task.type enum
        ret:            None / saga.Task
        '''
        return self._adaptor.update_location (old, new, ttype=ttype)


    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('LogicalFile', 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.list_of (surl.Url), st.Task))
=======
    @rus.takes   ('LogicalFile', 
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.list_of (surl.Url), st.Task))
>>>>>>> origin/titan
    def list_locations (self, ttype=None) :
        '''
        list_locations()

        List all physical locations of a logical file.

        ttype:          saga.task.type enum
        ret:            list [saga.Url] / saga.Task
        '''
        return self._adaptor.list_locations (ttype=ttype)


    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('LogicalFile', 
                  (surl.Url, basestring), 
                  sus.optional (int),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.nothing, st.Task))
=======
    @rus.takes   ('LogicalFile', 
                  (surl.Url, basestring), 
                  rus.optional (int, rus.nothing),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.nothing, st.Task))
>>>>>>> origin/titan
    def replicate (self, name, flags=None, ttype=None) :
        '''
        replicate(name)

        Replicate a logical file.

        name:           saga.Url
        flags:          flags enum
        ttype:          saga.task.type enum
        ret:            None / saga.Task
        '''
<<<<<<< HEAD
=======
        if not flags : flags = 0
>>>>>>> origin/titan
        return self._adaptor.replicate (name, flags, ttype=ttype)
    

    # --------------------------------------------------------------------------
    # non-GFD.90
    #
<<<<<<< HEAD
    @sus.takes   ('LogicalFile', 
                  (surl.Url, basestring), 
                  sus.optional ((surl.Url, basestring)),
                  sus.optional (int),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.nothing, st.Task))
=======
    @rus.takes   ('LogicalFile', 
                  (surl.Url, basestring), 
                  rus.optional ((surl.Url, basestring)),
                  rus.optional (int, rus.nothing),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.nothing, st.Task))
>>>>>>> origin/titan
    def upload (self, name, tgt=None, flags=None, ttype=None) :
        '''
        upload(name, tgt=None, flags=None)

        Upload a physical file.

        name:           saga.Url
        tgt:            saga.Url
        flags:          flags enum
        ttype:          saga.task.type enum
        ret:            None / saga.Task
        '''
<<<<<<< HEAD
=======
        if not flags : flags = 0
>>>>>>> origin/titan
        return self._adaptor.upload (name, tgt, flags, ttype=ttype)
    
 
    # --------------------------------------------------------------------------
    # non-GFD.90
    #
<<<<<<< HEAD
    @sus.takes   ('LogicalFile', 
                  (surl.Url, basestring), 
                  sus.optional ((surl.Url, basestring)),
                  sus.optional (int),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.nothing, st.Task))
=======
    @rus.takes   ('LogicalFile', 
                  (surl.Url, basestring), 
                  rus.optional ((surl.Url, basestring)),
                  rus.optional (int, rus.nothing),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.nothing, st.Task))
>>>>>>> origin/titan
    def download (self, name, src=None, flags=None, ttype=None) :
        '''
        download(name, src=None, flags=None)

        Download a physical file.

        name:           saga.Url
        src:            saga.Url
        flags:          flags enum
        ttype:          saga.task.type enum
        ret:            None / saga.Task
        '''
<<<<<<< HEAD
        return self._adaptor.download (name, src, flags, ttype=ttype)
    
  
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
=======
        if not flags : flags = 0
        return self._adaptor.download (name, src, flags, ttype=ttype)
    
  

>>>>>>> origin/titan

