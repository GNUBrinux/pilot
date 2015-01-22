
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


<<<<<<< HEAD
import saga.adaptors.base       as sab
from   saga.advert.constants    import *
import saga.attributes          as sa
from   saga.constants           import SYNC, ASYNC, TASK
import saga.namespace.directory as nsdir
import saga.session             as ss
import saga.task                as st
import saga.url                 as surl
import saga.utils.signatures    as sus
=======
import radical.utils.signatures as rus

import saga.adaptors.base       as sab
import saga.attributes          as sa
import saga.session             as ss
import saga.task                as st
import saga.url                 as surl
import saga.namespace.directory as nsdir

from   saga.advert.constants    import *
from   saga.constants           import SYNC, ASYNC, TASK
>>>>>>> origin/titan


# ------------------------------------------------------------------------------
#
class Directory (nsdir.Directory, sa.Attributes) :

    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('Directory', 
                  sus.optional ((surl.Url, basestring)), 
                  sus.optional (int),
                  sus.optional (ss.Session), 
                  sus.optional (sab.Base),
                  sus.optional (dict),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns (sus.nothing)
=======
    @rus.takes   ('Directory', 
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

        self._nsdirec = super  (Directory, self)
        self._nsdirec.__init__ (url, flags, session, 
                                _adaptor, _adaptor_state, _ttype=_ttype)

        # set attribute interface properties
        self._attributes_allow_private (True)
        self._attributes_camelcasing   (True)
        self._attributes_extensible    (True, getter=self._attribute_getter, 
                                              setter=self._attribute_setter,
                                              lister=self._attribute_lister,
                                              caller=self._attribute_caller)

        # register properties with the attribute interface 
        self._attributes_register   (ATTRIBUTE, None, sa.STRING, sa.SCALAR, sa.READONLY)
        self._attributes_register   (CHANGE,    None, sa.STRING, sa.SCALAR, sa.READONLY)
        self._attributes_register   (NEW,       None, sa.STRING, sa.SCALAR, sa.READONLY)
        self._attributes_register   (DELETE,    None, sa.STRING, sa.SCALAR, sa.READONLY)
        self._attributes_register   (TTL,       None, sa.INT,    sa.SCALAR, sa.WRITEABLE)

        self._attributes_set_setter (TTL, self.set_ttl)
        self._attributes_set_getter (TTL, self.get_ttl)



    # --------------------------------------------------------------------------
    #
    @classmethod
<<<<<<< HEAD
    @sus.takes   ('Directory', 
                  sus.optional ((surl.Url, basestring)), 
                  sus.optional (int), 
                  sus.optional (ss.Session), 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns (st.Task)
=======
    @rus.takes   ('Directory', 
                  rus.optional ((surl.Url, basestring)), 
                  rus.optional (int, rus.nothing), 
                  rus.optional (ss.Session), 
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns (st.Task)
>>>>>>> origin/titan
    def create (cls, url=None, flags=READ, session=None, ttype=None) :
        '''
        url:       saga.Url
        flags:     saga.advert.flags enum
        session:   saga.Session
        ttype:     saga.task.type enum
        ret:       saga.Task
        '''

<<<<<<< HEAD
=======
        if not flags : flags = 0
>>>>>>> origin/titan
        _nsdir = super (Directory, cls)
        return _nsdir.create (url, flags, session, ttype=ttype)


    # --------------------------------------------------------------------------
    #
    # attribute methods
    #
    # NOTE: we do not yet pass ttype, as async calls are not yet supported by
    # the attribute interface
    #
<<<<<<< HEAD
    @sus.takes   ('Directory', 
                  basestring,
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.anything, st.Task))
=======
    @rus.takes   ('Directory', 
                  basestring,
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.anything, st.Task))
>>>>>>> origin/titan
    def _attribute_getter (self, key, ttype=None) :

        return self._adaptor.attribute_getter (key)


    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('Directory', 
                  basestring,
                  sus.anything,
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.nothing, st.Task))
=======
    @rus.takes   ('Directory', 
                  basestring,
                  rus.anything,
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.nothing, st.Task))
>>>>>>> origin/titan
    def _attribute_setter (self, key, val, ttype=None) :

        return self._adaptor.attribute_setter (key, val)


    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('Directory', 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.list_of (sus.anything), st.Task))
=======
    @rus.takes   ('Directory', 
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.list_of (rus.anything), st.Task))
>>>>>>> origin/titan
    def _attribute_lister (self, ttype=None) :

        return self._adaptor.attribute_lister ()


    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('Directory', 
                  basestring, 
                  int, 
                  callable, 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.anything, st.Task))
=======
    @rus.takes   ('Directory', 
                  basestring, 
                  int, 
                  callable, 
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.anything, st.Task))
>>>>>>> origin/titan
    def _attribute_caller (self, key, id, cb, ttype=None) :

        return self._adaptor.attribute_caller (key, id, cb)



    # ----------------------------------------------------------------
    #
    # advert methods
    #
<<<<<<< HEAD
    @sus.takes   ('Directory', 
                  (surl.Url, basestring), 
                  float, 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.nothing, st.Task))
=======
    @rus.takes   ('Directory', 
                  (surl.Url, basestring), 
                  float, 
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.nothing, st.Task))
>>>>>>> origin/titan
    def set_ttl  (self, tgt=None, ttl=-1.0, ttype=None) : 
        """
        tgt :           saga.Url / None
        ttl :           int
        ttype:          saga.task.type enum
        ret:            None / saga.Task
        """

        if tgt  :  return self._adaptor.set_ttl      (tgt, ttl, ttype=ttype)
        else    :  return self._adaptor.set_ttl_self (     ttl, ttype=ttype)

     
    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('Directory', 
                  sus.optional ((surl.Url, basestring)), 
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((float, st.Task))
=======
    @rus.takes   ('Directory', 
                  rus.optional ((surl.Url, basestring)), 
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((float, st.Task))
>>>>>>> origin/titan
    def get_ttl  (self, tgt=None, ttype=None) : 
        """
        tgt :           saga.Url / None
        ttype:          saga.task.type enum
        ret:            int / saga.Task
        """

        if tgt  :  return self._adaptor.get_ttl      (tgt, ttype=ttype)
        else    :  return self._adaptor.get_ttl_self (     ttype=ttype)


    # --------------------------------------------------------------------------
    #
<<<<<<< HEAD
    @sus.takes   ('Directory', 
                  sus.optional (basestring),
                  sus.optional (basestring),
                  sus.optional ((basestring, object)),
                  sus.optional (int),
                  sus.optional (sus.one_of (SYNC, ASYNC, TASK)))
    @sus.returns ((sus.list_of (surl.Url), st.Task))
=======
    @rus.takes   ('Directory', 
                  rus.optional (basestring),
                  rus.optional (basestring),
                  rus.optional ((basestring, object)),
                  rus.optional (int, rus.nothing),
                  rus.optional (rus.one_of (SYNC, ASYNC, TASK)))
    @rus.returns ((rus.list_of (surl.Url), st.Task))
>>>>>>> origin/titan
    def find     (self, name_pattern, attr_pattern=None, obj_type=None,
                  flags=RECURSIVE, ttype=None) : 
        """
        name_pattern:   string
        attr_pattern:   string
        obj_type:       string
        flags:          flags enum
        ret:            list [saga.Url]
        """
        
<<<<<<< HEAD
=======
        if not flags : flags = 0
>>>>>>> origin/titan
        if attr_pattern or obj_type : 
            return self._adaptor.find_adverts (name_pattern, attr_pattern, obj_type, flags, ttype=ttype)
        else :
            return self._nsdirec.find         (name_pattern,                         flags, ttype=ttype)



<<<<<<< HEAD
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
=======

>>>>>>> origin/titan

