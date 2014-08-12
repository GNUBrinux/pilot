
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"



import saga.adaptors.base    as sab
import saga.attributes       as sa
import saga.base             as sb
from   saga.constants import LIFE_TIME, REMOTE_ID, REMOTE_HOST, REMOTE_PORT, TOKEN
from   saga.constants import TYPE, SERVER, USER_CERT, CERT_REPOSITORY
from   saga.constants import USER_PROXY, USER_KEY, USER_ID, USER_PASS, USER_VO
import saga.utils.signatures as sus


# ------------------------------------------------------------------------------
#
class Context (sb.Base, sa.Attributes) :
    '''
    A SAGA Context is a description of a security token. A context 
    can point for example to a X.509 certificate, an SSH key-pair or 
    an entry in a MyProxy key-server.
     
    It is important to understand that a Context only points to a security 
    token but it will not hold the certificate contents itself.

    Contexts are used to tell the adaptors which security tokens are 
    supposed to be used.  By default, most SAGA adaptors will try to
    pick up such tokens from their default location, but in some cases 
    it might be necessary to explicitly define them. An non-default
    SSH Context for example can be defined like this::

        ctx = saga.Context("SSH")

        ctx.user_id   = "johndoe"
        ctx.user_key  = "/home/johndoe/.ssh/key_for_machine_x"
        ctx.user_pass = "XXXX"  # password to decrypt 'user_key' (if required)

        session = saga.Session()
        session.add_context(ctx)

        js = saga.job.Service("ssh://machine_x.futuregrid.org",
                              session=session)

    Contexts in SAGA are extensible and implemented similar to the adaptor 
    mechanism. Currently, the following Context types are supported:

    '''

    # --------------------------------------------------------------------------
    #
    @sus.takes   ('Context', 
                  basestring, 
                  sus.optional (sab.Base),
                  sus.optional (dict))
    @sus.returns (sus.nothing)
    def __init__ (self, ctype, _adaptor=None, _adaptor_state={}) : 
        '''
        ctype: string
        ret:   None
        '''

        sb.Base.__init__ (self, ctype.lower(), _adaptor, _adaptor_state, ctype, ttype=None)


        import saga.attributes as sa

        # set attribute interface propertiesP
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # register properties with the attribute interface
        self._attributes_register  (TYPE,            None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (SERVER,          None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (TOKEN,           None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (CERT_REPOSITORY, None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (USER_PROXY,      None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (USER_CERT,       None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (USER_KEY,        None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (USER_ID,         None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (USER_PASS,       None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (USER_VO,         None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (LIFE_TIME,       -1,   sa.INT,    sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (REMOTE_ID,       None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (REMOTE_HOST,     None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (REMOTE_PORT,     None, sa.STRING, sa.VECTOR, sa.WRITEABLE)

        self.type = ctype


    # --------------------------------------------------------------------------
    #
    @sus.takes   ('Context')
    @sus.returns (basestring)
    def __str__  (self) :

        d = self.as_dict ()
        s = "{"

        for key in sorted (d.keys ()) :
            if  key == 'UserPass' and d[key] :
                s += "'UserPass' : '%s'" % ('x'*len(d[key]))
            else :
                s += "'%s' : '%s'" % (key, d[key])
            s += ', '

        return "%s}" % s[0:-2]


    # --------------------------------------------------------------------------
    #
    @sus.takes   ('Context')
    @sus.returns (basestring)
    def __repr__ (self) :

        return str(self)


    # --------------------------------------------------------------------------
    #
    @sus.takes      ('Context', 
                     ('Session', '_DefaultSession'))
    @sus.returns    (sus.nothing)
    def _initialize (self, session) :
        '''
        ret:  None
        '''
        self._adaptor._initialize (session)


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

