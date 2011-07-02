import socket
from struct import pack, unpack
from binascii import hexlify

# Server commands 
SrvrQuit = 1 # Disconnect 
SrvrGetError = 2 # Get extended error text 
SrvrAccount = 3 # Set account 
SrvrOpen = 4 # Open file 
SrvrClose = 5 # Close file 
SrvrRead = 6 # Read record 
SrvrReadl = 7 # Read record with shared lock 
SrvrReadlw = 8 # Read record with shared lock, waiting 
SrvrReadu = 9 # Read record with exclusive lock 
SrvrReaduw = 10 # Read record with exclusive lock, waiting 
SrvrSelect = 11 # Select file 
SrvrReadNext = 12 # Read next id from select list 
SrvrClearSelect = 13 # Clear select list 
SrvrReadList = 14 # Read a select list 
SrvrRelease = 15 # Release lock 
SrvrWrite = 16 # Write record 
SrvrWriteu = 17 # Write record, retaining lock 
SrvrDelete = 18 # Delete record 
SrvrDeleteu = 19 # Delete record, retaining lock 
SrvrCall = 20 # Call catalogued subroutine 
SrvrExecute = 21 # Execute command 
SrvrRespond = 22 # Respond to request for input 
SrvrEndCommand = 23 # Abort command 
SrvrLogin = 24 # Network login 
SrvrLocalLogin = 25 # QMLocal login 
SrvrSelectIndex = 26 # Select index 
SrvrEnterPackage = 27 # Enter a licensed package 
SrvrExitPackage = 28 # Exit from a licensed package 
SrvrOpenQMNet = 29 # Open QMNet file 
SrvrLockRecord = 30 # Lock a record 
SrvrClearfile = 31 # Clear file 
SrvrFilelock = 32 # Get file lock 
SrvrFileunlock = 33 # Release file lock 
SrvrRecordlocked = 34 # Test lock 
SrvrIndices1 = 35 # Fetch information about indices 
SrvrIndices2 = 36 # Fetch information about specific index 
SrvrSelectList = 37 # Select file and return list 
SrvrSelectIndexv = 38 # Select index, returning indexed values 
SrvrSelectIndexk = 39 # Select index, returning keys for indexed value 
SrvrFileinfo = 40 # FILEINFO() 
SrvrReadv = 41 # READV and variants 
SrvrSetLeft = 42 # Align index position to left 
SrvrSetRight = 43 # Align index position to right 
SrvrSelectLeft = 44 # Move index position to left 
SrvrSelectRight = 45 # Move index position to right 
SrvrMarkMapping = 46 # Enable/disable mark mapping 

# Server error status values 
SV_OK = 0 # Action successful 
SV_ON_ERROR = 1 # Action took ON ERROR clause 
SV_ELSE = 2 # Action took ELSE clause 
SV_ERROR = 3 # Action failed. Error text available 
SV_LOCKED = 4 # Action took LOCKED clause 
SV_PROMPT = 5 # Server requesting input 


class QMMessage(object):
    def __init__(self, m_type, data_out):
        self.out_message_type = m_type
        self.out_data = data_out if data_out is not None else ''
        self.out_len = len(self.out_data)

    def __str__(self):
        return "OUT DATA:%s; IN DATA:%s" % (self.out_data, self.in_data)

    def get_header(self):
        # HACKS
        return pack("=lh",len(self.out_data) + 6, self.out_message_type)

    in_data = None
    in_len = None
    in_error = None
    in_status = None
    in_error_text = None


class QMRecord(object):
    TEXT_MARK_STRING = "\xFB"
    SUBVALUE_MARK_STRING = "\xFC"
    VALUE_MARK_STRING = "\xFD"
    FIELD_MARK_STRING = "\xFE"
    ITEM_MARK_STRING = "\xFF"

    def __init__(self, src_data=None):
        if src_data is not None:
            self.data = self.unpack(src_data)

    def unpack(self, src_data):
        fields = src_data.split(self.FIELD_MARK_STRING)
        
        for field in fields:
            if self.VALUE_MARK_STRING in field:
                field = field.split(self.VALUE_MARK_STRING)
                for value in values:
                    if self.SUBVALUE_MARK_STRING in value:
                       value = value.split(self.SUBVALUE_MARK_STRING)

        self.data = fields

    def pack(self):
        data_out = ''
        if isinstance(self.data, list):
            for field in self.data:
                if isinstance(field, str):
                    data_out = data_out + field + self.FIELD_MARK_STRING
                else:
                    for value in field:
                        if isinstance(value, str):
                            data_out = data_out + value + self.VALUE_MARK_STRING
                        else:
                            for subval in value:
                                data_out = data_it + subval + self.SUBVALUE_MARK_STRING
        else:
            data_out = self.data

        return data_out


class QMClient(object):
    max_username_len = 32
    status = None
    __filenos = {}

    def __init__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


    def connect(self, account, username=None, password=None, host='localhost', port=4243):
        '''
            Connects to a OpenQM server account.
        '''
        status = False

        self.socket.connect((host, port))

        ack_ok = False
        while not ack_ok: # We need to wait for QM to be ready
            msg = self.socket.recv(1)
            if "\x06" in msg:
                ack_ok = True
        if username is not None:
            if len(username) > self.max_username_len:
                raise ValueError, "Username is too long"

            # HACKS
            d = pack("=h%is"%len(username), len(username), username)
            if len(d) % 2 == 1:
                d = d + "\x00"
            d = d + pack("=h%is"%len(password), len(password), password)
            if len(d) % 2 == 1:
                d = d + "\x00"
            login_msg = QMMessage(SrvrLogin, d)

            self._message_pair(login_msg)
            if login_msg.in_error != SV_OK:
                self.socket.close()
                raise Exception, "Error while connecting: %s" % login_msg.in_error_text

        account_msg = QMMessage(SrvrAccount, account)
        self._message_pair(account_msg)
        if account_msg.in_error != SV_OK:
            self.socket.close()
            raise Exception, "Error while connecting account: %s" % account_msg.in_error_text

        return True


    def disconnect(self):
        '''
            Closes the server socket.
        '''
        self._write_packet(QMMessage(SrvrQuit, None))
        self.socket.close()


    def logto(self, account):
        '''
            Tells the server to LOGTO an account.
        '''
        ret = self._message_pair(QMMessage(SrvrAccount, account))
        if ret is False:
            raise Exception, "Error while writing to server."

        if ret.in_error == SV_OK:
            return True
        elif ret.in_error == SV_ON_ERROR:
            raise Exception, "Server aborted while prossessing logto: %s" % ret.in_error_text
        elif ret.in_error == SV_ERROR:
            raise Exception, "Server returned an error: %s" % ret.in_error_text


    def execute(self, msg):
        '''
            Executes an arbitrary command on the server.
        '''
        ret = self._message_pair(QMMessage(SrvrExecute, msg))
        if ret is False:
            raise Exception, "Error while writing to server."

        if ret.in_error == SV_ON_ERROR:
            raise Exception, "Server aborted running command: %s" % ret.in_error_text
 
        return (ret.in_data, ret.in_error)


    def open(self, filename):
        """
            Opens a file on the server for use.
        """
        ret = self._message_pair(QMMessage(SrvrOpen, filename))
        if ret is False:
            raise Exception, "Error while writing to server."

        if ret.in_error == SV_ON_ERROR:
            raise Exception, "Server aborted opening file: %s" % ret.in_error_text
        
        self.__filenos[filename.lower()] = unpack("=h", ret.in_data)[0]

        return (self.__filenos[filename.lower()], ret.in_error)


    def close(self, filename):
        '''
            Closes a file on the server
        '''
        filnum = self.__filenos[filename.lower()]
        ret = self._message_pair(QMMessage(SrvrClose, pack("=h", filnum)))
        if ret is False:
            raise Exception, "Error while writing to server."

        if ret.in_error == SV_ON_ERROR:
            raise Exception, "Server aborted closing file: %s" % ret.in_error_text

        return (True, ret.in_error)


    def read(self, filename, rec_id):
        '''
            Read a record
        '''
        if filename.lower() not in self.__filenos:
            raise Exception, "File is not opened"

        fno = self.__filenos[filename.lower()]
        ret = self._read_record(fno, rec_id, SrvrRead)

        return (ret.in_data, ret.in_error)


    def read_shared(self, filename, rec_id, wait):
        '''
            Read a record with a shared lock
        '''
        if filename.lower() not in self.__filenos:
            raise Exception, "File is not opened"

        fno = self.__filenos[filename.lower()]
        ret = self._read_record(fno, rec_id, SrvrReadlw if wait else SrvrReadl)

        return (ret.in_data, ret.in_error)


    def read_excl(self, filename, rec_id, wait):
        """
            Read a record with an exclusive lock
        """
        if filename.lower() not in self.__filenos:
            raise Exception, "File is not opened"
    
        fno = self.__filenos[filename.lower()]
        ret = self._read_record(fno, rec_id, SrvrReaduw if wait else SrvrReadu)
            
        return (ret.in_data, ret.in_error)


    def record_lock(self, filename, rec_id, update=False, wait=False):
        if filename.lower() not in self.__filenos:
            raise Exception, "File is not opened"

        fno = self.__filenos[filename.lower()]
        flags = (1 if update else 0) + (2 if wait else 0)
        msg_data = pack("=hh%is"%len(rec_id), fno, flags, rec_id)
        ret = self._message_pair(QMMessage(SrvrLockRecord, msg_data))

        if ret.in_error == SV_ON_ERROR:
            raise Exception, "Server aborted record lock: %s" % ret.in_error_text


    def select(self, filename, list_id):
        '''
            Generates a select list containing the ids of all records in a file
        '''
        if filename.lower() not in self.__filenos:
            raise Exception, "File is not opened"
        fno = self.__filenos[filename.lower()]
        ret = self._message_pair(QMMessage(SrvrSelect, pack("=hh", fno, list_id)))

        if ret is False:
            raise Exception, "Error while writing to server."

        if ret.in_error == SV_ON_ERROR:
            raise Exception, "Server aborted on select: %s" % ret.in_error_text

        return (True, ret.in_error)


    def write(self, filename, rec_id, data):
        if filename.lower() not in self.__filenos:
            raise Exception, "File is not opened"

        fno = self.__filenos[filename.lower()]
        return self._write_record(fno, rec_id, data, SrvrWrite)


    def write_retain(self, filename, rec_id, data):
        if filename.lower() not in self.__filenos:
            raise Exception, "File is not opened"

        fno = self.__filenos[filename.lower()]
        return self._write_record(fno, rec_id, data, SrvrWriteu)


    def select_left(self, filename, index_name, list_id):
        '''
            Scan index position to the left
        '''
        return self._selectlr(filename, index_name, list_id, SrvrSelectLeft)


    def select_right(self, filename, index_name, list_id):
        '''
            Scan index position to the right
        '''
        return self._selectlr(filename, index_name, list_id, SrvrSelectRight)


    def select_index(self, filename, index_name, index_value, list_id):
        '''
            Generate select list from index entry
        '''
        if filename.lower() not in self.__filenos:
            raise Exception, "File is not opened"
        fno = self.__filenos[filename.lower()]
        msg = pack("=hhh", fno, list_id, len(index_name))
        msg = msg + pack("=%is" % len(index_name), index_name)
        if len(index_name) % 2 == 1:
            msg = msg + "\x00"
        msg = msg + pack("=h%is" % len(index_value), len(index_value), index_value)
        if len(index_name) % 2 == 1:
            msg = msg + "\x00"

        ret = self._message_pair(QMMessage(SrvrSelectIndex, msg))
        
        if ret is False:
            raise Exception, "Error while writing to server."

        if ret.in_error == SV_ON_ERROR:
            raise Exception, "Server aborted on select index: %s" % ret.in_error_text

        return (True, ret.in_error)


    def clear_select(self, list_id):
        '''
            Clears a select list on the server
        '''
        ret = self._message_pair(QMMessage(SrvrClearSelect, pack("=h", list_id)))

        if ret is False:
            raise Exception, "Error while writing to server."

        if ret.in_error == SV_ON_ERROR:
            raise Exception, "Server aborted clearing the select list: %s" % ret.in_error_text

        return (True, ret.in_error)


    def _selectlr(self,filename, index_name, list_id, msg_type):
        if filename.lower() not in self.__filenos:
            raise Exception, "File is not opened"
        fno = self.__filenos[filename.lower()]
        msg = pack("=hh%is"%len(index_name), fno, list_id, index_name)
        ret = self._message_pair(QMMessage(msg_type, msg))

        if ret is False:
            raise Exception, "Error while writing to server."

        if ret.in_error == SV_ON_ERROR:
            raise Exception, "Server aborted on select left/right: %s" % ret.in_error_text

        return (ret.in_data if ret.in_error == SV_OK else None, ret.in_error)


    def _read_record(self, fno, rec_id, msg_type):
        rec_id = rec_id if isinstance(rec_id, str) else str(rec_id)
        if msg_type not in [SrvrRead, SrvrReadl, SrvrReadlw, SrvrReadu, SrvrReaduw]:
            raise Exception, "Message Type is not valid for record read"

        if len(rec_id) < 1 and len(rec_id) > 255:
            raise Exception, "Record ID needs to be betwee 1 and 255 characters"

        msg_data = pack("=h%is"%len(rec_id), fno, rec_id)
        ret = self._message_pair(QMMessage(msg_type, msg_data))
        if ret.in_error == SV_ON_ERROR:
            raise Exception, "Server aborted reading record: %s" % ret.in_error_text

        return ret

    def _write_record(self, fno, rec_id, data, msg_type):
        if isinstance(data, QMRecord):
            data = data.pack()

        rec_id = rec_id if isinstance(rec_id, str) else str(rec_id)
        if msg_type not in [SrvrWrite, SrvrWriteu]:
            raise Exception, "Message Type is not valid for record write"
        
        if len(rec_id) < 1 and len(rec_id) > 255:
            raise Exception, "Record ID needs to be betwee 1 and 255 characters"

        msg_data = pack("=hh%is%is"%(len(rec_id), len(data)), fno, len(rec_id), rec_id, data)
        ret = self._message_pair(QMMessage(msg_type, msg_data))
        if ret is False:
            raise Exception, "Error while writing to server."

        if ret.in_error == SV_ON_ERROR:
            raise Exception, "Server aborted writing record: %s" % ret.in_error_text

        return (True, ret.in_error)



    def _get_response(self, q_msg):
        (msg, p_len, srvr_e, status) = self._read_packet()
        q_msg.in_status = status
        q_msg.in_data = msg
        q_msg.in_len = p_len
        q_msg.in_error = srvr_e

        self.status = srvr_e

        if q_msg.in_error == SV_ON_ERROR:
            q_msg.in_error_text = q_msg.in_data

        if q_msg.in_error == SV_ERROR:
            tmp_msg = QMMessage(SrvrGetError, "")
            self._write_packet(tmp_msg)
            (msg, p_len, srvr_e, status) = self._read_packet()
            q_msg.in_error_text = msg

        return q_msg

    def _message_pair(self, q_msg):
        if self._write_packet(q_msg):
            q_msg = self._get_response(q_msg)
            return q_msg

        return False

    def _write_packet(self, q_msg):
        p_data = q_msg.get_header()
        totalsent = 0
        while totalsent < len(p_data):
            sent = self.socket.send(p_data[totalsent:])
            if sent == 0:
                raise RuntimeError("socket connection broken")
            totalsent = totalsent + sent

        totalsent = 0
        while totalsent < len(q_msg.out_data):
            sent = self.socket.send(q_msg.out_data[totalsent:])
            if sent == 0:
                raise RuntimeError("socket connection broken")
            totalsent = totalsent + sent

        return True

    def _read_packet(self):
        totalrcvd = 0
        msg = ''
        buff_len = 0
        while buff_len < 10: # header length
            chunk = self.socket.recv(10)
            msg = msg + chunk
            buff_len = buff_len + len(chunk)
        (p_len, srvr_e, status) = unpack("=lhl", msg[:10])
        p_len = p_len - 10
        msg = msg[10:]
        totalrcvd = 0

        while p_len > totalrcvd:
            chunk = self.socket.recv(p_len)
            msg = msg + chunk
            totalrcvd = totalrcvd + len(chunk)

        return (msg, p_len, srvr_e, status)
