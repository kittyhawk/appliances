#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <string.h>
#include <sys/poll.h>


typedef int SysStatus;
#define GOOD 1
#define BAD -1
#define SUCCESS(rc) rc >= 0
#define FAILURE(rc) rc < 0

typedef unsigned int uval32;
typedef unsigned int uval;
typedef int FDType;
typedef int PortType;

struct constants {
  enum {
    verbose = 1,
    singleCon = 1
  };
} Constants;

struct arguments {
  uval pSetSize;
  uval pSetNum;
} Args;

void
handleError(int num, SysStatus rc)
{
    fprintf(stderr, "iod: %d: Got an unxpected rc=%d\n", num, rc);
}

int
assertFDState(FDType fd, int state)
{
    struct pollfd pollFDStruct;
    int rc;

    pollFDStruct.fd = fd;
    pollFDStruct.events = state;
    pollFDStruct.revents = 0;

    rc = poll(&pollFDStruct, 1, 0);

    if (rc == -1) {
        fprintf(stderr, "iod: Error: poll on fd=%d failed errno=%d\n", fd, errno);
        return 0;
    }

    if ( pollFDStruct.revents & state ) {
        return 1;
    }
    printf("iod: assertFDState Failed: state = %d != %d: ", state,
           pollFDStruct.revents);
    if (pollFDStruct.revents & POLLNVAL)
        printf("iod:  POLLNVAL: Invalid polling request");
    if (pollFDStruct.revents & POLLHUP)
        printf("iod:  POLLHUP: Hung up");
    if (pollFDStruct.revents & POLLERR)
        printf("iod:  POLLERR: Error condition");
    printf("iod: \n");

    return 0;
}

uval
setupListenSock(FDType *fd, PortType *port)
{
    struct sockaddr_in name;
    int one = 1;
    FDType fd_listen;
    socklen_t addrLen = sizeof(struct sockaddr);
    int rc;

    fd_listen = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (fd_listen < 0) {
        fprintf(stderr, "iod: Error: server socket create failed (%d)\n", errno);
        exit(1);
    }

    rc = setsockopt(fd_listen, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    if (rc != 0) {
        fprintf(stderr, "iod: setsockopt(SO_REUSEADDR): %s\n", strerror(errno));
        exit(1);
    }

    name.sin_family = AF_INET;
    name.sin_addr.s_addr = htonl(INADDR_ANY);
    if (*port) name.sin_port = htons(*port);
    else name.sin_port = 0;

    rc = bind(fd_listen, (struct sockaddr *)&name, addrLen);
    if (rc < 0) {
        fprintf(stderr, "iod: Error: server bind failed (%d)\n", errno);
        close (fd_listen);
        exit(1);
    }

    if (*port == 0) {
        rc = getsockname(fd_listen, (struct sockaddr *)&name, &addrLen);
        if (rc < 0) {
            fprintf(stderr, "iod: Error server getsocketname failed (%d)\n", errno);
            close(fd_listen);
            exit(1);
        }
        *port = ntohs(name.sin_port);
    }
    *fd = fd_listen;
    return 1;
}

class Msg {
  // modeled from CioStream.h DataStream.h  
  // prefix is common for the moment would
  //  have to move to children classes if this changes
protected:
  struct {
    uval32 type;
    uval32 version;
    uval32 target;
    uval32 len;
    uval32 jobid;
  } prefix;

  uval32 protocolVer;
  char *data;
  uval32 dataLen;
  uval32 myVersion;
  uval32 versionMsgLen;
  uval32 versionMsgType;

  void resetdata() { if (data) free(data); data=NULL; dataLen=0; }
  void setPrefix(uval32 type, uval32 jobid, uval32 len);
  virtual SysStatus handleVersionMessage(FDType fd) = 0;
  SysStatus handleVersionMessage(FDType fd, uval32 doTimeOut, uval32 sendResp);
  SysStatus dummyHandleMessage(FDType, uval32 type);
  SysStatus sendResult(FDType fd, uval32 resType, uval32 type, uval32 result,
		       uval32 value);
  virtual SysStatus sendResult(FDType fd, uval32 type, uval32 result, 
			       uval32 value)=0;
public:
  Msg() : data(0), dataLen(0) {}
  SysStatus loadPrefix(FDType fd);
  virtual SysStatus process(FDType fd)=0;
  void print();  
};

class LoginMessage {
  enum {
    // copied from CioStream.h
    MAXIMUM_GROUPS=32,
    M_SMP_MODE = 0,
    M_VIRTUAL_NODE_MODE = 1,
    M_DUAL_MODE = 2
  };
  // This is not complete only record
  // the ones we care about to fake out the system
  uval32 jobMode;
public:
  LoginMessage(): jobMode(M_SMP_MODE) {}  
  uval32 maxGroups() { return MAXIMUM_GROUPS; }
  void setJobMode(uval32 v) { jobMode = v; }
  uval32 getJobMode() { return jobMode; }
  uval32 numCPU() { 
    if (jobMode == M_SMP_MODE) return 1;
    if (jobMode == M_VIRTUAL_NODE_MODE) return 4;
    else return 2;
  }
} loginMessage;

class CtrlMsg : public Msg {
  // copied from CioStream.h
  enum { 
    VERSION_MSG_ENC_LEN=64, 
    CIO_VERSION_LATEST=22,
    CTRL_STREAM_PORT=7000,
  };
  
  enum MessageType               // sender         action
    {                              // ------   --------------------
      VERSION_MESSAGE,            // ctlsys   get protocol version
      LOGIN_MESSAGE,              // ctlsys   identify user
      LOAD_MESSAGE,               // ctlsys   load job
      START_MESSAGE,              // ctlsys   start job
      KILL_MESSAGE,               // ctlsys   signal job
      NODE_STATUS_MESSAGE,        // ciod     job status has changed
      START_TOOL_MESSAGE,         // ctlsys   start external tool process
      RESULT_MESSAGE,             // ciod     report result of previous message
      END_MESSAGE,                // ctlsys   end job
      RECONNECT_MESSAGE,          // ctlsys   reconnect to running job
      END_TOOL_MESSAGE,           // ctlsys   end external tool process
      NODE_LOCATION_MESSAGE,      // jobd     report location of node
      LAST_MESSAGE,               // invalid  message used to mark end of
                                  //  valid messages
    };

   enum NodeStatus {
      STATUS_UNKNOWN,             //!< Compute node status is unknown.
      STATUS_LOADED,              //!< Compute node had application loaded.
      STATUS_KILLED,              //!< Compute node ended by signal.
      STATUS_EXITED,              //!< Compute node ended normally.
      STATUS_ERROR,               //!< Compute node kernel detected an error.
   };
   
  virtual SysStatus handleVersionMessage(FDType fd);  
  SysStatus handleLoginMessage(FDType fd);
  virtual SysStatus sendResult(FDType fd, uval32 type, uval32 result, 
			       uval32 value)
  {
    return Msg::sendResult(fd, RESULT_MESSAGE, type, result, value);
  }
  SysStatus sendNodeStatus(FDType fd, uval32 cpu, uval32 p2p, NodeStatus stat,
			   uval32 val);
  SysStatus handleKillMessage(FDType fd);
public:
  CtrlMsg() { 
    myVersion = CIO_VERSION_LATEST; 
    versionMsgLen = VERSION_MSG_ENC_LEN;
    versionMsgType = VERSION_MESSAGE;
  }
  SysStatus process(FDType fd);
};

class Stream {
  static uval debugWriteFlg;

  static void debugWrite(int fd, char *buf, int size) {
    fprintf(stderr, "iod: debugWrite(fd=%d, buf=0x%x, size=%d):\n", 
	    fd, buf, size);
    for (int i=0; i<size; i++) {
      printf("iod:  buf[%d]=0x%02X\n", i, (unsigned char)buf[i]);
    }
  }

public:
  static void setDebugWriteOn(void) { debugWriteFlg=1; }
  static void setDebugWriteOff(void) { debugWriteFlg=0; }
  static void Init(void) { setDebugWriteOff(); }

#if 0
  static unsigned long ntohl(int x)
  {
    unsigned long n;
    unsigned char *p;
    
    n = x;
    p = (unsigned char*)&n;
    return (p[0]<<24)|(p[1]<<16)|(p[2]<<8)|p[3];
  }
  
  static unsigned long htonl(unsigned long h)
  {
    unsigned long n;
    unsigned char *p;
    
    p = (unsigned char*)&n;
    p[0] = h>>24;
    p[1] = h>>16;
    p[2] = h>>8;
    p[3] = h;
    return n;
  }
#endif
  static int readBytes(FDType fd, void *buf, int bytes) {
    int r,n;

    r=0;
    while (bytes) {
      n = read(fd, &(((char *)buf)[r]), bytes);
      if (n==0 || n==-1) {
	fprintf(stderr, "iod: Stream::readBytes: ERROR on read\n");
	return -1;
      }
      bytes -= n;
      r += n;
    }
    return r;
  }

  static int writeBytes(FDType fd, const void *buf, int bytes) {
    if (debugWriteFlg) {
      debugWrite(fd, (char *)buf, bytes);
    }
    return write(fd, buf, bytes);
  }

  static SysStatus  readUval32(FDType fd, uval32 *val) {
    uval32 res;
    int n;

    n = readBytes(fd, &res, sizeof(res));
    if (n!=sizeof(res)) return BAD;
    //    *val = Stream::ntohl(res);
    *val = ntohl(res);
    return GOOD;
  }

  static SysStatus writeUval32(FDType fd, uval32 val) {
    // val = Stream::htonl(val);
    val = htonl(val);
    if (writeBytes(fd, &val, sizeof(val))!=sizeof(val)) return BAD;
    return GOOD;
  }

  static SysStatus  readArray(FDType fd, void **arr, uval32 *size) {
    if (FAILURE(readUval32(fd, size))) return BAD;
    if (*size < 0) return BAD;
    *arr = malloc(*size);
    if (*arr==NULL) return BAD;
    if (readBytes(fd, *arr, *size) != *size) {
      free(*arr);
      return BAD;
    }
    return GOOD;
  }

  static SysStatus writeArray(FDType fd, const void *arr, uval32 size) {
    if (FAILURE(writeUval32(fd, size))) return BAD;
    if (writeBytes(fd, arr, size) != size) return BAD;
    return GOOD;
  }
  
  static SysStatus terminateResponse(FDType fd) {return 1;}
};

uval Stream::debugWriteFlg=0;

void
Msg::print()
{
  printf("iod: prefix: type=%d version=%d, target=0x%x len=%d jobid=0x%x\n",
	 prefix.type, prefix.version, prefix.target, prefix.len, 
	 prefix.jobid);
}

SysStatus 
Msg::loadPrefix(FDType fd) 
{
    int bytes;
    int rc;
   
    rc = Stream::readBytes(fd, &prefix, sizeof(prefix));
    if (rc != sizeof(prefix)) return BAD;
    return GOOD;
}

void
Msg::setPrefix(uval32 type, uval32 jobid, uval32 len)
{
#if 0
  prefix.type = Stream::htonl(type);
  prefix.version = Stream::htonl(protocolVer);
  prefix.target = 0;
  prefix.len = Stream::htonl(len);
  prefix.jobid = jobid;
#else
  prefix.type = htonl(type);
  prefix.version = htonl(protocolVer);
  prefix.target = 0;
  prefix.len = htonl(len);
  prefix.jobid = jobid;
#endif
}

SysStatus 
Msg::handleVersionMessage(FDType fd, uval doTimeOut, uval sendResp)
{
  SysStatus rc=GOOD;
  uval32  tmout;

  if (FAILURE(Stream::readUval32(fd, &protocolVer))) {
    rc = BAD;
    goto done;
  }
  printf("iod:   version=%d\n", protocolVer);
  if (protocolVer != myVersion) {
    rc = BAD;
    goto done;
  }

  if (doTimeOut) {
    if (FAILURE(Stream::readUval32(fd, &tmout))) {
      rc = BAD;
      goto done;
    }
    printf("iod:   tmout=%d\n", tmout);
  }

  if (FAILURE(Stream::readArray(fd, (void **)&data, &dataLen))) {
    rc = BAD;
      goto done;
  }
  
  printf("iod:   datalen=%d\n", dataLen);
  if (dataLen != versionMsgLen) {
    rc = BAD;
    goto done;
  }

  if (sendResp) {
    // send version message back as acknowledgment
    memset(data, '\0', versionMsgLen);
    
    setPrefix(versionMsgType, prefix.jobid,
	    sizeof(uval32)*3 + versionMsgLen);
    
    printf("iod: Msg::handleVersionMessage: sending: ");
    print();
    
    if (FAILURE(Stream::writeBytes(fd, &prefix, sizeof(prefix)))) {
      fprintf(stderr, "iod: ERROR: send of Prefix failed\n");
      rc = BAD;
      goto done; 
    } 
    if (FAILURE(Stream::writeUval32(fd, protocolVer))) {
      fprintf(stderr, "iod: ERROR: send of cioProtocolVer failed\n");
      rc = BAD;
      goto done;
    }
    if (FAILURE(Stream::writeUval32(fd, tmout))) {
      fprintf(stderr, "iod: ERROR: send of timeout failed\n");
      rc = BAD;
      goto done;
    }
    if (FAILURE(Stream::writeArray(fd, data, versionMsgLen))) {
      fprintf(stderr, "iod: ERROR: send of data failed\n");
      rc = BAD;
      goto done;
    }
    printf("iod: sent Version Message back as ACK\n");
  }
 done:
  return rc;
}

SysStatus 
Msg::sendResult(FDType fd, uval32 resType, uval32 type, uval32 result,
		uval32 value)
{
    setPrefix(resType, prefix.jobid,
	    sizeof(uval32)*3);
    printf("iod: CtrlMsg::sendResult: sending: ");
    print();

    if (FAILURE(Stream::writeBytes(fd, &prefix, sizeof(prefix)))) {
      fprintf(stderr, "iod: ERROR: send of Prefix failed\n");
      return BAD; 
    } 
    if (FAILURE(Stream::writeUval32(fd, type))) {
      fprintf(stderr, "iod: ERROR: send of type=%d failed\n", type);
      return BAD;
    }
    if (FAILURE(Stream::writeUval32(fd, result))) {
      fprintf(stderr, "iod: ERROR: send of result=%d failed\n", result);
      return BAD;
    }
    if (FAILURE(Stream::writeUval32(fd, value))) {
      fprintf(stderr, "iod: ERROR: send of value=%d failed\n", value);
      return BAD;
    }
    return GOOD;
}

SysStatus 
CtrlMsg::handleVersionMessage(FDType fd)
{
  return Msg::handleVersionMessage(fd, 1, 1);
}


SysStatus
CtrlMsg::handleLoginMessage(FDType fd)
{
   SysStatus rc=GOOD;
   char *homedir=NULL;
   uval32 tmp, ngroups;

   printf("iod:   job:\n");

  if (FAILURE(Stream::readUval32(fd, &tmp))) {
    rc = BAD;
    goto done;
  }
  printf("iod:     uid=%d\n", tmp);

  if (FAILURE(Stream::readUval32(fd, &tmp))) {
    rc = BAD;
    goto done;
  }
  printf("iod:     gid=%d\n", tmp);

  if (FAILURE(Stream::readUval32(fd, &tmp))) {
    rc = BAD;
    goto done;
  }
  printf("iod:     umask=%d\n", tmp);

  if (FAILURE(Stream::readArray(fd, (void **)&homedir, &tmp))) {
    rc = BAD;
    goto done;
  }
  printf("iod:     homedir=%s\n", homedir);

  if (FAILURE(Stream::readUval32(fd, &ngroups))) {
    rc = BAD;
    goto done;
  }
  printf("iod:     ngroups=%d\n", ngroups);

  if (ngroups > loginMessage.maxGroups()) {
    fprintf(stderr, "ERROR: ngroups=%d > MAX_GROUPS=%d\n", loginMessage.maxGroups());
    goto done;
  }
  for (uval i=0; i<ngroups; i++) {
    if (FAILURE(Stream::readUval32(fd, &tmp))) {
      rc = BAD;
      goto done;
    }
    printf("iod:       group[%d]=%d\n", i, tmp);
  }

  if (FAILURE(Stream::readUval32(fd, &tmp))) {
    rc = BAD;
    goto done;
  }
  printf("iod:     id=0x%x\n", tmp);

  if (FAILURE(Stream::readUval32(fd, &tmp))) {
    rc = BAD;
    goto done;
  }
  printf("iod:     mode=%d\n", tmp);
  loginMessage.setJobMode(tmp);

  rc=sendResult(fd, LOGIN_MESSAGE, 0, 0);
 done:
  if (homedir) free(homedir);
  return rc;
}

SysStatus 
CtrlMsg::sendNodeStatus(FDType fd, uval32 cpu, uval32 p2p, NodeStatus stat, 
			uval32 val)
{
    setPrefix(NODE_STATUS_MESSAGE, prefix.jobid,
	    sizeof(uval32)*4);

    //    printf("iod: CtrlMsg::sendNodeStatus: sending: cpu=%d p2p=%d stat=%d val=%d\n",
    //	   cpu, p2p, stat, val);
    //    print();

    if (FAILURE(Stream::writeBytes(fd, &prefix, sizeof(prefix)))) {
      fprintf(stderr, "iod: ERROR: send of Prefix failed\n");
      return BAD; 
    } 
    if (FAILURE(Stream::writeUval32(fd, cpu))) {
      fprintf(stderr, "iod: ERROR: send of cpu=%d failed\n", cpu);
      return BAD;
    }
    if (FAILURE(Stream::writeUval32(fd, p2p))) {
      fprintf(stderr, "iod: ERROR: send of p2p=%d failed\n", p2p);
      return BAD;
    }
    if (FAILURE(Stream::writeUval32(fd, stat))) {
      fprintf(stderr, "iod: ERROR: send of stat=%d failed\n", stat);
      return BAD;
    }
    if (FAILURE(Stream::writeUval32(fd, val))) {
      fprintf(stderr, "iod: ERROR: send of val=%d failed\n", val);
      return BAD;
    }
    return GOOD;
}


SysStatus 
CtrlMsg::handleKillMessage(FDType fd)
{
  uval32 signo;
  NodeStatus stat;
  SysStatus lrc;
  SysStatus rc = GOOD;

  if (FAILURE(Stream::readUval32(fd, &signo))) {
    rc = BAD;
    goto done;
  }

  printf("iod: handleKillMessage: got signo=%d\n", signo);
  if (signo == 9) {
    stat = STATUS_KILLED;
    printf("iod: node Status to send is KILLED (%d)\n", stat); 
  } else if (signo == 15) {
    stat = STATUS_EXITED;
    printf("iod: node Status to send is EXITED (%d)\n", stat);
  } else {
    stat = STATUS_ERROR;
    printf("iod: node Status to send is ERROR (%d)\n", stat);
  }
  
  printf("iod: sending node statuses for psetnum=%d and psetsize=%d\n",
	 Args.pSetNum, Args.pSetSize);
  //Stream::setDebugWriteOn();
  for (int node=0; node < Args.pSetSize; node++) {
    for (int cpu=0; cpu<loginMessage.numCPU(); cpu++) { 
      lrc=sendNodeStatus(fd, cpu, node + Args.pSetNum * Args.pSetSize, 
			 stat, 0); 
      if (FAILURE(lrc)) { 
	fprintf(stderr, "iod: ERROR: sendNodeStatus(%d, %d, %d, 0)=%d\n",
		cpu, node + Args.pSetNum * Args.pSetSize, stat, 
		rc); 
	rc=BAD; 
      }
    } 
  } 
  done:
  return rc;
} 

SysStatus 
Msg::dummyHandleMessage(FDType fd, uval32 type)
{
  data = (char *)malloc(prefix.len);
  if (data==0) {
    fprintf(stderr, "iod: Msg::dummyHandleMessage malloc faild\n");
    return BAD;
  }

  dataLen=Stream::readBytes(fd, data, prefix.len);
  if (prefix.len!=dataLen){
    fprintf(stderr, "iod: MSg::dummyHandleMessage readBytes "
	    "prefix.len=%d!=dataLen=%d\n",
	   prefix.len, dataLen);
    return BAD;
  }

  return sendResult(fd, type, 0, 0);
}

SysStatus 
CtrlMsg::process(FDType fd) 
{
  SysStatus rc=GOOD;

  print();
  switch(prefix.type) {
      case VERSION_MESSAGE: 
	printf("iod: VERSION_MESSAGE: received on CtrlStream\n");
	rc = handleVersionMessage(fd);
	break;
      case LOGIN_MESSAGE:
	printf("iod: LOGIN_MESSAGE: received on CtrlStream\n");
	rc = handleLoginMessage(fd);
	//rc = dummyHandleMessage(fd, LOGIN_MESSAGE);
	break;
      case LOAD_MESSAGE:
	printf("iod: LOAD_MESSAGE: received on CtrlStream\n");
	rc = dummyHandleMessage(fd, LOAD_MESSAGE);
	break;
      case START_MESSAGE:
	printf("iod: START_MESSAGE: received on CtrlStream\n");
	rc = dummyHandleMessage(fd, START_MESSAGE);
	break;
      case KILL_MESSAGE:
	printf("iod: KILL_MESSAGE: received on CtrlStream\n");
	rc = handleKillMessage(fd);
	//	rc = dummyHandleMessage(fd, START_MESSAGE);
	break;
      case END_MESSAGE:
	printf("iod: END_MESSAGE: received on CtrlStream\n");
        dummyHandleMessage(fd, END_MESSAGE);
	// force connection to close here in response
	// END_MESSAGE by explicity returning failure
	rc = BAD;
	break;
      default:
	printf("iod: UNSUPPORTED Message received on CtrlStream\n");
	rc = BAD;
  }
  resetdata();
  return rc;
}

class DataMsg : public Msg {
  // copied from DataStream.h
  enum { 
    DATA_STREAM_PORT=8000,
    VERSION_MSG_ENC_LEN=64,
    DATA_VERSION_LATEST=3
  };
  
   enum MessageType               // sender         action
   {                              // ------   --------------------
      VERSION_MESSAGE,            // ctlsys   get protocol version
      STDIN_MESSAGE,              // ctlsys   send stdin data
      STDOUT_MESSAGE,             // ciod     send stdout data
      STDERR_MESSAGE,             // ciod     send stderr data
      GENERAL_MESSAGE,            // mpirun   send control data
      TRACE_MESSAGE,              // ctlsys   send trace data
      LAST_MESSAGE
   };
  
  virtual SysStatus sendResult(FDType fd, uval32 type, uval32 result, 
			       uval32 value)
  {
    return GOOD;
  }
  SysStatus handleVersionMessage(FDType fd);
public:
  DataMsg() {
    myVersion = DATA_VERSION_LATEST; 
    versionMsgLen = VERSION_MSG_ENC_LEN;
    versionMsgType = VERSION_MESSAGE;
  }
  SysStatus process(FDType fd);
};


SysStatus 
DataMsg::handleVersionMessage(FDType fd)
{
  return Msg::handleVersionMessage(fd, 0, 0);
}

SysStatus 
DataMsg::process(FDType fd) 
{
  SysStatus rc=GOOD;

  printf("iod: DataMsg::process: Got prefix: ");
  print();
  switch(prefix.type) {
      case VERSION_MESSAGE:
	printf("iod: VERSION_MESSAGE: received on DataStream\n");  
	rc = handleVersionMessage(fd);
	break; 
      case STDIN_MESSAGE:
	printf("iod: STDIN_MESSAGE: received on DataStream\n"); 
	break; 
      case STDOUT_MESSAGE:
	printf("iod: STDOUT_MESSAGE: received on DataStream\n");
	break;
      case STDERR_MESSAGE:
	printf("iod: STDERR_MESSAGE: received on DataStream\n");
	break;
      case GENERAL_MESSAGE:
	printf("iod: GENERAL_MESSAGE: received on DataStream\n");
	break; 
      case TRACE_MESSAGE:
	printf("iod: TRACE_MESSAGE: received on DataStream\n");
	break;
      default:
	printf("iod: UNSUPPORTED Message received on DataStream\n");
  }
  resetdata();
  return rc;
}

class Connection {
protected:
    FDType fd;
public:
    virtual uval process() = 0;
    virtual void init(FDType fd) {
        printf("iod: Connection %p: inited for fd=%d\n", this, fd);
        this->fd = fd;
    }
    virtual void close() = 0;
};

class CtrlConnection : public Connection {
  CtrlMsg msg;
public:
  virtual void init(FDType fd) {
    Connection::init(fd);
  }
  
  virtual void close() {
    delete this;
  }

  virtual uval process() {
    SysStatus rc = msg.loadPrefix(fd);
    if (SUCCESS(rc)) {
      rc = msg.process(fd);
      if (SUCCESS(rc)) {
	rc = Stream::terminateResponse(fd);
	if (SUCCESS(rc)) return 1;
      }
      return 0;
    }
    handleError(0, rc);
    return 0;
  }

};


class DataConnection : public Connection {
  DataMsg msg;
public:
  virtual void init(FDType fd) {
    Connection::init(fd);
  }
  
  virtual void close() {
    delete this;
  }

  virtual uval process() {
    SysStatus rc = msg.loadPrefix(fd);
    if (SUCCESS(rc)) {
      rc = msg.process(fd);
      if (SUCCESS(rc)) {
	rc = Stream::terminateResponse(fd);
	if (SUCCESS(rc)) return 1;
      }
      return 0;
    }
    handleError(0, rc);
    return 0;
  }
};

class Connections {
    enum {MAX_CON=1000};
    static Connection *cons[MAX_CON];
    static uval num;
    static FDType firstConFD;

    static uval fdToConIdx(FDType fd) {
        if (fd < firstConFD) {
            fprintf(stderr, "iod: oops fd out of range for a connection\n");
            exit(1);
        }
        if ((fd - firstConFD) >= MAX_CON) {
            fprintf(stderr, "iod: oops more connections than I can handle\n");
            return 0;
        }
        return fd - firstConFD;
    }

public:
    static void init(FDType lastListenFD) {
        num = 0;
        firstConFD = lastListenFD + 1;
        for (uval i=0; i<MAX_CON; i++) cons[i]=0;
    }

    static uval openConnectionForFD(Connection * con, FDType fd) {
        uval newConIdx = fdToConIdx(fd);

        if (cons[newConIdx] != NULL) {
            fprintf(stderr, "iod: oops a connection object exists already"
                    "for conIdx=%d fd=%d\n", newConIdx, fd);
            exit(1);
        }
        con->init(fd);
        cons[newConIdx] = con;
        num++;
        return 1;
    }

    static void closeConnectionForFD(FDType fd) {
        uval conIdx = fdToConIdx(fd);
        if (cons[conIdx] != NULL) {
            cons[conIdx]->close();
            cons[conIdx] = NULL;
            num--;
        }
    }

    static uval processConnectionForFD(FDType fd) {
        uval conIdx = fdToConIdx(fd);
        Connection *con = cons[conIdx];
        if (con == NULL) {
            fprintf(stderr, "iod: oops no open connection for fd=%d\n", fd);
            exit(1);
        }
	
        if (assertFDState(fd, POLLIN) == 0  || con->process() == 0) {
            closeConnectionForFD(fd);
            return 0;
        }
        return 1;
    }
};

FDType Connections::firstConFD;
uval Connections::num;
Connection *Connections::cons[Connections::MAX_CON];

void
acceptConn(FDType fd_listen, int *max_fd, fd_set *orig_fds, Connection *con)
{
    FDType new_fd;
    new_fd = accept(fd_listen, NULL, NULL);
    if (new_fd < 0) {
        fprintf(stderr, "iod: Error: server accept failed (%d)\n", errno);
    } else {
        if (Connections::openConnectionForFD(con, new_fd)) {
            if (new_fd > *max_fd) *max_fd = new_fd;
            FD_SET(new_fd, orig_fds);
        }
    }
}

void
touchPidFile(void)
{
  int fd,n;
  pid_t pid=getpid();
  char pidstr[80];
  
  bzero(pidstr,80);
  snprintf(pidstr, 80, "%d", pid);
  fd = open("/tmp/iod.pid", 
	    O_CREAT|O_WRONLY|O_TRUNC,
	    S_IWUSR|S_IRUSR|S_IRGRP|S_IROTH);
  if (fd) {
    n = write(fd, pidstr, strlen(pidstr));
    if (n!=strlen(pidstr)) {
      fprintf(stderr, "iod: ERROR: touchPidFile write failed n=%d!=strlen(pidstr)=%d" 
	      " pidstr=%s\n", pidstr);
    }
  } else {
    fprintf(stderr, "iod: ERROR: touchPidFile open /tmp/iod.pid failed\n");
  }
  close(fd);
}

void
serverLoop()
{
  fd_set fds, orig_fds;
  FDType fd_ctrllisten, fd_datalisten;
  int max_fd;
  int rc;
  PortType port;

  port = 7000;
  setupListenSock(&fd_ctrllisten, &port);
  rc = listen(fd_ctrllisten, 8);
  if (rc < 0) {
    fprintf(stderr, "iod: Error: server listen failed (%d)\n", errno);
    exit(1);
  }
  printf("iod: Listening on Control Port = %d\n", port);

  port = 8000;
  setupListenSock(&fd_datalisten, &port);
  rc = listen(fd_datalisten, 8);
  if (rc < 0) {
    fprintf(stderr, "iod: Error: server listen failed (%d)\n", errno);
    exit(1);
  }
  printf("iod: Listening on Data Port = %d\n", port);

  FD_ZERO(&orig_fds);
  FD_SET(fd_ctrllisten, &orig_fds);
  FD_SET(fd_datalisten, &orig_fds);

  max_fd = fd_datalisten;

  Connections::init(max_fd);

  // Let otheres know we are alive and accepting
  // connections
  touchPidFile();

  while (1) {

    fds = orig_fds;

    rc = select(max_fd+1, &fds, NULL, NULL, NULL);

    if (rc < 0) {
      fprintf(stderr, "iod: Error: server select failed (%d)\n", errno);
      perror("iod: select");
      exit(1);
    }

    if (rc == 0) {
      fprintf(stderr, "iod: What Select timed out\n");
      exit(1);
    } else {
        if (FD_ISSET(fd_ctrllisten, &fds)) {
            // got a new ctrl connection
            printf("iod: got an ctrl connection on %d\n", fd_ctrllisten);
            acceptConn(fd_ctrllisten, &max_fd, &orig_fds, new CtrlConnection);
            rc--;
            FD_CLR(fd_ctrllisten, &fds);
	    if (Constants.singleCon) { 
	      FD_CLR(fd_ctrllisten, &orig_fds);
	      close(fd_ctrllisten);
	      FDType tmpfd = open("/dev/null",  O_RDONLY);
	      if (tmpfd != fd_ctrllisten) {
		printf("iod: yikes tmpfd = %d != fd_ctrllisten =%d\n", tmpfd,
		       fd_ctrllisten);
		exit(-1);
	      }
	    }
        }
        if (FD_ISSET(fd_datalisten, &fds)) {
            // got a new data connection
            printf("iod: got an data connection on %d\n", fd_datalisten);
            acceptConn(fd_datalisten, &max_fd, &orig_fds,
                       new DataConnection);
            rc--;
            FD_CLR(fd_datalisten, &fds);
	    if (Constants.singleCon) {
	      FD_CLR(fd_datalisten, &orig_fds);
	      close(fd_datalisten);
	      FDType tmpfd = open("/dev/null",  O_RDONLY);
	      if (tmpfd != fd_datalisten) {
		printf("iod: yikes tmpfd = %d != fd_datalisten =%d\n", tmpfd, 
		       fd_datalisten);
		exit(-1);
	      }
	    }
        }
      FDType i = 0;
      while (rc > 0 && i <= max_fd) {
          if (FD_ISSET(i, &fds)) {
              printf("iod: Found activity on fd=%d\n", i);
              if (Connections::processConnectionForFD(i)==0) {
		printf("iod: Closing server side of connection\n");
		FD_CLR(i, &orig_fds);
		close(i);
              }
          } 
          i++;
      }

    }
  }
}

int 
main(int argc, char **argv)
{
  
  close(STDIN_FILENO);
  open("/dev/null", O_RDWR);
  close(STDOUT_FILENO);
  open("/dev/console", O_WRONLY);
  close(STDERR_FILENO);
  dup2(STDOUT_FILENO, STDERR_FILENO);

  if (argc!=3) {
    fprintf(stderr, "iod: %s: <psetNum> <psetSize>\n", argv[0]);
    exit(-1);
  }

  Args.pSetNum = atoi(argv[1]);
  Args.pSetSize = atoi(argv[2]);

  printf("iod: Started with Args.pSetNum=%d Args.pSetSize=%d\n",
	 Args.pSetNum, Args.pSetSize);

  Stream::Init();

  serverLoop();
}
