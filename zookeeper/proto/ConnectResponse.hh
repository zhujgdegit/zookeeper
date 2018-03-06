// File generated by hadoop record compiler. Do not edit.

#ifndef ConnectResponse_HH_
#define ConnectResponse_HH_

#include "Efc.hh"
#include "../../jute/inc/ERecord.hh"
#include "../../jute/inc/EBinaryInputArchive.hh"
#include "../../jute/inc/EBinaryOutputArchive.hh"
#include "../../jute/inc/ECsvOutputArchive.hh"

namespace efc {
namespace ezk {

class ConnectResponse: public ERecord {
public:
	int protocolVersion;
	int timeOut;
	llong sessionId;
	sp<EA<byte> > passwd;

	ConnectResponse() {
	}
	ConnectResponse(int protocolVersion, int timeOut, llong sessionId,
			sp<EA<byte> > passwd) {
		this->protocolVersion = protocolVersion;
		this->timeOut = timeOut;
		this->sessionId = sessionId;
		this->passwd = passwd;
	}
	int getProtocolVersion() {
		return protocolVersion;
	}
	void setProtocolVersion(int m_) {
		protocolVersion = m_;
	}
	int getTimeOut() {
		return timeOut;
	}
	void setTimeOut(int m_) {
		timeOut = m_;
	}
	llong getSessionId() {
		return sessionId;
	}
	void setSessionId(llong m_) {
		sessionId = m_;
	}
	sp<EA<byte> > getPasswd() {
		return passwd;
	}
	void setPasswd(sp<EA<byte> > m_) {
		passwd = m_;
	}
	virtual void serialize(EOutputArchive* a_, const char* tag)
			THROWS(EIOException) {
		a_->startRecord(this, tag);
		a_->writeInt(protocolVersion, "protocolVersion");
		a_->writeInt(timeOut, "timeOut");
		a_->writeLLong(sessionId, "sessionId");
		a_->writeBuffer(passwd.get(), "passwd");
		a_->endRecord(this, tag);
	}
	virtual void deserialize(EInputArchive* a_, const char* tag)
			THROWS(EIOException) {
		a_->startRecord(tag);
		protocolVersion = a_->readInt("protocolVersion");
		timeOut = a_->readInt("timeOut");
		sessionId = a_->readLLong("sessionId");
		passwd = a_->readBuffer("passwd");
		a_->endRecord(tag);
	}
	virtual EString toString() {
		try {
			EByteArrayOutputStream s;
			ECsvOutputArchive a_(&s);
			a_.startRecord(this, "");
			a_.writeInt(protocolVersion, "protocolVersion");
			a_.writeInt(timeOut, "timeOut");
			a_.writeLLong(sessionId, "sessionId");
			a_.writeBuffer(passwd.get(), "passwd");
			a_.endRecord(this, "");
			s.write('\0');
			return (char*) s.data();
		} catch (EThrowable& ex) {
			ex.printStackTrace();
		}
		return "ERROR";
	}
	void write(EDataOutput* out) THROWS(EIOException) {
		EBinaryOutputArchive archive(out);
		serialize(&archive, "");
	}
	void readFields(EDataInput* in) THROWS(EIOException) {
		EBinaryInputArchive archive(in);
		deserialize(&archive, "");
	}
	virtual int compareTo(EObject* peer_) THROWS(EClassCastException) {
		ConnectResponse* peer = dynamic_cast<ConnectResponse*>(peer_);
		if (!peer) {
			throw EClassCastException(__FILE__, __LINE__,
					"Comparing different types of records.");
		}
		int ret = 0;
		ret = (protocolVersion == peer->protocolVersion) ?
				0 : ((protocolVersion < peer->protocolVersion) ? -1 : 1);
		if (ret != 0)
			return ret;
		ret = (timeOut == peer->timeOut) ?
				0 : ((timeOut < peer->timeOut) ? -1 : 1);
		if (ret != 0)
			return ret;
		ret = (sessionId == peer->sessionId) ?
				0 : ((sessionId < peer->sessionId) ? -1 : 1);
		if (ret != 0)
			return ret;
		{
			sp<EA<byte> > my = passwd;
			sp<EA<byte> > ur = peer->passwd;
			ret = compareBytes(my.get(), 0, my->length(), ur.get(), 0,
					ur->length());
		}
		if (ret != 0)
			return ret;
		return ret;
	}
	virtual boolean equals(EObject* peer_) {
		ConnectResponse* peer = dynamic_cast<ConnectResponse*>(peer_);
		if (!peer) {
			return false;
		}
		if (peer_ == this) {
			return true;
		}
		boolean ret = false;
		ret = (protocolVersion == peer->protocolVersion);
		if (!ret)
			return ret;
		ret = (timeOut == peer->timeOut);
		if (!ret)
			return ret;
		ret = (sessionId == peer->sessionId);
		if (!ret)
			return ret;
		ret = bufEquals(passwd.get(), peer->passwd.get());
		if (!ret)
			return ret;
		return ret;
	}
	virtual int hashCode() {
		int result = 17;
		int ret;
		ret = (int) protocolVersion;
		result = 37 * result + ret;
		ret = (int) timeOut;
		result = 37 * result + ret;
		ret = (int) (sessionId ^ (((ullong) sessionId) >> 32));
		result = 37 * result + ret;
		ret = EArrays::toString(passwd.get()).hashCode();
		result = 37 * result + ret;
		return result;
	}
	static EString signature() {
		return "LConnectResponse(iilB)";
	}
};

} /* namespace ezk */
} /* namespace efc */
#endif /* ConnectResponse_HH_ */
