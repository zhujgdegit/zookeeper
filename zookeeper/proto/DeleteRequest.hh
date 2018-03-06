// File generated by hadoop record compiler. Do not edit.

#ifndef DeleteRequest_HH_
#define DeleteRequest_HH_

#include "Efc.hh"
#include "../../jute/inc/ERecord.hh"
#include "../../jute/inc/EBinaryInputArchive.hh"
#include "../../jute/inc/EBinaryOutputArchive.hh"
#include "../../jute/inc/ECsvOutputArchive.hh"

namespace efc {
namespace ezk {

class DeleteRequest: public ERecord {
public:
	EString path;
	int version;

	DeleteRequest() {
	}
	DeleteRequest(EString path, int version) {
		this->path = path;
		this->version = version;
	}
	EString getPath() {
		return path;
	}
	void setPath(EString m_) {
		path = m_;
	}
	int getVersion() {
		return version;
	}
	void setVersion(int m_) {
		version = m_;
	}
	virtual void serialize(EOutputArchive* a_, const char* tag)
			THROWS(EIOException) {
		a_->startRecord(this, tag);
		a_->writeString(path, "path");
		a_->writeInt(version, "version");
		a_->endRecord(this, tag);
	}
	virtual void deserialize(EInputArchive* a_, const char* tag)
			THROWS(EIOException) {
		a_->startRecord(tag);
		path = a_->readString("path");
		version = a_->readInt("version");
		a_->endRecord(tag);
	}
	virtual EString toString() {
		try {
			EByteArrayOutputStream s;
			ECsvOutputArchive a_(&s);
			a_.startRecord(this, "");
			a_.writeString(path, "path");
			a_.writeInt(version, "version");
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
		DeleteRequest* peer = dynamic_cast<DeleteRequest*>(peer_);
		if (!peer) {
			throw EClassCastException(__FILE__, __LINE__,
					"Comparing different types of records.");
		}
		int ret = 0;
		ret = path.compareTo(&peer->path);
		if (ret != 0)
			return ret;
		ret = (version == peer->version) ?
				0 : ((version < peer->version) ? -1 : 1);
		if (ret != 0)
			return ret;
		return ret;
	}
	virtual boolean equals(EObject* peer_) {
		DeleteRequest* peer = dynamic_cast<DeleteRequest*>(peer_);
		if (!peer) {
			return false;
		}
		if (peer_ == this) {
			return true;
		}
		boolean ret = false;
		ret = path.equals(peer->path);
		if (!ret)
			return ret;
		ret = (version == peer->version);
		if (!ret)
			return ret;
		return ret;
	}
	virtual int hashCode() {
		int result = 17;
		int ret;
		ret = path.hashCode();
		result = 37 * result + ret;
		ret = (int) version;
		result = 37 * result + ret;
		return result;
	}
	static EString signature() {
		return "LDeleteRequest(si)";
	}
};

} /* namespace ezk */
} /* namespace efc */
#endif /* DeleteRequest_HH_ */
