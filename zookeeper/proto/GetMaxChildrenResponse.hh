// File generated by hadoop record compiler. Do not edit.

#ifndef GetMaxChildrenResponse_HH_
#define GetMaxChildrenResponse_HH_

#include "Efc.hh"
#include "../../jute/inc/ERecord.hh"
#include "../../jute/inc/EBinaryInputArchive.hh"
#include "../../jute/inc/EBinaryOutputArchive.hh"
#include "../../jute/inc/ECsvOutputArchive.hh"

namespace efc {
namespace ezk {

class GetMaxChildrenResponse: public ERecord {
public:
	int max;

	GetMaxChildrenResponse() {
	}
	GetMaxChildrenResponse(int max) {
		this->max = max;
	}
	int getMax() {
		return max;
	}
	void setMax(int m_) {
		max = m_;
	}
	virtual void serialize(EOutputArchive* a_, const char* tag)
			THROWS(EIOException) {
		a_->startRecord(this, tag);
		a_->writeInt(max, "max");
		a_->endRecord(this, tag);
	}
	virtual void deserialize(EInputArchive* a_, const char* tag)
			THROWS(EIOException) {
		a_->startRecord(tag);
		max = a_->readInt("max");
		a_->endRecord(tag);
	}
	virtual EString toString() {
		try {
			EByteArrayOutputStream s;
			ECsvOutputArchive a_(&s);
			a_.startRecord(this, "");
			a_.writeInt(max, "max");
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
		GetMaxChildrenResponse* peer = dynamic_cast<GetMaxChildrenResponse*>(peer_);
		if (!peer) {
			throw EClassCastException(__FILE__, __LINE__,
					"Comparing different types of records.");
		}
		int ret = 0;
		ret = (max == peer->max) ? 0 : ((max < peer->max) ? -1 : 1);
		if (ret != 0)
			return ret;
		return ret;
	}
	virtual boolean equals(EObject* peer_) {
		GetMaxChildrenResponse* peer = dynamic_cast<GetMaxChildrenResponse*>(peer_);
		if (!peer) {
			return false;
		}
		if (peer_ == this) {
			return true;
		}
		boolean ret = false;
		ret = (max == peer->max);
		if (!ret)
			return ret;
		return ret;
	}
	virtual int hashCode() {
		int result = 17;
		int ret;
		ret = (int) max;
		result = 37 * result + ret;
		return result;
	}
	static EString signature() {
		return "LGetMaxChildrenResponse(i)";
	}
};

} /* namespace ezk */
} /* namespace efc */
#endif /* GetMaxChildrenResponse_HH_ */
