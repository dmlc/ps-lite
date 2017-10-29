#pragma once
#include <fstream>
#include <chrono>
#include <cstdio>
#include <string>
//#include "ps/sarray.h"
#include <stdio.h>
#include <stdlib.h>
#include <execinfo.h>
#include <cxxabi.h>
#include <sstream>
#include <string>
#include <assert.h>
#include <algorithm>
#include <sstream>
#include <chrono>
#include <atomic>
#include <unordered_map>
//#include "logging.h"
#include <signal.h>
#include <cmath>
#include <stdio.h>
#include <string.h>
#include "../../../src/engine/profiler.h"
#include <sys/stat.h>
#include <string.h>
#include <fcntl.h>



template<typename T>
T atomic_fetch_add_ex(std::atomic<T> *obj, T arg)
{
	T expected = obj->load();
	while (!atomic_compare_exchange_weak(obj, &expected, expected + arg));
	return expected;
}


template<typename T>
void atomic_replace_if(std::atomic<T> *obj, T other, std::function<bool(T, T)> Predicate)
{
	while (true)
	{
		T old = obj->load();
		auto shouldSwap = Predicate(old, other);
		if (shouldSwap == false)
		{
			return;
		}
		else
		{
			if (atomic_compare_exchange_weak(obj, &old, other))
			{
				//success!
				return;
			}
		}
	}
}

template <class V>
static std::string SummarizeContinousBuffer(V* valPtr, int len, int firstK = 10)
{
	std::stringstream ss;
	ss << "Summary: " << (uint64_t)valPtr << "-" << (uint64_t)(valPtr + len - 1) <<  " Count = " << len << " (";
	for (int i = 0; i < firstK && i < len; i++)
	{
		ss << valPtr[i] << ",";
	}
	ss << ") Fingerprint:";
	uint64_t result = 0;
	auto fpPtr = (char*)valPtr;
	auto fpSz = sizeof(V) * len;
	for (int i = 0; i < fpSz; i++)
	{
		result ^= (fpPtr[i] << (8 * (i % 8)));
	}
	ss << result;
	return ss.str();
}



static std::string GetStacktraceString(unsigned int max_frames = 100)
{
	std::stringstream ss;
	ss << "stack trace:\n";

	// storage array for stack trace address data
	void* addrlist[max_frames + 1];

	// retrieve current stack addresses
	int addrlen = backtrace(addrlist, sizeof(addrlist) / sizeof(void*));

	if (addrlen == 0) {
		ss << "  <empty, possibly corrupt>\n";
		return ss.str();
	}

	// resolve addresses into strings containing "filename(function+address)",
	// this array must be free()-ed
	char** symbollist = backtrace_symbols(addrlist, addrlen);

	// allocate string which will be filled with the demangled function name
	size_t funcnamesize = 256;
	char* funcname = (char*)malloc(funcnamesize);

	// iterate over the returned symbol lines. skip the first, it is the
	// address of this function.
	for (int i = 1; i < addrlen; i++)
	{
		char *begin_name = 0, *begin_offset = 0, *end_offset = 0;

		// find parentheses and +address offset surrounding the mangled name:
		// ./module(function+0x15c) [0x8048a6d]
		for (char *p = symbollist[i]; *p; ++p)
		{
			if (*p == '(')
				begin_name = p;
			else if (*p == '+')
				begin_offset = p;
			else if (*p == ')' && begin_offset) {
				end_offset = p;
				break;
			}
		}

		if (begin_name && begin_offset && end_offset
			&& begin_name < begin_offset)
		{
			*begin_name++ = '\0';
			*begin_offset++ = '\0';
			*end_offset = '\0';

			// mangled name is now in [begin_name, begin_offset) and caller
			// offset in [begin_offset, end_offset). now apply
			// __cxa_demangle():

			int status;
			char* ret = abi::__cxa_demangle(begin_name,
				funcname, &funcnamesize, &status);
			if (status == 0) {
				funcname = ret; // use possibly realloc()-ed string
				ss << "  " << symbollist[i] << " : " << funcname << "+" << begin_offset << "\n";
			}
			else {
				// demangling failed. Output function name as a C function with
				// no arguments.
				ss << " " << symbollist[i] << " : " << begin_name << "+" << begin_offset << "\n";
			}
		}
		else
		{
			// couldn't parse the line? print the whole line.
			ss << " " << symbollist[i] << "\n";
		}
	}

	free(funcname);
	free(symbollist);
	return ss.str();
}

/*static std::string BreakHackForStream()
{
    raise(SIGTRAP);
    return "";
    }*/

static void print_stacktrace(FILE *out = stderr, unsigned int max_frames = 100)
{
	fprintf(out, "stack trace:\n");

	// storage array for stack trace address data
	void* addrlist[max_frames + 1];

	// retrieve current stack addresses
	int addrlen = backtrace(addrlist, sizeof(addrlist) / sizeof(void*));

	if (addrlen == 0) {
		fprintf(out, "  <empty, possibly corrupt>\n");
		return;
	}

	// resolve addresses into strings containing "filename(function+address)",
	// this array must be free()-ed
	char** symbollist = backtrace_symbols(addrlist, addrlen);

	// allocate string which will be filled with the demangled function name
	size_t funcnamesize = 256;
	char* funcname = (char*)malloc(funcnamesize);

	// iterate over the returned symbol lines. skip the first, it is the
	// address of this function.
	for (int i = 1; i < addrlen; i++)
	{
		char *begin_name = 0, *begin_offset = 0, *end_offset = 0;

		// find parentheses and +address offset surrounding the mangled name:
		// ./module(function+0x15c) [0x8048a6d]
		for (char *p = symbollist[i]; *p; ++p)
		{
			if (*p == '(')
				begin_name = p;
			else if (*p == '+')
				begin_offset = p;
			else if (*p == ')' && begin_offset) {
				end_offset = p;
				break;
			}
		}

		if (begin_name && begin_offset && end_offset
			&& begin_name < begin_offset)
		{
			*begin_name++ = '\0';
			*begin_offset++ = '\0';
			*end_offset = '\0';

			// mangled name is now in [begin_name, begin_offset) and caller
			// offset in [begin_offset, end_offset). now apply
			// __cxa_demangle():

			int status;
			char* ret = abi::__cxa_demangle(begin_name,
				funcname, &funcnamesize, &status);
			if (status == 0) {
				funcname = ret; // use possibly realloc()-ed string
				fprintf(out, "  %s : %s+%s\n",
					symbollist[i], funcname, begin_offset);
			}
			else {
				// demangling failed. Output function name as a C function with
				// no arguments.
				fprintf(out, "  %s : %s()+%s\n",
					symbollist[i], begin_name, begin_offset);
			}
		}
		else
		{
			// couldn't parse the line? print the whole line.
			fprintf(out, "  %s\n", symbollist[i]);
		}
	}

	free(funcname);
	free(symbollist);
}




class DIMELOG
{
private:
	std::ofstream SRVLOG;
	std::ofstream SRVLOG_OUT;
	std::ofstream SRVLOG_METRIC2;
	std::ofstream SRVLOG_METRIC3;
	std::ofstream SRVLOG_METRIC4;
	std::ofstream SRVLOG_METRIC1;
public:
	static DIMELOG* Get() {
		static DIMELOG e; return &e;
	}
	DIMELOG()
	{
		std::remove("SRVLOG.DIME");
		std::remove("SRVLOG.OUT.DIME");
		std::remove("SRVLOG.METRIC1.DIME");
		std::remove("SRVLOG.METRIC2.DIME");
		std::remove("SRVLOG.METRIC3.DIME");
		std::remove("SRVLOG.METRIC4.DIME");
		SRVLOG.open("SRVLOG.DIME", std::ios_base::out | std::ios_base::app);
		SRVLOG_OUT.open("SRVLOG.OUT.DIME", std::ios_base::out | std::ios_base::app);
		SRVLOG_METRIC1.open("SRVLOG.METRIC1.DIME", std::ios_base::out | std::ios_base::app);
		SRVLOG_METRIC2.open("SRVLOG.METRIC2.DIME", std::ios_base::out | std::ios_base::app);
		SRVLOG_METRIC3.open("SRVLOG.METRIC3.DIME", std::ios_base::out | std::ios_base::app);
		SRVLOG_METRIC4.open("SRVLOG.METRIC4.DIME", std::ios_base::out | std::ios_base::app);
	}
	void DimeLogMetric1(double item1)
	{
		SRVLOG_METRIC1 << item1 << "\n"; //do not force a flush
	}
	void DimeLogMetric2(double item1, double item2)
	{
		SRVLOG_METRIC2 << item1 << "," << item2 << "\n";
	}
	void DimeLogMetric4(double item1, double item2, double item3, double item4)
	{
		SRVLOG_METRIC4 << item1 << "," << item2 << "," << item3 << "," << item4 << "\n"; //do not force a flush
	}

	void DimeLogMetric3(double item1, double item2, double item3)
	{
		SRVLOG_METRIC3 << item1 << "," << item2 << "," << item3 << "\n"; //do not force a flush
	}
	void DimeLogOut(int myid, int keyDigest, int id, int bytes)
	{
		std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
		auto microsec = std::chrono::duration_cast<std::chrono::microseconds>(
			now.time_since_epoch())
			.count();
		//std::ofstream log_file();
		SRVLOG_OUT << microsec << "," << keyDigest << "," << myid << "," << id << "," << bytes << "\n"; //do not force a flush
	}
	void DimeLog(int myid, int keyDigest, int id, int bytes)
	{
		std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
		auto microsec = std::chrono::duration_cast<std::chrono::microseconds>(
			now.time_since_epoch())
			.count();
		SRVLOG << microsec << "," << keyDigest << "," << myid << "," << id << "," << bytes << "\n"; //do not force a flush
	}
};


#ifdef PHUB_PERF_DIAG
#define MEASURE_THIS_TIME_BEG_WITH_EXTSEQ(name,seq,start,extseq)  do {PerfMonLite::Get().LogTimeBegin(name, seq,start,extseq);}while(0);
#define MEASURE_THIS_TIME_END_WITH_EXTSEQ(name,seq,extseq) do {PerfMonLite::Get().LogTimeEnd(name, seq,extseq);}while(0);
#define MEASURE_THIS_TIME_BEG_EX(name, seq,start)  MEASURE_THIS_TIME_BEG_WITH_EXTSEQ(name,seq,start,0)  
#define MEASURE_THIS_TIME_BEG(name, seq) MEASURE_THIS_TIME_BEG_WITH_EXTSEQ(name,seq,0,0)
#define MEASURE_THIS_TIME_END(name, seq) MEASURE_THIS_TIME_END_WITH_EXTSEQ(name,seq,0)
#define TIMESTAMP_METASLIM_PTR(ms) do {PerfMonLite& pMon = PerfMonLite::Get();(ms)->PHubAdditionalPayload = pMon.GetCurrentTimestamp();} while(0);
#else
#define MEASURE_THIS_TIME_BEG_EX(name, seq,start)
#define MEASURE_THIS_TIME_BEG_WITH_EXTSEQ(name,seq,start,extseq)
#define MEASURE_THIS_TIME_END_WITH_EXTSEQ(name,seq,extseq)
#define MEASURE_THIS_TIME_BEG(name,seq)
#define MEASURE_THIS_TIME_END(name, seq)
#define TIMESTAMP_METASLIM_PTR(ms)
#endif


#ifdef PHUB_PERF_DIAG

const std::string PERFMON_WORKER_IB_SEND_PULL_POLL = "WORKER_IB_SEND_PULL_POLL"; //PHUB.h
const std::string PERFMON_WORKER_IB_SEND_PUSH_POLL = "WORKER_IB_SEND_PUSH_POLL"; //PHUB.h
const std::string PERFMON_PHUB_IB_SEND_PUSH_ACK_POLL = "PHUB_IB_SEND_PUSH_ACK_POLL"; //PHUB.h
const std::string PERFMON_PHUB_IB_SEND_PULL_ACK_POLL = "PHUB_IB_SEND_PULL_ACK_POLL";
//the above 3 only applies to infiniband and PSHUB

const std::string PERFMON_WORKER_MSG_IN_FLIGHT_TIME_PUSH = "WORKER_MSG_IN_FLIGHT_TIME_PUSH"; //ibverbs zmq
const std::string PERFMON_WORKER_MSG_IN_FLIGHT_TIME_PULL = "WORKER_MSG_IN_FLIGHT_TIME_PULL"; //ibverbs zmq

const std::string PERFMON_PHUB_MSG_IN_FLIGHT_TIME_PUSH_ACK = "PHUB_MSG_IN_FLIGHT_TIME_PUSH_ACK"; //ibverbs zmq
const std::string PERFMON_PHUB_MSG_IN_FLIGHT_TIME_PULL_ACK = "PHUB_MSG_IN_FLIGHT_TIME_PULL_ACK"; //ibverbs zmq
const std::string PERFMON_WORKER_PULL_ACK_PIPELINE_CALLBACK_FIRST_MUTEX_WAIT = "WORKER_PULL_ACK_PIPELINE_FIRST_MUTEX_WAIT";
const std::string PERFMON_WORKER_PULL_ACK_PIPELINE_CALLBACK_BODY = "WORKER_PULL_ACK_PIPELINE_CALLBACK_BODY";
const std::string PERFMON_WORKER_PULL_ACK_PIPELINE_CALLBACK_SECOND_MUTEX_WAIT = "WORKER_PULL_ACK_PIPELINE_CALLBACK_SECOND_MUTEX_WAIT";

const std::string PERFMON_PHUB_KEY_MERGE = "PHUB_KEY_MERGE"; //pshub_van and dist_sever
const std::string PERFMON_WORKER_PUSH_PIPELINE = "WORKER_PUSH_PIPELINE"; //dist_server.h->van.cc
const std::string PERFMON_WORKER_PULL_PIPELINE = "WORKER_PULL_PIPELINE"; //dist_server.h->van.cc
const std::string PERFMON_PHUB_PUSH_ACK_PIPELINE = "PHUB_PUSH_ACK_PIPELINE"; //van.cc
const std::string PERFMON_PHUB_PULL_ACK_PIPELINE = "PHUB_PULL_ACK_PIPELINE"; //van.cc
const std::string PERFMON_WORKER_PUSH_ACK_PIPELINE = "WORKER_PUSH_ACK_PIPELINE"; // handled for everyone.
const std::string PERFMON_WORKER_PULL_ACK_PIPELINE = "WORKER_PULL_ACK_PIPELINE"; //infiniband->kv_app.h handled for everyone.

//const std::string PERFMON_PHUB_PUSH_PIPELINE = "PERFMON_PHUB_PUSH_PIPELINE"; //van.cc
//const std::string PERFMON_PHUB_PULL_PIPELINE = "PERFMON_PHUB_PULL_PIPELINE"; //van.cc
//const std::string PERFMON_PHUB_POLL_RECV = "PERFMON_PHUB_POLL_RECV";
//const std::string PERFMON_WORKER_POLL_RECV = "PERFMON_WORKER_POLL_RECV";


class PerfMonLite
{
private:

public:
	static PerfMonLite e;

	enum SummaryMode
	{
		Full,
		Aggregated
	};

	enum OutputFormat
	{
		CSV,
		Formatted
	};

	static void Init()
	{
		e.RegisterEvent(PERFMON_WORKER_PUSH_PIPELINE);
		e.RegisterEvent(PERFMON_WORKER_MSG_IN_FLIGHT_TIME_PUSH);
		e.RegisterEvent(PERFMON_PHUB_PUSH_ACK_PIPELINE);
		e.RegisterEvent(PERFMON_PHUB_MSG_IN_FLIGHT_TIME_PUSH_ACK);
		e.RegisterEvent(PERFMON_WORKER_PUSH_ACK_PIPELINE);
		e.RegisterEvent(PERFMON_WORKER_PULL_PIPELINE);
		e.RegisterEvent(PERFMON_WORKER_MSG_IN_FLIGHT_TIME_PULL);
		e.RegisterEvent(PERFMON_PHUB_PULL_ACK_PIPELINE);
		e.RegisterEvent(PERFMON_PHUB_MSG_IN_FLIGHT_TIME_PULL_ACK);
		e.RegisterEvent(PERFMON_WORKER_PULL_ACK_PIPELINE);
		e.RegisterEvent(PERFMON_WORKER_PULL_ACK_PIPELINE_CALLBACK_FIRST_MUTEX_WAIT);
		e.RegisterEvent(PERFMON_WORKER_PULL_ACK_PIPELINE_CALLBACK_BODY);
		e.RegisterEvent(PERFMON_WORKER_PULL_ACK_PIPELINE_CALLBACK_SECOND_MUTEX_WAIT);

		e.RegisterEvent(PERFMON_WORKER_IB_SEND_PULL_POLL);
		e.RegisterEvent(PERFMON_WORKER_IB_SEND_PUSH_POLL);
		e.RegisterEvent(PERFMON_PHUB_IB_SEND_PUSH_ACK_POLL);
        e.RegisterEvent(PERFMON_PHUB_IB_SEND_PULL_ACK_POLL);
		//e.RegisterEvent(PERFMON_PHUB_POLL_RECV);
		//e.RegisterEvent(PERFMON_WORKER_POLL_RECV);
		e.RegisterEvent(PERFMON_PHUB_KEY_MERGE);
	}
	static PerfMonLite& Get() {
		return PerfMonLite::e;
	}
	static constexpr int MAX_SEQUENCE_SIZE = 8192;
	static constexpr int SEQUENCE_MASK = MAX_SEQUENCE_SIZE - 1;
	static constexpr int MAX_EVENT_TYPE = 32;
	static constexpr int EVENT_TYPE_MASK = MAX_EVENT_TYPE - 1;
	static constexpr int MAX_EXTENDED_SEQUENCE_SIZE = 128;
	static constexpr int EXTENDED_SEQUENCE_MASK = MAX_EXTENDED_SEQUENCE_SIZE - 1;
	struct PerfMonEntry
	{
		uint64_t EventSequence = 0;
		std::atomic<double> Maximum;
		std::atomic<double> Minimum;
		std::atomic<double> Sum;
		//the above dont have lock free impl of fetch and add. give them one.
		std::atomic<int> Counter;
		uint64_t Temporary[MAX_EXTENDED_SEQUENCE_SIZE];
	    void* AdditionalStates[MAX_EXTENDED_SEQUENCE_SIZE];
		PerfMonEntry()
		{
		    memset(Temporary, 0, sizeof(uint64_t) * MAX_EXTENDED_SEQUENCE_SIZE);
		    memset(AdditionalStates, 0, sizeof(void*) * MAX_EXTENDED_SEQUENCE_SIZE);
		}
	};
	std::unordered_map<std::string, int> EventName2Index;
	PerfMonEntry Storage[MAX_EVENT_TYPE][MAX_SEQUENCE_SIZE];

	void RegisterEvent(std::string name)
	{
		int sz = EventName2Index.size();
		EventName2Index[name] = sz;
	}
	//////////////////////////////////////////////////////////////////////////
	//The extendedSequence selector is added to prevent multiple people working on the same key (double pushes etc)
	//////////////////////////////////////////////////////////////////////////
	void LogTimeBegin(std::string eventName, uint64_t seq = 0, uint64_t startTime = 0, size_t extendedSeq = 0)
	{
		if (EventName2Index.find(eventName) == EventName2Index.end())
		{
			printf("[perfmon-lite][error] %s is not a registered event name. TRACE=%s\n", eventName.c_str(), GetStacktraceString().c_str());
		}
		//

		/*if (seq > SEQUENCE_MASK)
		{
		}
		if (extendedSeq > EXTENDED_SEQUENCE_MASK)
		{
			printf("[perfmon-lite][warning] in event %s, %d exeeds the maximum allowed extended sequence. A modulo to %d is made. %s\n",
				eventName.c_str(), extendedSeq, extendedSeq & EXTENDED_SEQUENCE_MASK, GetStacktraceString().c_str());
	}*/
		seq &= SEQUENCE_MASK;
		extendedSeq &= EXTENDED_SEQUENCE_MASK;
		/*if(eventName == PERFMON_WORKER_PUSH_PIPELINE)
			{
			printf("acquired a timestamp %s seq = %llu, ext = %d\n",eventName.c_str(), seq, extendedSeq);
			}*/

			
		//if(eventName == PERFMON_WORKER_MSG_IN_FLIGHT_TIME_PUSH)
		//{
		//    raise(SIGTRAP);
		//}
		auto eid = EventName2Index[eventName];
		PerfMonEntry& entry = Storage[eid][seq];
		//printf("[perfmon-lite][warning] in event %s,temp = %p, %d is sequence. XSEQ is %d. eid is %d %s\n",
		//       eventName.c_str(), &entry.Temporary[extendedSeq], seq, extendedSeq, eid,GetStacktraceString().c_str());

#if MXNET_USE_PROFILER //this is needed because for unit tests MXNET is not involved.
		auto pStat = mxnet::engine::Profiler::Get()->AddOprStat(true, 0);
		memcpy(pStat->opr_name, eventName.c_str(), eventName.length() > sizeof(pStat->opr_name - 1) ? sizeof(pStat->opr_name) - 1 : eventName.length());
		SetOprStart(pStat);
		pStat->thread_id = seq;
		entry.AdditionalStates[extendedSeq] = pStat;
		//printf("--------------------[%s]seq = %llu, ext = %d, additionalstates = %p, entry = %p, storage = %p, this = %p\n", eventName.c_str(), seq, extendedSeq,  entry.AdditionalStates[ extendedSeq], &entry, Storage, this);

#endif
		entry.EventSequence = seq;
		entry.Counter += 1;
		if (startTime == 0)
			entry.Temporary[extendedSeq] = std::chrono::system_clock::now().time_since_epoch().count();
		else
			entry.Temporary[extendedSeq] = startTime;

	}

	void LogTimeEnd(std::string eventName, uint64_t seq = 0, size_t extendedSeq = 0)
	{
		//CHECK(EventName2Index.find(eventName)!=EventName2Index.end()) << eventName <<" is not a registered event name.";
		if (EventName2Index.find(eventName) == EventName2Index.end())
		{
			printf("[perfmon-lite][error] %s is not a registered event name.\n ", eventName.c_str());
		}
		auto dbgSeq = seq;
		seq &= SEQUENCE_MASK;
		extendedSeq &= EXTENDED_SEQUENCE_MASK;

		auto eid = EventName2Index[eventName];
		PerfMonEntry& entry = Storage[eid][seq];
		if (entry.Temporary[extendedSeq] == 0 &&
			eventName != PERFMON_WORKER_PUSH_PIPELINE &&
			eventName != PERFMON_WORKER_PULL_PIPELINE &&
			eventName != PERFMON_WORKER_PUSH_ACK_PIPELINE &&
			eventName != PERFMON_WORKER_PULL_ACK_PIPELINE)
		{
			//excessive calls may be produced if key is chunked. we only track the primary key (physical key).
			printf("Unpaired call to %s, seq = %llu(%llu). extSeq = %d, %s\n", eventName.c_str(), seq, dbgSeq, extendedSeq, GetStacktraceString().c_str());
			raise(SIGTRAP);
		}

		auto now = std::chrono::system_clock::now().time_since_epoch().count();
		double reading = now - entry.Temporary[extendedSeq];
		if (now < entry.Temporary[extendedSeq] || entry.Temporary[extendedSeq] == 0) return;//drop this.
		//printf("[perfmon-lite][warning]done in event %s, %d is sequence. XSEQ is %d. eid = %d %s\n",
		//       eventName.c_str(), seq, extendedSeq, eid, GetStacktraceString().c_str());

#if MXNET_USE_PROFILER //this is needed because for unit tests MXNET is not involved.
        auto pStat = (mxnet::engine::OprExecStat*)entry.AdditionalStates[extendedSeq];
	//printf("[end of end]additionalstates = %p, temp = %p, entry = %p, Storage = %p, this = %p\n", entry.AdditionalStates[ extendedSeq],&entry.Temporary[extendedSeq] ,&entry, Storage, this);
	//printf("--------------------[%s,PAIRED]seq = %llu, ext = %d, additionalstates = %p, entry = %p, storage = %p, this = %p\n", eventName.c_str(), seq, extendedSeq,  entry.AdditionalStates[ extendedSeq], &entry, Storage, this);

        SetOprEnd(pStat);
	// pStat->WellFormed = true;
#endif
		entry.Temporary[extendedSeq] = 0;
		entry.AdditionalStates[extendedSeq] = NULL;
		//if(eventName == PERFMON_WORKER_MSG_IN_FLIGHT_TIME_PULL)
		//{
		//    printf("released a timestamp %s seq = %d, reading = %f, temp = %llu, now = %llu\n",eventName.c_str(), (int)seq, reading, entry.Temporary, now);
		//}

		atomic_fetch_add_ex(&entry.Sum, reading);
		atomic_replace_if<double>(&entry.Maximum, reading, [](double a, double b) {return a < b; });
		atomic_replace_if<double>(&entry.Minimum, reading, [](double a, double b) {return a > b; });
	}

	void LogEvent(std::string eventName, double reading, uint64_t seq = 0)
	{
		assert(seq < MAX_SEQUENCE_SIZE && seq >= 0);
		auto eid = EventName2Index[eventName];
		PerfMonEntry& entry = Storage[eid][seq];
		entry.EventSequence = seq;
		entry.Counter += 1;
		atomic_fetch_add_ex(&entry.Sum, reading);
		atomic_replace_if<double>(&entry.Maximum, reading, [](double a, double b) {return a < b; });
		atomic_replace_if<double>(&entry.Minimum, reading, [](double a, double b) {return a > b; });
	}

	std::string Summarize(std::string eventName, SummaryMode mode = SummaryMode::Full, OutputFormat format = OutputFormat::CSV)
	{
		std::stringstream ss;
		auto eid = EventName2Index[eventName];
		if (format == OutputFormat::Formatted)
		{
			ss << "EVENT_SOURCE:" << eventName << std::endl;
		}
		else if (format == OutputFormat::CSV)
		{
			//print headers
			if (mode == SummaryMode::Full)
			{
				ss << "EVENT_SOURCE,COUNTER,SEQUENCE,MIN,MAX,AVERAGE" << std::endl;
			}
			else if (mode == SummaryMode::Aggregated)
			{
				ss << "EVENT_SOURCE,COUNTER,SEQCOUNT,AVERAGE,STDEV,MAX,MIN" << std::endl;
			}
		}

		int counter = 0, eventSourceCnt = 0;
		double min = std::numeric_limits<double>::max(), max = std::numeric_limits<double>::min();
		double sum = 0;
		for (int i = 0; i < MAX_SEQUENCE_SIZE; i++)
		{
			PerfMonEntry& curr = Storage[eid][i];

			if (curr.Counter != 0)
			{
				if (mode == SummaryMode::Full)
				{
					if (format == OutputFormat::Formatted)
					{
						ss << "    COUNTER :" << curr.Counter << std::endl;
						ss << "    SEQUENCE:" << curr.EventSequence << std::endl;
						ss << "    MIN     :" << curr.Minimum << std::endl;
						ss << "    MAX     :" << curr.Maximum << std::endl;
						ss << "    AVERAGE :" << (curr.Sum / curr.Counter) << std::endl;
						ss << "    ---------" << std::endl;
					}
					else if (format == OutputFormat::CSV)
					{
						ss << eventName << "," << curr.Counter << "," << curr.EventSequence << "," << curr.Minimum << "," << curr.Maximum << "," << curr.Sum / curr.Counter << std::endl;
					}
				}
				min = min > curr.Minimum ? double(curr.Minimum) : min;
				max = max < curr.Maximum ? double(curr.Maximum) : max;
				sum += curr.Sum;
				counter += curr.Counter;
				eventSourceCnt++;
			}
		}
		//now, let's summarize
		if (counter != 0 && mode == SummaryMode::Aggregated)
		{
			double avg = 0, std = 0;
			int seq = 0;
			avg = sum / counter;
			for (int i = 0; i < MAX_SEQUENCE_SIZE; i++)
			{
				PerfMonEntry& curr = Storage[eid][i];

				if (curr.Counter != 0)
				{
					seq++;
					std += pow((curr.Sum / curr.Counter) - avg, 2);
				}
			}
			std = sqrt(std) / seq;
			if (format == OutputFormat::Formatted)
			{
				ss << "    COUNTER :" << counter << std::endl;
				ss << "    SEQCNT  :" << eventSourceCnt << std::endl;
				ss << "    AVERAGE :" << avg << std::endl;
				ss << "    STDDEV  :" << std << std::endl;
				ss << "    MAX     :" << max << std::endl;
				ss << "    MIN     :" << min << std::endl;
				ss << "    ---------" << std::endl;
			}
			else if (format == OutputFormat::CSV)
			{
				ss << eventName << "," << counter << "," << eventSourceCnt << "," << avg << "," << std << "," << max << "," << min << std::endl;
			}

		}
		//printf("check point %s\n",ss.str().c_str());
		return ss.str();
	}

	std::string Summarize(SummaryMode mode = SummaryMode::Full, OutputFormat format = OutputFormat::Formatted)
	{
		std::stringstream ss;
		for (auto kv : EventName2Index) {
			ss << Summarize(kv.first, mode);
		}
		return ss.str();
	}

	uint64_t GetCurrentTimestamp()
	{
		return std::chrono::system_clock::now().time_since_epoch().count();
	}

};

#endif
