#pragma once

namespace mc
{
	class CCouchbaseProxy
	{
	public : 
		enum ENUM_STORE_MODE { ADD=0, SET, REPLACE, APPEND, PREPEND };

	public : 
		class CInstance;
		class CWorker;

		typedef std::vector<CInstance*>							lstInstance_t;
		typedef std::function<void(bool,const std::string&)>	callback_t;

	protected : 
		lstInstance_t		lstInstance_;
		CWorker*			worker_;

	public : 
		CCouchbaseProxy(unsigned int workerNum);
		~CCouchbaseProxy();

	public : 
		bool	connect(unsigned int iid,const char* connstr);	//couchbase://host1.net;host2.net/mybucket
		bool	close(unsigned int iid=(unsigned int)-1);

		bool	requestGet(unsigned int iid,const char* keyStr,size_t keyLen,const callback_t& cb);
		bool	requestGetReplica(unsigned int iid,const char* keyStr,size_t keyLen,const callback_t& cb);
		bool	requestSet(unsigned int iid,CCouchbaseProxy::ENUM_STORE_MODE mode,const char* keyStr,size_t keyLen,const char* valueStr,size_t valueLen,const callback_t& cb);
		bool	requestRemove(unsigned int iid,const char* keyStr,size_t keyLen,const callback_t& cb);
		bool	requestViewQuery(unsigned int iid,const char* doc,size_t docLen,const char* view,size_t viewLen,const callback_t& cb,const char* opt=nullptr,size_t optLen=0);

		size_t	handleResultCallback();

	public : 
		static void test();
	};
}