#include "stdafx.h"
#include "libcouchbase/couchbase.h"
#include "libcouchbase/views.h"
#include "CouchbaseProxy.h"

#include <iostream>
#include <ostream>

namespace mc
{
	typedef CCouchbaseProxy::callback_t callback_t;

	class CRequest
	{
	protected : 
		std::string		key_;
		std::string		value_;

		callback_t		callback_;

	public : 
		CRequest(const std::string& key,const std::string& value) : key_(key),value_(value),callback_(nullptr) {}
		CRequest(const std::string& key,const std::string& value,const callback_t& callback) : key_(key),value_(value),callback_(callback) {}
		~CRequest(){}

	public : 
		const char*		getKey() const { return key_.c_str(); }
		size_t			getKeyLen() const { return key_.size(); }

		const char*		getValue() const { return value_.c_str(); }
		size_t			getValueLen() const { return value_.size(); }

		const callback_t& getCallback() const { return callback_; }
	};

	class CResponse
	{
	protected : 
		bool			success_;
		std::string		result_;
		callback_t		callback_;

	public : 
		CResponse() : success_(true),result_(),callback_(){}
		CResponse(bool success,std::string result,const callback_t& callback) : success_(success),result_(std::move(result)),callback_(callback){}
		explicit CResponse(const CResponse& rhs) : success_(rhs.success_),result_(rhs.result_),callback_(rhs.callback_){}

	public :
		void			dispatch() const
		{
			callback_(success_,result_);
		}
	};

	class CCouchbaseProxy::CInstance
	{
		typedef			tbb::concurrent_queue<CResponse> queResponse_t;

	protected :
		lcb_t			instance_;
		std::string		strError_;

		queResponse_t	queResponse_;

	public : 
		CInstance() : instance_(nullptr),strError_(),queResponse_() {}
		~CInstance(){}

	public : 
		bool			connect(const char* connStr);
		void			close();
		void			breakout();

		bool			requestGet(const char* keyStr,size_t keyLen,void* cookie);
		bool			requestGetReplica(const char* keyStr,size_t keyLen,void* cookie);
		bool			requestSet(CCouchbaseProxy::ENUM_STORE_MODE mode,const char* keyStr,size_t keyLen,const char* valueStr,size_t valueLen,void* cookie);
		bool			requestRemove(const char* keyStr,size_t keyLen,void* cookie);
		bool			requestViewQuery(const char* doc,size_t docLen,const char* view,size_t viewLen,const char* opt,size_t optLen,void* cookie);
		
		void			addResult(const CResponse& result) { queResponse_.push(result); }
		size_t			handleResult();

	public : 
		const char*		getStrError() const { return strError_.c_str(); }

	protected : 
		static void		_cb_store(lcb_t instance, const void* cookie, lcb_storage_t op, lcb_error_t err,const lcb_store_resp_t* resp);
		static void		_cb_get(lcb_t instance, const void* cookie,lcb_error_t err,const lcb_get_resp_t* resp);
		static void		_cb_remove(lcb_t instance, const void* cookie, lcb_error_t err, const lcb_remove_resp_t* resp);
		static void		_cb_view_query(lcb_t instance, int ign, const lcb_RESPVIEWQUERY* rv);
	};

	void CCouchbaseProxy::CInstance::_cb_store(lcb_t lcbInstance, const void* cookie, lcb_storage_t /*op*/, lcb_error_t err,const lcb_store_resp_t* /*resp*/)
	{
		if(cookie==nullptr)
			return ;		//no return..

		CRequest* request=const_cast<CRequest*>(reinterpret_cast<const CRequest*>(cookie));
		if(request->getCallback()==nullptr)
			return ;		//no return.

		CCouchbaseProxy::CInstance* instance=const_cast<CCouchbaseProxy::CInstance*>(reinterpret_cast<const CCouchbaseProxy::CInstance*>(lcb_get_cookie(lcbInstance)));
		if(err!=LCB_SUCCESS)
		{
			instance->addResult(CResponse(false,lcb_strerror(lcbInstance,err),request->getCallback()));
			return ;
		}
		if(request->getCallback()!=nullptr)
			instance->addResult(CResponse(true,nullptr,request->getCallback()));
	}

	void CCouchbaseProxy::CInstance::_cb_get(lcb_t lcbInstance, const void* cookie,lcb_error_t err,const lcb_get_resp_t* resp)
	{
		assert(cookie);

		CRequest* request=const_cast<CRequest*>(reinterpret_cast<const CRequest*>(cookie));
		CCouchbaseProxy::CInstance* instance=const_cast<CCouchbaseProxy::CInstance*>(reinterpret_cast<const CCouchbaseProxy::CInstance*>(lcb_get_cookie(lcbInstance)));
		if(err!=LCB_SUCCESS)
		{
			instance->addResult(CResponse(false,lcb_strerror(lcbInstance,err),request->getCallback()));
			return ;
		}
		instance->addResult(CResponse(true,std::string((const char*)resp->v.v0.bytes,(size_t)resp->v.v0.nbytes),request->getCallback()));
	}

	void CCouchbaseProxy::CInstance::_cb_remove(lcb_t lcbInstance, const void* cookie, lcb_error_t err, const lcb_remove_resp_t* /*resp*/)
	{
		if(cookie==nullptr)
			return ;		//no return..

		CRequest* request=const_cast<CRequest*>(reinterpret_cast<const CRequest*>(cookie));
		CCouchbaseProxy::CInstance* instance=const_cast<CCouchbaseProxy::CInstance*>(reinterpret_cast<const CCouchbaseProxy::CInstance*>(lcb_get_cookie(lcbInstance)));
		if(err!=LCB_SUCCESS)
		{
			instance->addResult(CResponse(false,lcb_strerror(lcbInstance,err),request->getCallback()));
			return ;
		}
		if(request->getCallback()!=nullptr)
			instance->addResult(CResponse(true,std::string(),request->getCallback()));
	}

	void CCouchbaseProxy::CInstance::_cb_view_query(lcb_t lcbInstance,int /*ign*/,const lcb_RESPVIEWQUERY* rv)
	{
		if(rv->cookie==nullptr)
			return;		//no return

		CRequest* request=const_cast<CRequest*>(reinterpret_cast<const CRequest*>(rv->cookie));
		CCouchbaseProxy::CInstance* instance=const_cast<CCouchbaseProxy::CInstance*>(reinterpret_cast<const CCouchbaseProxy::CInstance*>(lcb_get_cookie(lcbInstance)));

		if(rv->rc!=LCB_SUCCESS)
		{
			instance->addResult(CResponse(false,lcb_strerror(lcbInstance,rv->rc),request->getCallback()));
			return ;
		}

		if(rv->rflags&LCB_RESP_F_FINAL) 
		{
			instance->addResult(CResponse(true,std::string(),request->getCallback()));
			return ;
		}

		if(rv->docresp==nullptr) 
		{
			instance->addResult(CResponse(true,std::string((const char*)rv->value,(size_t)rv->nvalue),request->getCallback()));
			return ;
		}
		instance->addResult(CResponse(true,std::string((const char*)rv->docresp->value,(size_t)rv->docresp->nvalue),request->getCallback()));
	}

	bool CCouchbaseProxy::CInstance::connect(const char* connStr)
	{
		struct lcb_create_st cropts;
		memset(&cropts,0,sizeof(lcb_create_st));
		cropts.version = 3;
		cropts.v.v3.connstr = connStr;
		lcb_error_t err;
		err = lcb_create(&instance_, &cropts);
		if (err != LCB_SUCCESS) 
		{
			strError_=lcb_strerror(instance_,err);
			return false;
		}
		lcb_connect(instance_);
		lcb_set_cookie(instance_,this);
		lcb_wait(instance_);
		if ( (err = lcb_get_bootstrap_status(instance_)) != LCB_SUCCESS ) 
		{
			strError_=lcb_strerror(instance_,err);
			return false;
		}

		lcb_set_store_callback(instance_, CCouchbaseProxy::CInstance::_cb_store);
		lcb_set_get_callback(instance_, CCouchbaseProxy::CInstance::_cb_get);
		lcb_set_remove_callback(instance_, CCouchbaseProxy::CInstance::_cb_remove);

		return true;
	}

	void CCouchbaseProxy::CInstance::close()
	{
		if(instance_==nullptr)
			return ;
		lcb_destroy(instance_);
		instance_=nullptr;
	}

	void CCouchbaseProxy::CInstance::breakout()
	{
		if(instance_==nullptr)
			return ;
		lcb_breakout(instance_);
	}

	bool CCouchbaseProxy::CInstance::requestGet(const char* keyStr,size_t keyLen,void* cookie)
	{
		assert(strlen(keyStr)==keyLen);

		lcb_get_cmd_t gcmd;
		memset(&gcmd,0,sizeof(gcmd));
		gcmd.v.v0.key = keyStr;
		gcmd.v.v0.nkey = keyLen;

		lcb_error_t err;
		lcb_get_cmd_t* gcmdlist=&gcmd;
		if((err = lcb_get(instance_,cookie,1,&gcmdlist)) != LCB_SUCCESS) 
		{
			strError_=lcb_strerror(instance_,err);
			return false;
		}
		lcb_wait(instance_);
		return true;
	}

	bool CCouchbaseProxy::CInstance::requestGetReplica(const char* keyStr,size_t keyLen,void* cookie)
	{
		assert(strlen(keyStr)==keyLen);

		lcb_get_replica_cmd_t gcmd;
		memset(&gcmd,0,sizeof(gcmd));
		gcmd.v.v1.key = keyStr;
		gcmd.v.v1.nkey = keyLen;
		gcmd.v.v1.strategy = LCB_REPLICA_FIRST;

		lcb_error_t err;
		lcb_get_replica_cmd_t* gcmdlist=&gcmd;
		if((err = lcb_get_replica(instance_,cookie,1,&gcmdlist)) != LCB_SUCCESS) 
		{
			strError_=lcb_strerror(instance_,err);
			return false;
		}
		lcb_wait(instance_);
		return true;
	}

	bool CCouchbaseProxy::CInstance::requestSet(CCouchbaseProxy::ENUM_STORE_MODE mode,const char* keyStr,size_t keyLen,const char* valueStr,size_t valueLen,void* cookie)
	{
		const lcb_storage_t STORE_MODE[]={LCB_ADD,LCB_SET,LCB_REPLACE,LCB_APPEND,LCB_PREPEND};

		assert(strlen(keyStr) == keyLen);
		assert(strlen(valueStr)==valueLen);

		lcb_store_cmd_t scmd;
		memset(&scmd,0,sizeof(scmd));
		scmd.v.v0.key = keyStr;
		scmd.v.v0.nkey = keyLen;
		scmd.v.v0.bytes = valueStr;
		scmd.v.v0.nbytes = valueLen;
		scmd.v.v0.operation = STORE_MODE[mode];

		lcb_error_t err;
		lcb_store_cmd_t* scmdlist=&scmd;
		if((err=lcb_store(instance_,cookie,1,&scmdlist))!=LCB_SUCCESS)
		{
			strError_=lcb_strerror(instance_,err);
			return false;
		}
		lcb_wait(instance_);
		return true;
	}

	bool CCouchbaseProxy::CInstance::requestRemove(const char* keyStr,size_t keyLen,void* cookie)
	{
		assert(strlen(keyStr)==keyLen);

		lcb_remove_cmd_t rcmd;
		memset(&rcmd,0,sizeof(rcmd));
		rcmd.v.v0.key = keyStr;
		rcmd.v.v0.nkey = keyLen;

		lcb_error_t err;
		lcb_remove_cmd_t* rcmdlist = &rcmd;
		if((err=lcb_remove(instance_,cookie,1,&rcmdlist))!=LCB_SUCCESS)
		{
			strError_=lcb_strerror(instance_,err);
			return false;
		}
		lcb_wait(instance_);
		return true;
	}

	bool CCouchbaseProxy::CInstance::requestViewQuery(const char* doc,size_t docLen,const char* view,size_t viewLen,const char* opt,size_t optLen,void* cookie)
	{
		lcb_CMDVIEWQUERY cmd;
		memset(&cmd,0,sizeof(cmd));
		cmd.ddoc = doc;
		cmd.nddoc = docLen;
		cmd.view = view;
		cmd.nview = viewLen;
		cmd.optstr = opt;
		cmd.noptstr = optLen;
		cmd.callback = _cb_view_query;

		lcb_error_t err;
		if((err=lcb_view_query(instance_,cookie,&cmd))!=LCB_SUCCESS)
		{
			strError_=lcb_strerror(instance_,err);
			return false;
		}
		lcb_wait(instance_);
		return true;
	}

	size_t CCouchbaseProxy::CInstance::handleResult()
	{
		size_t processNum=0;

		CResponse response;
		while(queResponse_.try_pop(response)==true)
		{
			response.dispatch();
			++processNum;
		}

		return processNum;
	}

	//////////////////////////////////////////////////////////////////////////
	class CCouchbaseProxy::CWorker
	{
	protected : 
		typedef std::function<void()>									task_t;
		typedef std::function<void(unsigned int iid,const task_t&)>		executor_t;
		typedef tbb::concurrent_queue<task_t>							queRequest_t;
		typedef std::vector<queRequest_t>								lstRequestQueue_t;

		CCouchbaseProxy*			proxy_;
		unsigned int				workerNum_;
		std::vector<std::thread*>	threads_;
		lstRequestQueue_t			lstRequestQueue_;

		executor_t					executor_;

		bool						stop_;

	public : 
		CWorker(CCouchbaseProxy* proxy,unsigned int workerNum);
		~CWorker(){}

	public : 
		void		execute(unsigned int iid,const task_t& task) { executor_(iid,task); }
		void		stop();
	};

	CCouchbaseProxy::CWorker::CWorker(CCouchbaseProxy* proxy,unsigned int workerNum) : proxy_(proxy),workerNum_(workerNum),stop_(false)
	{
		for (size_t i=0;i<workerNum_;++i)
		{
			threads_.push_back(new std::thread(
				[=]()
				{
					while (stop_ == false)
					{
						auto iter = lstRequestQueue_.begin();
						auto iterEnd = lstRequestQueue_.end();
						for (; iter != iterEnd; ++iter)
						{
							std::function<void()> fn;
							if ((*iter).try_pop(fn) == false)
								continue;
							fn();
						}

						std::this_thread::sleep_for(std::chrono::milliseconds(10));
					}
				}
			));
			lstRequestQueue_.push_back(queRequest_t());
		}
		
		if(workerNum_==0)
			executor_=[](unsigned int /*iid*/,const task_t& fn){ fn(); };
		else
			executor_=[=](unsigned int iid,const task_t& fn) { lstRequestQueue_[iid%workerNum_].push(fn); };
	}

	void CCouchbaseProxy::CWorker::stop()
	{
		stop_=true;
		for (std::thread* iter : threads_)
		{
			iter->join();
			delete iter;
		}
		threads_.clear();
	}

	//////////////////////////////////////////////////////////////////////////
	CCouchbaseProxy::CCouchbaseProxy(unsigned int workerNum) : worker_(nullptr)
	{
		worker_=new CWorker(this,workerNum);
	}

	CCouchbaseProxy::~CCouchbaseProxy()
	{
		close();
		worker_->stop();
		delete worker_;
	}

	bool CCouchbaseProxy::connect(unsigned int iid,const char* connstr)
	{
		for(size_t x=lstInstance_.size();x<iid+1;++x)
			lstInstance_.push_back(nullptr);

		if(lstInstance_[iid]!=nullptr)
			return false;

		lstInstance_[iid]=new CCouchbaseProxy::CInstance;
		return lstInstance_[iid]->connect(connstr);
	}

	bool CCouchbaseProxy::close(unsigned int iid/*=(unsigned int)-1*/)
	{
		if(iid==(unsigned int)-1)
		{
			for (auto iter : lstInstance_)
			{
				if(iter==nullptr)
					continue;
				iter->breakout();
				iter->close();
			}
			lstInstance_.clear();
			return true;
		}

		if(lstInstance_.size()<=iid || lstInstance_[iid]==nullptr)
			return false;

		lstInstance_[iid]->breakout();
		lstInstance_[iid]->close();
		lstInstance_[iid]=nullptr;

		return true;
	}

	bool CCouchbaseProxy::requestGet(unsigned int iid,const char* keyStr,size_t keyLen,const callback_t& cb)
	{
		if(lstInstance_.size()<=iid || lstInstance_[iid]==nullptr)
			return false;

		worker_->execute(iid,
			[=]()
			{
				CRequest request(std::string(keyStr,keyLen),std::string(),cb);
				lstInstance_[iid]->requestGet(request.getKey(),request.getKeyLen(),&request);
			}
		);
		return true;
	}

	bool CCouchbaseProxy::requestGetReplica(unsigned int iid,const char* keyStr,size_t keyLen,const callback_t& cb)
	{
		if(lstInstance_.size()<=iid || lstInstance_[iid]==nullptr)
			return false;

		worker_->execute(iid,
			[=]()
			{
				CRequest request(std::string(keyStr,keyLen),std::string(),cb);
				lstInstance_[iid]->requestGetReplica(request.getKey(),request.getKeyLen(),&request);
			}
		);
		return true;
	}

	bool CCouchbaseProxy::requestSet(unsigned int iid,CCouchbaseProxy::ENUM_STORE_MODE mode,const char* keyStr,size_t keyLen,const char* valueStr,size_t valueLen,const callback_t& cb)
	{
		if(lstInstance_.size()<=iid || lstInstance_[iid]==nullptr)
			return false;

		worker_->execute(iid,
			[=]()
			{
				CRequest request(std::string(keyStr,keyLen),std::string(valueStr,valueLen),cb);
				lstInstance_[iid]->requestSet(mode,request.getKey(),request.getKeyLen(),request.getValue(),request.getValueLen(),&request);
			}
		);
		return true;
	}

	bool CCouchbaseProxy::requestRemove(unsigned int iid,const char* keyStr,size_t keyLen,const callback_t& cb)
	{
		if(lstInstance_.size()<=iid || lstInstance_[iid]==nullptr)
			return false;

		worker_->execute(iid,
			[=]()
			{
				CRequest request(std::string(keyStr,keyLen),std::string(),cb);
				lstInstance_[iid]->requestRemove(request.getKey(),request.getKeyLen(),&request);
			}
		);
		return true;
	}

	bool CCouchbaseProxy::requestViewQuery(unsigned int iid,const char* doc,size_t docLen,const char* view,size_t viewLen,const callback_t& cb,const char* opt/*=nullptr*/,size_t optLen/*=0*/)
	{
		if(lstInstance_.size()<=iid || lstInstance_[iid]==nullptr)
			return false;

		worker_->execute(iid,
			[=]()
			{
				const char* defaultOpt="limit=100";
				std::string userOpt;
				if(opt==nullptr || optLen==0)
					userOpt.assign(defaultOpt,strlen(defaultOpt));
				else
					userOpt.assign(opt,optLen);
				CRequest request(std::string(doc,docLen),std::string(view,viewLen),cb);
				lstInstance_[iid]->requestViewQuery(request.getKey(),request.getKeyLen(),request.getValue(),request.getValueLen(),userOpt.c_str(),userOpt.length(),&request);
			}
		);
		return true;
	}

	size_t CCouchbaseProxy::handleResultCallback()
	{
		size_t handleResultNum=0;

		auto iter=lstInstance_.begin();
		auto iterEnd=lstInstance_.end();
		for(;iter!=iterEnd;++iter)
		{
			if((*iter)==nullptr)
				continue;
			handleResultNum+=(*iter)->handleResult();
		}
		return handleResultNum;
	}

	void CCouchbaseProxy::test()
	{
	}
}
