/*
 * Copyright 2010-2020 DatasysLab@iit.edu(http://datasys.cs.iit.edu/index.html)
 *      Director: Ioan Raicu(iraicu@cs.iit.edu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file is part of ZHT library(http://datasys.cs.iit.edu/projects/ZHT/index.html).
 *      Tonglin Li(tli13@hawk.iit.edu) with nickname Tony,
 *      Xiaobing Zhou(xzhou40@hawk.iit.edu) with nickname Xiaobingo,
 *      Ke Wang(kwang22@hawk.iit.edu) with nickname KWang,
 *      Dongfang Zhao(dzhao8@@hawk.iit.edu) with nickname DZhao,
 *      Ioan Raicu(iraicu@cs.iit.edu).
 *
 * cpp_zhtclient.cpp
 *
 *  Created on: Sep 16, 2012
 *      Author: Xiaobingo
 *      Contributor: Tony, KWang, DZhao
 */

#include "cpp_zhtclient.h"

#include  <stdlib.h>
#include <string.h>

#include "zpack.pb.h"
#include "ConfHandler.h"
#include "Env.h"
#include "StrTokenizer.h"
#include "Util.h"

#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <errno.h>
#include "bigdata_transfer.h"
#include "tcp_proxy_stub.h"
#include "ZHTUtil.h"
#include <unistd.h>
using namespace iit::datasys::zht::dm;

ZHTClient::ZHTClient() :
		_proxy(0), _msg_maxsize(0) {

}

ZHTClient::ZHTClient(const string& zhtConf, const string& neighborConf) {

	init(zhtConf, neighborConf);
}

ZHTClient::~ZHTClient() {

	if (_proxy != NULL) {

		delete _proxy;
		_proxy = NULL;
	}
}

int ZHTClient::init(const string& zhtConf, const string& neighborConf) {

	ConfHandler::initConf(zhtConf, neighborConf);

	_msg_maxsize = Env::get_msg_maxsize();

	_proxy = ProxyStubFactory::createProxy();

	if (_proxy == 0)
		return -1;
	else
		return 0;
}

int ZHTClient::init(const char *zhtConf, const char *neighborConf) {

	string szhtconf(zhtConf);
	string sneighborconf(neighborConf);

	int rc = init(szhtconf, sneighborconf);

	return rc;
}

int ZHTClient::commonOp(const string &opcode, const string &key,
		const string &val, const string &val2, string &result, int lease) {

	if (opcode != Const::ZSC_OPC_LOOKUP && opcode != Const::ZSC_OPC_REMOVE
			&& opcode != Const::ZSC_OPC_INSERT
			&& opcode != Const::ZSC_OPC_APPEND
			&& opcode != Const::ZSC_OPC_CMPSWP
			&& opcode != Const::ZSC_OPC_STCHGCB)
		return Const::toInt(Const::ZSC_REC_UOPC);

	string sstatus = commonOpInternal(opcode, key, val, val2, result, lease);

	int status = Const::ZSI_REC_CLTFAIL;
	if (!sstatus.empty())
		status = Const::toInt(sstatus);

	return status;
}

int ZHTClient::lookup(const string &key, string &result) {

	string val;
	string val2;
	int rc = commonOp(Const::ZSC_OPC_LOOKUP, key, val, val2, result, 1);

	result = extract_value(result);

	return rc;
}

int ZHTClient::lookup(const char *key, char *result) {

	string skey(key);
	string sresult;

	int rc = lookup(skey, sresult);

	strncpy(result, sresult.c_str(), sresult.size() + 1);

	return rc;
}

int ZHTClient::remove(const string &key) {

	string val;
	string val2;
	string result;
	int rc = commonOp(Const::ZSC_OPC_REMOVE, key, val, val2, result, 1);

	return rc;
}

int ZHTClient::remove(const char *key) {

	string skey(key);

	int rc = remove(skey);

	return rc;
}

int ZHTClient::insert(const string &key, const string &val) {

	string val2;
	string result;
	int rc = commonOp(Const::ZSC_OPC_INSERT, key, val, val2, result, 1);

	return rc;
}

int ZHTClient::insert(const char *key, const char *val) {

	string skey(key);
	string sval(val);

	int rc = insert(skey, sval);

	return rc;
}

int ZHTClient::append(const string &key, const string &val) {

	string val2;
	string result;
	int rc = commonOp(Const::ZSC_OPC_APPEND, key, val, val2, result, 1);

	return rc;
}

int ZHTClient::append(const char *key, const char *val) {

	string skey(key);
	string sval(val);

	int rc = append(skey, sval);

	return rc;
}

string ZHTClient::extract_value(const string &returnStr) {

	string val;

	StrTokenizer strtok(returnStr, ":");
	/*
	 * hello,zht:hello,ZHT ==> zht:ZHT
	 * */

	if (strtok.has_more_tokens()) {

		while (strtok.has_more_tokens()) {

			ZPack zpack;
			zpack.ParseFromString(strtok.next_token());

			if (zpack.valnull())
				val.append("");
			else
				val.append(zpack.val());

			val.append(":");
		}

		size_t found = val.find_last_of(":");
		val = val.substr(0, found);

	} else {

		ZPack zpack;
		zpack.ParseFromString(returnStr);

		if (zpack.valnull())
			val = "";
		else
			val = zpack.val();
	}

	return val;
}

int ZHTClient::compare_swap(const string &key, const string &seen_val,
		const string &new_val, string &result) {

	int rc = commonOp(Const::ZSC_OPC_CMPSWP, key, seen_val, new_val, result, 1);

	result = extract_value(result);

	return rc;
}

int ZHTClient::compare_swap(const char *key, const char *seen_val,
		const char *new_val, char *result) {

	string skey(key);
	string sseen_val(seen_val);
	string snew_val(new_val);
	string sresult;

	int rc = compare_swap(skey, sseen_val, snew_val, sresult);

	strncpy(result, sresult.c_str(), sresult.size() + 1);

	return rc;
}

int ZHTClient::state_change_callback(const string &key,
		const string &expected_val, int lease) {

	string val2;
	string result;

	int rc = commonOp(Const::ZSC_OPC_STCHGCB, key, expected_val, val2, result,
			lease);

	return rc;
}

int ZHTClient::state_change_callback(const char *key, const char *expeded_val,
		int lease) {

	string skey(key);
	string sexpeded_val(expeded_val);

	int rc = state_change_callback(skey, sexpeded_val, lease);

	return rc;
}

string ZHTClient::commonOpInternal(const string &opcode, const string &key,
		const string &val, const string &val2, string &result, int lease) {

	ZPack zpack;
	zpack.set_opcode(opcode); //"001": lookup, "002": remove, "003": insert, "004": append, "005", compare_swap
	zpack.set_replicanum(3); // Reserved but not used at this point.

	if (key.empty())
		return Const::ZSC_REC_EMPTYKEY; //-1, empty key not allowed.
	else
		zpack.set_key(key);

	if (val.empty()) {

		zpack.set_val("^"); //coup, to fix ridiculous bug of protobuf! //to debug
		zpack.set_valnull(true);
	} else {

		zpack.set_val(val);
		zpack.set_valnull(false);
	}

	if (val2.empty()) {

		zpack.set_newval("?"); //coup, to fix ridiculous bug of protobuf! //to debug
		zpack.set_newvalnull(true);
	} else {

		zpack.set_newval(val2);
		zpack.set_newvalnull(false);
	}

	zpack.set_lease(Const::toString(lease));

	string msg = zpack.SerializeAsString();

	/*ZPack tmp;
	 tmp.ParseFromString(msg);
	 printf("{%s}:{%s,%s}\n", tmp.key().c_str(), tmp.val().c_str(),
	 tmp.newval().c_str());*/

	char *buf = (char*) calloc(_msg_maxsize, sizeof(char));
	size_t msz = _msg_maxsize;

	/*send to and receive from*/
	_proxy->sendrecv(msg.c_str(), msg.size(), buf, msz);

	/*...parse status and result*/
	string sstatus;

	string srecv(buf);

	if (srecv.empty()) {

		sstatus = Const::ZSC_REC_SRVEXP;
	} else {

		result = srecv.substr(3); //the left, if any, is lookup result or second-try zpack
		sstatus = srecv.substr(0, 3); //status returned, the first three chars, like 001, -98...
	}

	free(buf);
	return sstatus;
}


//Duplicated from ip_proxy_stub.cpp
int loopedrecv(int sock, void *senderAddr, string &srecv) {

	ssize_t recvcount = -2;
	socklen_t addr_len = sizeof(struct sockaddr);

	BdRecvBase *pbrb = new BdRecvFromServer();

	char buf[Env::BUF_SIZE];

	while (1) {

		memset(buf, '\0', sizeof(buf));

		ssize_t count;
		if (senderAddr == NULL)
			count = ::recv(sock, buf, sizeof(buf), 0);
		else
			count = ::recvfrom(sock, buf, sizeof(buf), 0,
					(struct sockaddr *) senderAddr, &addr_len);

		if (count == -1 || count == 0) {

			recvcount = count;

			break;
		}

		bool ready = false;

		string bd = pbrb->getBdStr(sock, buf, count, ready);

		if (ready) {

			srecv = bd;
			recvcount = srecv.size();

			break;
		}

		memset(buf, '\0', sizeof(buf));
	}

	delete pbrb;
	pbrb = NULL;

	return recvcount;
}

int sendTo_BD(int sock, const void* sendbuf, int sendcount) {

	BdSendBase *pbsb = new BdSendToServer((char*) sendbuf);
	int sentSize = pbsb->bsend(sock);
	delete pbsb;
	pbsb = NULL;

	//prompt errors
	if (sentSize < sendcount) {

		//todo: bug prone
		/*cerr << "TCPProxy::sendTo(): error on BdSendToServer::bsend(...): "
		 << strerror(errno) << endl;*/
	}

	return sentSize;
}

pthread_t ZHTClient::start_receiver_thread(int port){
	//recv_args arg;
	thread_arg.client_listen_port = port;

	pthread_t th;
	pthread_create(&th, NULL, ZHTClient::client_receiver_thread, (void*)&thread_arg);

	//pthread_join(th, NULL);
	// pthread_create(&id1, NULL, ZHTClient::listeningSocket, (void *)&_param);
	return th;
}

bool CLIENT_RECEIVE_RUN = true; //needed for global variables.
map<string, string> req_results_map; //needed for global variables.

void * ZHTClient::client_receiver_thread(void* argum) {
	cout << "client thread started."<<endl;
	recv_args *args = (recv_args *) argum;
	int port = args->client_listen_port;

	struct sockaddr_in svrAdd_in;
	int svrSock = -1;
	//printf("success 1\n");
	memset(&svrAdd_in, 0, sizeof(struct sockaddr_in));
	svrAdd_in.sin_family = AF_INET;
	svrAdd_in.sin_addr.s_addr = INADDR_ANY;
	svrAdd_in.sin_port = htons(port);
	//printf("success 2\n");
	svrSock = socket(AF_INET, SOCK_STREAM, 0);
	if (bind(svrSock, (struct sockaddr*) &svrAdd_in, sizeof(struct sockaddr))
			< 0) {
		perror("bind error");
		exit(-1);
	}
	//printf("bind \n");

	if (listen(svrSock, 8000) < 0) {
		printf("listen error\n");
	}
	//printf("listen \n");

	/* make the socket reusable */
	int reuse_addr = 1;
	int ret = setsockopt(svrSock, SOL_SOCKET, SO_REUSEADDR, &reuse_addr,
			sizeof(reuse_addr));
	if (ret < 0) {
		cerr << "reuse socket failed: [" << svrSock << "], " << endl;
		return NULL;
	}

	sockaddr *in_addr = (sockaddr *) calloc(1, sizeof(struct sockaddr));
	socklen_t in_len = sizeof(struct sockaddr);
	int infd;
	struct sockaddr_in client_addr;
	socklen_t clilen;
	int connfd = -1;

	while (CLIENT_RECEIVE_RUN) {

		connfd = accept(svrSock, (struct sockaddr *) &client_addr, &clilen);
		string result;
		int recvcount = loopedrecv(connfd, NULL, result);
		ZPack pack;
		pack.ParseFromString(result);

		for(int i =0; i<pack.batch_item_size(); i++){
			BatchItem item = pack.batch_item(i);
			cout << "Client listening thread received: key = "<< item.key()<<endl;
			if(0 == item.opcode().compare("003")){//if lookup. Maybe need to return other status in string form.
				req_results_map.insert(std::pair<string, string>(item.key(), item.val()));
				cout << "Client listening thread received: key = "<< item.key()<<endl;
				cout << "Value = "<< item.val()<<endl;
			}
		}

		CLIENT_RECEIVE_RUN = false;
		//How to handle received result?
	}
	close(connfd);
	//return 0;
}

int results_handler(string result){
	ZPack res;
	res.ParseFromString(result);

	return 0;
}

int ZHTClient::addToBatch(Request item, ZPack &batch) {
	BatchItem* newItem = batch.add_batch_item();
	newItem->set_key(item.key);
	newItem->set_val(item.val);
	newItem->set_client_ip(item.client_ip);
	newItem->set_client_port(item.client_port);
	newItem->set_opcode(item.opcode);
	newItem->set_max_wait_time(item.max_tolerant_latency);
	newItem->set_consistency(item.consistency);
	return 0;
}

int ZHTClient::makeBatch(list<Request> src, ZPack &batch) {

	list<Request>::iterator it;
	for (it = src.begin(); it != src.end(); it++) {
		addToBatch(*it, batch);
	}

	if (0 != batch.batch_item_size()) {
		batch.set_pack_type(ZPack_Pack_type_BATCH_REQ);
		batch.set_key(src.front().key); // use any key for batch's key, since they all go to one place.
	} else
		batch.set_pack_type(ZPack_Pack_type_SINGLE);

	return 0;
}

int ZHTClient::teardown() {

	if (_proxy->teardown())
		return 0;
	else
		return -1;
}

int ZHTClient::send_batch(ZPack &batch) {

	// set batch type for message
	batch.set_pack_type(ZPack_Pack_type_BATCH_REQ);

	// serialize the message to string
	string msg = batch.SerializeAsString();

	char *buf = (char*) calloc(_msg_maxsize, sizeof(char));
	size_t msz = _msg_maxsize;

	ZPack temp;
	temp.ParseFromString(msg.c_str());

	/*send to and receive from*/
	//_proxy->sendrecv(msg.c_str(), msg.size(), buf, msz);
	TCPProxy tcp;

	ZHTUtil zu;
	HostEntity he = zu.getHostEntityByKey(msg);
	int sock = tcp.getSockCached(he.host, he.port);
	tcp.sendTo(sock, (void*)msg.c_str(), msg.size());
//	sock
//	sendTo_BD();


	cout << "cpp_zhtclient.cpp: ZHTClient::send_batch():  " << buf << endl;
	return 0;

	//...parse status and result
	/*
	 string sstatus;

	 string srecv(buf);

	 if (srecv.empty()) {

	 sstatus = Const::ZSC_REC_SRVEXP;
	 } else {

	 result = srecv.substr(3); //the left, if any, is lookup result or second-try zpack
	 sstatus = srecv.substr(0, 3); //status returned, the first three chars, like 001, -98...
	 }

	 free(buf);
	 return sstatus;
	 */
}

//This function accumulate requests and send in batch when a condition is satisfied
//It use a hash map track status of active requests
//This method is called from a client service loop, which keep receiving requests.

class AggregatedSender {
public:

	int req_handler(Request in_req, int policy); //call this every time when a request is sent by the client
	int batch_monitor(int para, ZHTClient zc);
	int init(void);
	//A monitor thread, watch the condition and decide when to send the batch.
	//Running from the beginning.
	//multiple policies applicable.
private:

	//Track request status
	map<string, int> req_stats_map;
	//map<string, string> req_results_map;
	//Batch container
	//list<Request> send_list;
	ZPack req_batch;	//contains a list of requests
	pthread_mutex_t mutex_monitor_condition = PTHREAD_MUTEX_INITIALIZER;
	pthread_mutex_t mutex_batch = PTHREAD_MUTEX_INITIALIZER;
	double batch_deadline;// = TIME_MAX;// batch -wide deadline, a absolute time stamp.
	bool MONITOR_RUN;	// = false;
	int latency_time;// = 500; //in microsec. Batch must go by this much time before deadline. It's left for transferring and svr side processing.
};

int AggregatedSender::init() {
	//this->mutex_batch = PTHREAD_MUTEX_INITIALIZER;
	//this->mutex_monitor_condition = PTHREAD_MUTEX_INITIALIZER;
	this->batch_deadline = TIME_MAX;// batch -wide deadline, a absolute time stamp.
	this->MONITOR_RUN = false;
	this->latency_time = 500;
	return 0;
}

int AggregatedSender::req_handler(Request in_req, int policy) {

	in_req.arrival_time = TimeUtil::getTime_usec();

	this->req_stats_map.insert(std::pair<string, int>(in_req.key, 1));//1: stay in queue, 0 results returned.

	//push req to queue, and update condition related parameters.
	ZHTClient::addToBatch(in_req, this->req_batch);

	double in_req_deadline = in_req.arrival_time
			+ in_req.max_tolerant_latency * 1000; //max_tolerant_latency is in ms

	if (this->batch_deadline >= in_req_deadline) { // new req is the most urgent one

		pthread_mutex_lock(&mutex_monitor_condition);

		this->batch_deadline = in_req_deadline;

		pthread_mutex_unlock(&mutex_monitor_condition);

	}

	return 0;
}

int AggregatedSender::batch_monitor(int para, ZHTClient zc) {
	bool condition = false;
	while (this->MONITOR_RUN) {

		condition = (this->batch_deadline - TimeUtil::getTime_usec()
				<= this->latency_time); //time is out!

		if (condition) {
			pthread_mutex_lock(&mutex_batch);

			zc.send_batch(this->req_batch);

			ZPack p = this->req_batch;
			this->req_batch = ZPack();

			pthread_mutex_lock(&mutex_monitor_condition);

			this->batch_deadline = TIME_MAX;

			pthread_mutex_unlock(&mutex_monitor_condition);

			pthread_mutex_unlock(&mutex_batch);
		}

	}

	return 0;
}

