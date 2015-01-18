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

//include <gsl/gsl_fit.h> //For linear regression
using namespace iit::datasys::zht::dm;

pthread_mutex_t mutex_send_update;
int MSG_MAXSIZE = 1000 * 1000 * 2;

bool CLIENT_RECEIVE_RUN = false;
bool MONITOR_RUN = false;
vector<Batch> BATCH_VECTOR_GLOBAL;
list<req_latency_rec> REQ_LATENCY_LOG;
list<batch_latency_record> BATCH_LATENCY_LOG;
bool RECORDING_LATENCY = true;
float SYS_OVERHEAD = 1;
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
	MSG_MAXSIZE = Env::get_msg_maxsize();
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
	pthread_mutex_init(&(mutex_send_update), NULL);
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
	//double s2 = TimeUtil::getTime_msec();
	//Tony: changed commu method for big msg
	TCPProxy tcp;
	ZHTUtil zu;
	HostEntity he = zu.getHostEntityByKey(msg);
	int sock = tcp.getSockCached(he.host, he.port);
	tcp.sendTo(sock, (void*) msg.c_str(), msg.size());
	string res;

	int recvcount = loopedrecv(sock, NULL, res);
	//_proxy->sendrecv(msg.c_str(), msg.size(), buf, msz);
	//Tony: changed commu method for big msg

	//double e2 = TimeUtil::getTime_msec();
	//cout << " ZHTClient::commonOpInternal: recvsend: _proxy->sendrecv(): cost: "<< e2 - s2 << " ms." << endl;
	/*...parse status and result*/
	string sstatus;

	//string res(buf);

	if (res.empty()) {

		sstatus = Const::ZSC_REC_SRVEXP;
	} else {

		result = res.substr(3); //the left, if any, is lookup result or second-try zpack
		sstatus = res.substr(0, 3); //status returned, the first three chars, like 001, -98...
	}

	free(buf);
	return sstatus;
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

pthread_t ZHTClient::start_receiver_thread(int port) {
	//recv_args arg;
	thread_arg.client_listen_port = port;
	CLIENT_RECEIVE_RUN = true;
	pthread_t th;
	pthread_create(&th, NULL, ZHTClient::client_receiver_thread,
			(void*) &thread_arg);

	//pthread_join(th, NULL);
	// pthread_create(&id1, NULL, ZHTClient::listeningSocket, (void *)&_param);
	return th;
}

//bool CLIENT_RECEIVE_RUN = true; //needed for global variables.

void * ZHTClient::client_receiver_thread(void* argum) {
	//cout << "client thread started."<<endl;
	recv_args *args = (recv_args *) argum;
	int port = args->client_listen_port;

	struct sockaddr_in svrAdd_in;
	int svrSock = -1;
	int reuse_addr = 1;
	//printf("success 1\n");
	memset(&svrAdd_in, 0, sizeof(struct sockaddr_in));
	svrAdd_in.sin_family = AF_INET;
	svrAdd_in.sin_addr.s_addr = INADDR_ANY;
	svrAdd_in.sin_port = htons(port);
	//printf("success 2\n");
	svrSock = socket(AF_INET, SOCK_STREAM, 0);

	int ret = setsockopt(svrSock, SOL_SOCKET, SO_REUSEADDR, &reuse_addr,
			sizeof(reuse_addr));
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
	int i = 0;
	//connfd = accept(svrSock, (struct sockaddr *) &client_addr, &clilen);
	while (CLIENT_RECEIVE_RUN) {
		//cout << "client_receiver_thread: while(), accept..." << endl;
		connfd = accept(svrSock, (struct sockaddr *) &client_addr, &clilen);
		string result;
		//cout<<"before loopedrecv..."<<endl;
		int recvcount = loopedrecv(connfd, NULL, result);
//		cout << "after loopedrecv, ...recvcount = " << recvcount
//				<< ", result.size() = "<<result.size()<<endl;

		ZPack res_pack;
		if (res_pack.ParseFromString(result)) {
			//cout << "res_pack unpack: true" << endl;
		}

		if (true == RECORDING_LATENCY) {
			double batch_arr_time = TimeUtil::getTime_msec();
//			cout.precision(17);
//			cout << ++i << "th batch, size = " << result.size()
//					<<", res_pack.batch_item_size() = "<<res_pack.batch_item_size()
//					<< ", started at " << res_pack.batch_start_time()
//					<< ", actual latency: "
//					<< batch_arr_time - res_pack.batch_start_time() << endl;

			//usleep(100);
			//res_pack.batch_item_size()
			//for (int j = 0; j < 1; j++);//do nothing, but help?

			for (int j = 0; j < res_pack.batch_item_size(); j++) { //recording latency for analysis
				BatchItem batch_item = res_pack.batch_item(j);
//				cout << "item_" << j + 1 << ", key = " << batch_item.key()
//						<< ", val = " << batch_item.val() << endl;
				request_latency_record rec;
				rec.qos_latency = batch_item.qos_latency();
				rec.actual_latency = batch_arr_time - batch_item.submit_time();
				//cout<< "submit_time: " << batch_item.submit_time()<<endl;
				REQ_LATENCY_LOG.push_back(rec);
			}

			batch_latency_record batch_rec;
			batch_rec.num_item = res_pack.batch_item_size();
			batch_rec.actual_latency = batch_arr_time
					- res_pack.batch_start_time();
			BATCH_LATENCY_LOG.push_back(batch_rec);
		}

//		cout << "Client received a batch, contains " << res.batch_item_size()
//				<< " items" << endl;

//		for (int i = 0; i < res.batch_item_size(); i++) {
//			BatchItem batch_item = res.batch_item(i);
//			cout << "item_" << i + 1 << ", key = " << batch_item.key()
//					<< ", val = " << batch_item.val() << endl;
//		}

		//CLIENT_RECEIVE_RUN = false;
		//How to handle received result?
	}
	//close(connfd);
	//return 0;
}

int results_handler(string result) {
	ZPack res;
	res.ParseFromString(result);

	return 0;
}

int ZHTClient::teardown() {

	if (_proxy->teardown())
		return 0;
	else
		return -1;
}

// test all servers and
int ZHTClient::batcherVectorInit() {
	//TODO: simple imp: assume all server network connections work the same way: only test one server and apply param to all servers

	//TODO: complete imp: do this for all servers.

	return 0;
}

double ZHTClient::expcTransLatency(void) {

//	this->trans_factor; //Calculated by linear regression, transferring latency = a*size+b
//	this-> trans_const;
//	this->batch_size_byte;
//	this->batch_size_record;
//
//	return this->trans_factor * this->batch_size_byte + this-> trans_const;

	return 0;
}

//This function accumulate requests and send in batch when a condition is satisfied
//It use a hash map track status of active requests
//This method is called from a client service loop, which keep receiving requests.

Batch::Batch() {
	this->init();
	pthread_mutex_init(&(this->mutex_batch_local), NULL);
	// mutex has to be initialized here, since Batch::init() is called by clear(), initialization will be done again.
}

int Batch::init(void) {
	this->req_batch.Clear();
	this->req_batch.set_pack_type(ZPack_Pack_type_BATCH_REQ);
	//this->req_batch_swap.set_pack_type(ZPack_Pack_type_BATCH_REQ);
	this->batch_deadline = TIME_MAX;
	this->batch_num_item = 0;
	this->batch_size_byte = 0;
	SYS_OVERHEAD = 2; // in ms
	this->batch_start_time = 0;
	//this->in_sending = false;

	return 0;
}

bool Batch::check_condition(int policy_index, int max_num_item,
		unsigned long max_batch_size) {
	bool condition = false;
	switch (policy_index) {
	case 1:
		condition = this->check_condition_deadline();
		break;
	case 2:
		condition = this->check_condition_deadline_num_item(max_num_item);
		break;
	case 3:
		condition = this->check_condition_deadline_batch_size_byte(
				max_batch_size);
		break;
	case 4:
		condition = this->check_condition_num_item_batch_size_byte(max_num_item,
				max_batch_size);
		break;
	case 5:
		condition = this->check_condition_num_item(max_num_item);
		break;
	default:
		condition = false;
		cout << "Invalid policy index, index = " << policy_index << endl;
		break;
	}
	return condition;
}

bool Batch::check_condition_deadline(void) {
	double t = TimeUtil::getTime_msec();

	if (this->batch_deadline - t <= SYS_OVERHEAD) {
//		cout.precision(23);
//		cout << "Deadline check: batch_deadline: " << this->batch_deadline
//				<< endl;
//		cout << "Deadline check: now: " << t << endl;
//
//		cout << "Deadline check: Time left: " << (this->batch_deadline - t)
//				<< ", this->latency_time: " << SYS_OVERHEAD << endl;
		return true;
	} else
		return false;
//	return (this->batch_deadline - TimeUtil::getTime_msec()
//			<= this->latency_time);

}

bool Batch::check_condition_deadline_num_item(int max_item) {
//if(this->batch_num_item > 0)
//cout<<" this->batch_num_item = "<< this->batch_num_item<<endl;
	return (this->check_condition_deadline() || this->batch_num_item >= max_item);

}

bool Batch::check_condition_deadline_batch_size_byte(unsigned long max_size) {

	return (this->check_condition_deadline()
			|| this->batch_size_byte >= max_size);

}

bool Batch::check_condition_num_item_batch_size_byte(int max_item,
		unsigned long max_size_byte) {
	return (this->batch_num_item >= max_item
			|| this->batch_size_byte >= max_size_byte);
}

bool Batch::check_condition_num_item(int max_item) {
	return (this->batch_num_item >= max_item);
}
int Batch::addToBatch(Request item) { //protected by local mutex

	item.submit_time = TimeUtil::getTime_msec();

	if (0 == this->batch_start_time) { //1st item, means the start of a batching.
		//cout << "1st item, batch starts at" << item.submit_time << endl;
		this->batch_start_time = item.submit_time;
		this->req_batch.set_batch_start_time(item.submit_time);
	}

	pthread_mutex_lock(&this->mutex_batch_local);

	BatchItem* newItem = this->req_batch.add_batch_item();
	newItem->set_key(item.key);
	newItem->set_val(item.val);
	newItem->set_client_ip(item.client_ip);
	newItem->set_client_port(item.client_port);
	newItem->set_opcode(item.opcode);
	newItem->set_qos_latency(item.qos_latency);
	newItem->set_consistency(item.consistency);

	newItem->set_submit_time(item.submit_time);
//Used to update batch deadline.
	double in_req_deadline = item.submit_time + item.qos_latency; // * 1000; //max_tolerant_latency is in ms

//cout <<"in_req_deadline: "<< in_req_deadline<<endl;
//cout <<"this->batch_deadline: "<<this->batch_deadline<<endl;
	if (this->batch_deadline > in_req_deadline) { // new req is the most urgent one
		this->batch_deadline = in_req_deadline;
	}

	this->batch_size_byte = this->req_batch.ByteSize();

	this->batch_num_item++;

	pthread_mutex_unlock(&this->mutex_batch_local);

	return 0;
}

//DO NOT USE.
//int Batch::addToSwapBatch(Request item) {
//	item.arrival_time = TimeUtil::getTime_usec();
//
//	BatchItem* newItem = this->req_batch_swap.add_batch_item(); //add_batch_item();
//	newItem->set_key(item.key);
//	newItem->set_val(item.val);
//	newItem->set_client_ip(item.client_ip);
//	newItem->set_client_port(item.client_port);
//	newItem->set_opcode(item.opcode);
//	newItem->set_max_wait_time(item.max_tolerant_latency);
//	newItem->set_consistency(item.consistency);
//
//	//Used to update batch deadline.
//	double in_req_deadline = item.arrival_time
//			+ item.max_tolerant_latency * 1000; //max_tolerant_latency is in ms
//
//	if (this->batch_deadline > in_req_deadline) { // new req is the most urgent one
//		this->batch_deadline = in_req_deadline;
//	}
//
//	this->batch_size_byte = this->req_batch_swap.ByteSize();
//
//	this->batch_num_item++;
//
//	return 0;
//}

//DO NOT USE. Only for internal benchmarking.
int Batch::makeBatch(list<Request> src) {

	list<Request>::iterator it;
	for (it = src.begin(); it != src.end(); it++) {
		this->addToBatch(*it);
	}

	if (0 != this->req_batch.batch_item_size()) {
		this->req_batch.set_pack_type(ZPack_Pack_type_BATCH_REQ);
		this->req_batch.set_key(src.front().key); // use any key for batch's key, since they all go to one place.
	} else
		this->req_batch.set_pack_type(ZPack_Pack_type_SINGLE);

	return 0;
}

int Batch::clear_batch(void) {
//Mutex is used in the beginning of send_batch, it can't be obtained here; and clear will only be called after send, so it's safe.
//pthread_mutex_lock(&this->mutex_batch_local);
	this->init();
//pthread_mutex_unlock(&this->mutex_batch_local);
	return 0;
}

int Batch::send_batch(void) {	//protected by local mutex

//this->in_sending = true;

//pthread_mutex_lock(&this->mutex_batch_local);

// serialize the message to string
	string msg = this->req_batch.SerializeAsString();

	char *buf = (char*) calloc(MSG_MAXSIZE, sizeof(char));
	size_t msz = MSG_MAXSIZE;

	ZPack temp;
	temp.ParseFromString(msg.c_str());
//	cout << "sending batch, batch_item_size = " << temp.batch_item_size()
//			<< endl;

	/*send to and receive from*/
//_proxy->sendrecv(msg.c_str(), msg.size(), buf, msz);
	TCPProxy tcp;

	ZHTUtil zu;
	HostEntity he = zu.getHostEntityByKey(msg);
	int sock = tcp.getSockCached(he.host, he.port);
	tcp.sendTo(sock, (void*) msg.c_str(), msg.size());

	this->clear_batch();
	//usleep(500000);

//pthread_mutex_unlock(&this->mutex_batch_local);

//cout << "cpp_zhtclient.cpp: ZHTClient::send_batch():  " << buf << endl;
//this->in_sending = false;
	return 0;
}

int Batch::send_batch(ZPack &batch) {

// set batch type for message
	batch.set_pack_type(ZPack_Pack_type_BATCH_REQ);

// serialize the message to string
	string msg = batch.SerializeAsString();

	char *buf = (char*) calloc(MSG_MAXSIZE, sizeof(char));
	size_t msz = MSG_MAXSIZE;

	ZPack temp;
	temp.ParseFromString(msg.c_str());

	/*send to and receive from*/
//_proxy->sendrecv(msg.c_str(), msg.size(), buf, msz);
	TCPProxy tcp;

	ZHTUtil zu;
	HostEntity he = zu.getHostEntityByKey(msg);
	int sock = tcp.getSockCached(he.host, he.port);
	tcp.sendTo(sock, (void*) msg.c_str(), msg.size());

//cout << "cpp_zhtclient.cpp: ZHTClient::send_batch():  " << buf << endl;
	return 0;
}

//Use GSL linear regression to calculate expected

int AggregatedSender::init() {
//pthread_mutex_init(&(this->mutex_monitor_condition), NULL);
//pthread_mutex_init(&(this->mutex_batch_all), NULL);
//pthread_mutex_init(&(this->mutex_in_sending), NULL);
//this->batch_deadline = TIME_MAX;// batch -wide deadline, a absolute time stamp.
	MONITOR_RUN = false;

	Batch init_batch = Batch();
	for (int i = 0; i < ConfHandler::NeighborVector.size(); i++) {
		BATCH_VECTOR_GLOBAL.push_back(init_batch);
	}

	return 0;
}

pthread_t AggregatedSender::start_batch_monitor_thread(monitor_args args) {
//recv_args arg;
	MONITOR_RUN = true;

	mon_args.batch_size = args.batch_size;
	mon_args.num_item = args.num_item;
	mon_args.policy_index = args.policy_index;

	pthread_t th;
	pthread_create(&th, NULL, AggregatedSender::batch_monitor_thread,
			(void *) &mon_args);

//pthread_join(th, NULL);
// pthread_create(&id1, NULL, ZHTClient::listeningSocket, (void *)&_param);
	return th;
}

int AggregatedSender::stop_batch_monitor_thread(void) {
	MONITOR_RUN = false;
	return 0;
}

int AggregatedSender::req_handler(Request in_req, string & immediate_result) {
//cout.precision(20);
	usleep(10);	//to fix the gap of mutex coverage
	if (0 == in_req.qos_latency) {
		cout << "req_handler: max_tolerant_latency = 0" << endl;
		//TODO: how to handle immediate return results for direct request?

	} else {
		//cout << "req_handler: regular req, latency != 0" << endl;
		int svr_index = HashUtil::genHash(in_req.key)
				% ConfHandler::NeighborVector.size();

		//addToBatch is protected by mutex, so this whole method is safe, don't need another mutex.
		BATCH_VECTOR_GLOBAL.at(svr_index).addToBatch(in_req);
	}

	return 0;
}

void* AggregatedSender::batch_monitor_thread(void* argu) {

	monitor_args* param = (monitor_args*) argu;

//ZHTClient zc;
	int policy_index = param->policy_index;	//param->policy_index;
	int num_item = param->num_item;
	unsigned long batch_size = param->batch_size;
	cout << "AggregatedSender::batch_monitor_thread: " << " num_item = "

	<< num_item << ", batch_size = " << batch_size << ", policy_index = "
			<< policy_index << endl << endl;

	bool condition = false;
	while (MONITOR_RUN) {
		//cout << "batch_monitor_thread: while(MONITOR_RUN), MONITOR_RUN = "<< MONITOR_RUN << endl;
		//Check for all batches and see if any condition is met, send batch if met.
		for (vector<Batch>::iterator it = BATCH_VECTOR_GLOBAL.begin();
				it != BATCH_VECTOR_GLOBAL.end(); ++it) {
			//cout << "batch_monitor_thread: for(vector<Batch>) " << endl;

			pthread_mutex_lock(&((*it).mutex_batch_local));

			condition = (*it).check_condition(policy_index, num_item,
					batch_size);

			if (condition) {
//				cout << "batch_monitor_thread: condition match, sending... batch_deadline  = "
//						<< (*it).batch_deadline << ", batch_num_item = " << (*it).batch_num_item
//						<< ", batch_size_byte = "<<(*it).batch_size_byte
//						<< endl;
				(*it).send_batch(); //Protected by mutex, no need to use in other places in this method.
			}

			pthread_mutex_unlock(&((*it).mutex_batch_local));

		}
	}
	cout << "batch_monitor_thread: end while, MONITOR_RUN = " << MONITOR_RUN
			<< endl;
//return 0;
}

