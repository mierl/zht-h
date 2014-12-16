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
 * cpp_zhtclient.h
 *
 *  Created on: Sep 16, 2012
 *      Author: Xiaobingo
 *      Contributor: Tony, KWang, DZhao
 */

#ifndef ZHTCLIENT_H_
#define ZHTCLIENT_H_

#include <stdint.h>
#include <map>
#include <string>
using namespace std;

#include "lru_cache.h"

#include "ProxyStubFactory.h"
#include "zpack.pb.h"
/*
 *
 */

//Tony: request for batch processing
class Request {
public:
	string client_ip;
	int client_port;
	long seq_num; //used by OHT
	string opcode;
	string key;
	string val;
	int max_tolerant_latency; //The longest time this request can wait, in ms.
	double arrival_time;
	//enum Consistency_level {STRONG, WEAK, EVENTUAL};
	enum BatchItem_Consistency_level consistency; //From zpack.pb.h

};

typedef struct recv_thread_args {

	int client_listen_port;

} recv_args;

class ZHTClient {

public:
	ZHTClient();

	ZHTClient(const string &zhtConf, const string &neighborConf);
	virtual ~ZHTClient();

	int init(const string &zhtConf, const string &neighborConf);
	int init(const char *zhtConf, const char *neighborConf);
	int lookup(const string &key, string &result);
	int lookup(const char *key, char *result);
	int remove(const string &key);
	int remove(const char *key);
	int insert(const string &key, const string &val);
	int insert(const char *key, const char *val);
	int append(const string &key, const string &val);
	int append(const char *key, const char *val);
	int compare_swap(const string &key, const string &seen_val,
			const string &new_val, string &result);
	int compare_swap(const char *key, const char *seen_val, const char *new_val,
			char *result);
	int state_change_callback(const string &key, const string &expected_val,
			int lease);
	int state_change_callback(const char *key, const char *expeded_val,
			int lease);
	int teardown();

	//Tony: ZHT-H addtion
	//TODO: implement following methods.
	int init();
	//int send_batch(ZPack &batch); // called by ZHTClient.commonOp, but maybe not here.
	//static int makeBatch(list<Request> src, ZPack &batch);
	//static int addToBatch(Request item, ZPack &batch); // by GPB
	static void* client_receiver_thread(void* arg);
	pthread_t start_receiver_thread(int port);

	//end.
private:
	int commonOp(const string &opcode, const string &key, const string &val,
			const string &val2, string &result, int lease);
	string commonOpInternal(const string &opcode, const string &key,
			const string &val, const string &val2, string &result, int lease);
	string extract_value(const string &returnStr);

	//Tony: ZHT-H addtion
	recv_args thread_arg;
	//map<string, string> req_ret_status_map;

private:
	ProtoProxy *_proxy;
	int _msg_maxsize;
};

typedef struct monitor_send_thread_args {

	int policy_index;
	int num_item;
	unsigned long batch_size;

} monitor_args;

class Batch {
public:
	Batch();
	int init(void);
	bool check_condition(int policy_index, int num_item,
			unsigned long batch_size);
	bool check_condition_deadline(void);

	//A series of methods, test the condition by different policy
	int clear_batch(void);
	int addToBatch(Request item);
	int addToSwapBatch(Request item);
	int send_batch(void);
	int makeBatch(list<Request> src);
	static int send_batch(ZPack &batch);
	double batch_deadline;
	unsigned int batch_num_item;
	unsigned long batch_size_byte;
	int latency_time;
	pthread_mutex_t mutex_batch_local;
private:
	ZPack req_batch;
	//ZPack req_batch_swap;
	//bool in_sending;
	//double batch_deadline;
	//pthread_mutex_t mutex_batch_local;
	bool check_condition_num_item(int max_item);
	bool check_condition_deadline_num_item(int max_item);
	bool check_condition_deadline_batch_size_byte(unsigned long max_size);
	bool check_condition_num_item_batch_size_byte(int max_item,
			unsigned long max_size_byte);

};

class AggregatedSender {
public:

	int req_handler(Request in_req, string & immediate_result); //call this every time when a request is sent by the client
	int init(void);
	pthread_t start_batch_monitor_thread(monitor_args args);
	int stop_batch_monitor_thread(void);
	//A monitor thread, watch the condition and decide when to send the batch.Running from the beginning. multiple policies applicable.

private:
	static void* batch_monitor_thread(void* argu);
	//Track request status
	map<string, int> req_stats_map;
	//map<string, string> req_results_map;
	//Batch container
	//list<Request> send_list;
	ZPack req_batch;	//contains a list of requests

	//pthread_mutex_t mutex_monitor_condition;	// = PTHREAD_MUTEX_INITIALIZER;
	//pthread_mutex_t mutex_batch_all;	// = PTHREAD_MUTEX_INITIALIZER;
	//pthread_mutex_t mutex_in_sending;
	//double batch_deadline;// = TIME_MAX;// batch -wide deadline, a absolute time stamp.
	// = false;
	int latency_time;// = 500; //in microsec. Batch must go by this much time before deadline. It's left for transferring and svr side processing.
	monitor_args mon_args;
};

//Global variables: for threads accessing.
double const TIME_MAX = 9999999999000000;//a reasonably long time in the future.
extern bool MONITOR_RUN;
extern bool CLIENT_RECEIVE_RUN;
extern vector<Batch> BATCH_VECTOR_GLOBAL;//Has to be global, since it must be accessed by some threads. It hold multiple batches, each for a dest server.
//Tony: request for batch processing end.

#endif /* ZHTCLIENT_H_ */
