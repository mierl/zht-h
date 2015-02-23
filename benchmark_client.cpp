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
 * benchmark_client.cpp
 *
 *  Created on: Sep 23, 2012
 *      Author: Tony
 *      Contributor: Xiaobingo, KWang, DZhao
 */

#include <iostream>
#include <fstream>
#include <stdio.h>

#include <vector>
#include <error.h>
#include <getopt.h>
#include <unistd.h>

#include "zpack.pb.h"
#include "Util.h"

#include "cpp_zhtclient.h"
#include "ZHTUtil.h"

#include <fstream>
#include <sstream>
#include <string>

using namespace std;
using namespace iit::datasys::zht::dm;

ZHTClient zc;
int numOfOps = -1;
int keyLen = 10;
int valLen = 118;
vector<string> pkgList;
vector<Request> RAND_REQ_LIST;
bool IS_BATCH = false;
bool is_single_batch = false;
ZPack batch_pack;
int client_listen_port = 50009;
Batch BATCH;
bool IS_STATIC_QOS = false;
int STATIC_QOS = 10;

monitor_args DynamicBatchMonitorArgs;
string LogFilePathPrefix = "";
void init_packages(bool is_batch) {

	srand(time(NULL));

	int QoS_Latency[] = { 5, 500, 500, 500, 500 }; //Don't set 0 for now, not implemented yet.

	if (is_batch) {
		//Batch batch;
		BATCH.init();
		string ip = ZHTUtil::getLocalIP();
		for (int i = 0; i < numOfOps; i++) {
			Request req;
			req.opcode = "003"; //003: insert, 3 bytes
			req.client_ip = ip; ////"localhost", 16 bytes;
			req.client_port = client_listen_port; //4 bytes
			req.consistency = BatchItem_Consistency_level_EVENTUAL; //4 bytes
			if (IS_STATIC_QOS) {	//4 bytes
				req.qos_latency = STATIC_QOS;
			} else
				req.qos_latency = QoS_Latency[rand() % 5]; //randomly set max_latency, but

			req.key = HashUtil::randomString(keyLen); //
			req.val = HashUtil::randomString(valLen); //
			req.transferSize = keyLen + valLen + 28;
			if (is_single_batch) { // only send one batch, only one server.
				BATCH.addToBatch(req);
				//cout << "single batching: req push: " << i << endl;
			} else { //dynamic batching for multiple servers
				//cout << "dynamic batching: req push: " <<i<<endl;
				RAND_REQ_LIST.push_back(req);
			}
		}

	} else {
		for (int i = 0; i < numOfOps; i++) {

			ZPack single_pack;
			single_pack.set_key(HashUtil::randomString(keyLen));
			single_pack.set_val(HashUtil::randomString(valLen));

			pkgList.push_back(single_pack.SerializeAsString());
		}
	}
}

int benchmarkInsert() {

	double start = 0;
	double end = 0;
	start = TimeUtil::getTime_msec();
	int errCount = 0;

	int c = 0;
	vector<string>::iterator it;
	int x = 2;
	for (it = pkgList.begin(); it != pkgList.end(); it++) {

		c++;

		string pkg_str = *it;
		ZPack pkg;
		pkg.ParseFromString(pkg_str);
		//sleep(x);

		double s2 = TimeUtil::getTime_msec();
		int ret = zc.insert(pkg.key(), pkg.val());
		double e2 = TimeUtil::getTime_msec();
		//cout << "Single insert cost: "<<e2 - s2 <<" ms."<<endl;
		//cout << "insert, val = "<<pkg.val()<<endl;
		if (ret < 0) {
			errCount++;
		}
	}

	end = TimeUtil::getTime_msec();

	char buf[200];
	sprintf(buf, "Inserted packages, %d, %d, cost(ms), %f", numOfOps - errCount,
			numOfOps, end - start);
	cout << "Average latency: " << ((end - start) / numOfOps) << "ms." << endl;
	cout << buf << endl;

	return 0;
}

int benchmarkAppend() {

	vector<string> pkgList_append = pkgList;

	vector<string>::iterator it;
	//for (it = pkgList.begin(); it != pkgList.end(); it++) {

	//	ZPack package;
	//	package.ParseFromString((*it));

	//	package. add_listitem("item-----6-append");

	//	pkgList_append.push_back(package.SerializeAsString());
	//}

	double start = 0;
	double end = 0;
	start = TimeUtil::getTime_msec();
	int errCount = 0;

	int c = 0;
	for (it = pkgList_append.begin(); it != pkgList_append.end(); it++) {

		c++;

		string pkg_str = *it;
		ZPack pkg;
		pkg.ParseFromString(pkg_str);

		int ret = zc.append(pkg.key(), HashUtil::randomString(valLen));

		if (ret < 0) {
			errCount++;
		}
	}

	end = TimeUtil::getTime_msec();

	char buf[200];
	sprintf(buf, "Appended packages, %d, %d, cost(ms), %f", numOfOps - errCount,
			numOfOps, end - start);
	cout << buf << endl;

	return 0;
}

float benchmarkLookup() {

	double start = 0;
	double end = 0;
	start = TimeUtil::getTime_msec();
	int errCount = 0;

	int c = 0;
	vector<string>::iterator it;
	for (it = pkgList.begin(); it != pkgList.end(); it++) {

		string result;
		c++;

		string pkg_str = *it;
		ZPack pkg;
		pkg.ParseFromString(pkg_str);

		int ret = zc.lookup(pkg.key(), result);
		//cout << "1 Found result, value: "<< result << endl;
		if (ret < 0) {
			errCount++;
		} else if (result.empty()) { //empty string
			errCount++;
		}

	}

	end = TimeUtil::getTime_msec();

	char buf[200];
	sprintf(buf, "Lookuped packages, %d, %d, cost(ms), %f", numOfOps - errCount,
			numOfOps, end - start);
	//cout <<   << endl;

	return 0;
}

float benchmarkRemove() {

	double start = 0;
	double end = 0;
	start = TimeUtil::getTime_msec();
	int errCount = 0;

	int c = 0;
	vector<string>::iterator it;
	for (it = pkgList.begin(); it != pkgList.end(); it++) {

		string result;
		c++;

		string pkg_str = *it;
		ZPack pkg;
		pkg.ParseFromString(pkg_str);

		int ret = zc.remove(pkg.key());

		if (ret < 0) {
			errCount++;
		}
	}

	end = TimeUtil::getTime_msec();

	char buf[200];
	sprintf(buf, "Removed packages, %d, %d, cost(ms), %f", numOfOps - errCount,
			numOfOps, end - start);
	cout << buf << endl;

	return 0;
}

int benchmark_single_batch() {
	pthread_t th = zc.start_receiver_thread(client_listen_port);
	//sleep(1);
	//cout << "starting batch benchmark" << endl;

	int n = batch_pack.batch_item_size();
	//cout << "Number of batch items" <<  n << endl;

	double start = TimeUtil::getTime_msec();
	//Batch::send_batch(batch_pack);
	//TCPProxy tcp = TCPProxy();
	BATCH.send_batch();
	CLIENT_RECEIVE_RUN = false;
	pthread_join(th, NULL);
	double end = TimeUtil::getTime_msec();

	cout << "Batch benchmark time in ms: " << end - start << endl;
	cout << "Average latency: " << (end - start) / numOfOps << endl;

	return 0;
}

int writeReqLogToFile(string pathPrefix, list<req_latency_rec> log) {
	ofstream outFile;
	string filePath = pathPrefix + "_ReqLog.txt";
	outFile.open(filePath.c_str(), std::ofstream::out | std::ofstream::app);

	list<req_latency_rec>::iterator it;
	for (it = REQ_LATENCY_LOG.begin(); it != REQ_LATENCY_LOG.end(); it++) {
		std::stringstream s;
		s << (*it).qos_latency << " " << (*it).actual_latency;
		outFile << s.str() << endl;
		//std::to_string((*it).qos_latency) << " " << std::to_string((*it).qos_latency);
	}
	outFile.close();
	return 0;
}

int writeBatchLogToFile(string pathPrefix, list<batch_latency_record> log) {
	ofstream outFile;
	string filePath = pathPrefix + "_BatchLog.txt";
	outFile.open(filePath.c_str(), std::ofstream::out | std::ofstream::app);

	list<batch_latency_record>::iterator it;
	for (it = BATCH_LATENCY_LOG.begin(); it != BATCH_LATENCY_LOG.end(); it++) {
		std::stringstream s;
		s << (*it).num_item << " " << (*it).actual_latency;
		outFile << s.str() << endl;
		//std::to_string((*it).qos_latency) << " " << std::to_string((*it).qos_latency);
	}
	outFile.close();
	return 0;
}

int benchmark_dynamic_batching(void) {
	AggregatedSender sender;
	sender.init();
	pthread_t th_recv = zc.start_receiver_thread(client_listen_port);
	sleep(1);
//	monitor_args DynamicBatchMonitorArgs;
//	DynamicBatchMonitorArgs.batch_size = 20000;
//	DynamicBatchMonitorArgs.num_item = 1000;
//	DynamicBatchMonitorArgs.policy_index = 5;
	//1: deadline only; 2: deadline + nbatch_size_um_item;
	//3: deadline + batch_size_bytes; 4: num + size_bytes; 5: num_item only
	CONDITION_PARAM = DynamicBatchMonitorArgs;

	//pthread_t th_monit = sender.start_batch_monitor_thread(	DynamicBatchMonitorArgs);

//	cout << "start_batch_monitor_thread done, " << "RAND_REQ_LIST.size() = "
//			<< RAND_REQ_LIST.size() << ", num_item = "
//			<< DynamicBatchMonitorArgs.num_item << ", batch_size = "
//			<< DynamicBatchMonitorArgs.batch_size << ", policy_index = "
//			<< DynamicBatchMonitorArgs.policy_index << endl << endl;
	string result;
	int i = 0;

	double start = TimeUtil::getTime_msec();
	for (vector<Request>::iterator it = RAND_REQ_LIST.begin();
			it != RAND_REQ_LIST.end(); ++it, i++) {
		//cout << "Req_" << i << ": max_tolerant_latency = "<< (*it).max_tolerant_latency << endl;
		//usleep(1);
		sender.req_handler(*it, result);
	}
	double end = TimeUtil::getTime_msec();
	cout << "Finished " << numOfOps << " requests in " << end - start
			<< "ms, client throughput = " << 1000 * numOfOps / (end - start)
			<< " ops/s, avg latency = " << (end - start) / numOfOps << " ms."
			<< endl << endl;
//	cout.precision(25);
//	cout << start<<endl;
//	cout << end<<endl;

	sleep(3);
	MONITOR_RUN = false;
	//pthread_join(th_monit, NULL);

	sleep(5);
	CLIENT_RECEIVE_RUN = false;
	cout << "Cancel receiver thread now..." << endl;
	//pthread_cancel(th_recv);
	pthread_join(th_recv, NULL);//wait... won't run into here because it still wait at accept()
	if (true == RECORDING_LATENCY) {		//write latency log to a file.
		cout << "Writing latency logs ...";
		writeReqLogToFile(LogFilePathPrefix, REQ_LATENCY_LOG);
		writeBatchLogToFile(LogFilePathPrefix, BATCH_LATENCY_LOG);
		cout<<"done."<< endl;
	}

	return 0;
}

int benchmark(string &zhtConf, string &neighborConf) {

	srand(getpid() + TimeUtil::getTime_usec());

	if (zc.init(zhtConf, neighborConf) != 0) {

		cout << "ZHTClient initialization failed, program exits." << endl;
		return -1;
	}

	init_packages(IS_BATCH);

	if (IS_BATCH) {
		if (is_single_batch) {
			benchmark_single_batch();
		} else
			benchmark_dynamic_batching();

	} else {
		benchmarkInsert();

		//benchmarkLookup();

		//benchmarkAppend();

		//benchmarkRemove();
	}

	zc.teardown();

	return 0;

}

void printUsage(char *argv_0);

int main(int argc, char **argv) {

	extern char *optarg;

	int printHelp = 0;

	string zhtConf = "";
	string neighborConf = "";

	IS_BATCH = false;
	int c;
	while ((c = getopt(argc, argv, "z:n:o:v:b:s:i:p:l:S:Q:X:h")) != -1) {
		switch (c) {
		case 'z':
			zhtConf = string(optarg);
			break;
		case 'n':
			neighborConf = string(optarg);
			break;
		case 'o':
			numOfOps = atoi(optarg);
			break;
		case 'v':
			valLen = atoi(optarg);
			break;
		case 'b': {
			IS_BATCH = true;
			string batch_type = "";
			batch_type = string(optarg);
			if (0 == batch_type.compare("S")) {
				is_single_batch = true;
				cout << "Single batching." << endl;
			} else if (0 == batch_type.compare("D")) {
				is_single_batch = false;
				cout << "Dynamic batching." << endl;
			}
		}
			break;
		case 's':
			DynamicBatchMonitorArgs.batch_size = atoi(optarg);
			cout << "Dynamic batch size: " << DynamicBatchMonitorArgs.batch_size
					<< endl;
			break;
		case 'i':
			DynamicBatchMonitorArgs.num_item = atoi(optarg);
			cout << "Dynamic batch number of items: "
					<< DynamicBatchMonitorArgs.num_item << endl;
			break;
		case 'p':
			DynamicBatchMonitorArgs.policy_index = atoi(optarg);
			cout << "Dynamic batch policy: "
					<< DynamicBatchMonitorArgs.policy_index << endl;
			break;
		case 'l': {
			RECORDING_LATENCY = true;
			LogFilePathPrefix = string(optarg);
			cout << "Log files Path prefix: " << LogFilePathPrefix << endl;

		}
			break;
		case 'S': {
			SYS_OVERHEAD = atoi(optarg); //SYS_OVERHEAD
		}
			break;

		case 'Q': {
			IS_STATIC_QOS = true;
			STATIC_QOS = atoi(optarg);
		}
			break;

		case 'X': { // Special operations
			if (0 == string(optarg).compare("V")) {
				VIRTUAL = true;
				cout << "Virtual mode..." << endl;
			}

		}
			break;

		case 'h':
			printHelp = 1;
			break;
		default:
			fprintf(stderr, "Illegal argument \"%c\"\n", c);
			printUsage(argv[0]);
			exit(1);
		}
	}

	int helpPrinted = 0;
	if (printHelp) {
		printUsage(argv[0]);
		helpPrinted = 1;
	}

	try {
		if (!zhtConf.empty() && !neighborConf.empty() && numOfOps != -1) {

			benchmark(zhtConf, neighborConf);

		} else {

			if (!helpPrinted)
				printUsage(argv[0]);
		}
	} catch (exception& e) {

		fprintf(stderr, "%s, exception caught:\n\t%s",
				"benchmark_client.cpp::main", e.what());
	}

}

void printUsage(char *argv_0) {

	fprintf(stdout, "Usage:\n%s %s\n", argv_0,
			"-z zht.conf -n neighbor.conf -o number_of_operations -v length_value [-b (batching) s(single batch)/d(dynamic)] [-h(help)] ");
}
