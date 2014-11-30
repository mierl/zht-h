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
 * HTWorker.cpp
 *
 *  Created on: Sep 10, 2012
 *      Author: Xiaobingo
 *      Contributor: Tony, KWang, DZhao
 */

#include "HTWorker.h"

#include "Const-impl.h"
#include "Env.h"
#include "ConfHandler.h"

#include <unistd.h>
#include <iostream>
#include <pthread.h>
#include <stdlib.h>

using namespace std;
using namespace iit::datasys::zht::dm;

WorkerThreadArg::WorkerThreadArg() :
		_stub(NULL) {
}

WorkerThreadArg::WorkerThreadArg(const ZPack &zpack, const ProtoAddr &addr,
		const ProtoStub * const stub) :
		_zpack(zpack), _addr(addr), _stub(stub) {
}

WorkerThreadArg::~WorkerThreadArg() {
}

NoVoHT* HTWorker::PMAP = NULL;

HTWorker::QUEUE* HTWorker::PQUEUE = new QUEUE();

bool HTWorker::FIRST_ASYNC = false;

int HTWorker::SCCB_POLL_INTERVAL = Env::get_sccb_poll_interval();

HTWorker::HTWorker() :
		_stub(NULL), _instant_swap(get_instant_swap()) {

	init_me();
}

HTWorker::HTWorker(const ProtoAddr& addr, const ProtoStub* const stub) :
		_addr(addr), _stub(stub), _instant_swap(get_instant_swap()) {

	init_me();
}

HTWorker::~HTWorker() {
}

string HTWorker::run(const char *buf) {

	ZPack zpack;
	string buff(buf);
	zpack.ParseFromString(buff);

	string result;

	if(ZPack_Pack_type_BATCH_REQ  == zpack.pack_type()){//batch
//		cout << "HTWrorker::run(): ZPack_Pack_type_BATCH_REQ received."<< endl;
//		cout << "Batch contains "<< zpack.batch_item_size() << " items."<<endl;
//		cout <<"zpack.key: "<< zpack.key() <<endl;
//		cout <<"zpack.batch_item(i).val: "<<zpack.batch_item(0).val() << endl<< endl;
//
//		cout << "printing batch items received... " << endl;
//		for(int i =0; i < zpack.batch_item_size(); i++){
//			BatchItem b = zpack.batch_item(i);
//			cout <<"batch item recieved key: " << b.key() << endl;
//			cout << "batch item received value: " << b.val() << endl;
//		}
//
//		result = Const::ZSC_REC_UOPC; // "OK";
		result = process_batch(zpack);
	}else if(ZPack_Pack_type_SINGLE == zpack.pack_type()){//single

	if (zpack.opcode() == Const::ZSC_OPC_LOOKUP) {

		result = lookup(zpack);
	} else if (zpack.opcode() == Const::ZSC_OPC_INSERT) {

		result = insert(zpack);
	} else if (zpack.opcode() == Const::ZSC_OPC_APPEND) {

		result = append(zpack);
	} else if (zpack.opcode() == Const::ZSC_OPC_CMPSWP) {

		result = compare_swap(zpack);
	} else if (zpack.opcode() == Const::ZSC_OPC_REMOVE) {

		result = remove(zpack);
	} else if (zpack.opcode() == Const::ZSC_OPC_STCHGCB) {

		result = state_change_callback(zpack);
	} else {

		result = Const::ZSC_REC_UOPC;
	}
	}
	return result;
}

string HTWorker::process_batch(const ZPack &zpack){
	cout << "HTWrorker::run(): ZPack_Pack_type_BATCH_REQ received."<< endl;
	cout << "Batch contains "<< zpack.batch_item_size() << " items."<<endl;
	cout << "iterating over batch items and processing... " << endl;

	ZPack response_pack;
	response_pack.set_pack_type(ZPack_Pack_type_BATCH_REQ);

	int count = 0;
	for(int i =0; i < zpack.batch_item_size(); i++){
		BatchItem batch_item = zpack.batch_item(i);
		ZPack batch_item_zpack;
		batch_item_zpack.set_key(batch_item.key());
		batch_item_zpack.set_val(batch_item.val());
		batch_item_zpack.set_opcode(batch_item.opcode());
		batch_item_zpack.set_client_ip(batch_item.client_ip());
		batch_item_zpack.set_client_port(batch_item.client_port());
		batch_item_zpack.set_max_wait_time(batch_item.max_wait_time());
		//batch_item_zpack.set_consistency(batch_item.consistency());
		string result = "";

		if (batch_item_zpack.opcode() == Const::ZSC_OPC_LOOKUP) {
			batch_item.set_val(lookup_shared(batch_item_zpack));
		} else if (batch_item_zpack.opcode() == Const::ZSC_OPC_INSERT) {
			batch_item.set_val(insert_shared(batch_item_zpack));
		} else if (batch_item_zpack.opcode() == Const::ZSC_OPC_APPEND) {
			batch_item.set_val(append_shared(batch_item_zpack));
		} else if (batch_item_zpack.opcode() == Const::ZSC_OPC_CMPSWP) {
			if (batch_item_zpack.key().empty()){
				batch_item.set_val(Const::ZSC_REC_EMPTYKEY); //-1
			}else{
				result = compare_swap_internal(batch_item_zpack);
				string lkpresult = lookup_shared(batch_item_zpack);
				//batch_item.set_val(batch_item.val().append(erase_status_code(lkpresult)));
			}
		} else if (batch_item_zpack.opcode() == Const::ZSC_OPC_REMOVE) {
			batch_item.set_val(remove_shared(batch_item_zpack));
		} else if (batch_item_zpack.opcode() == Const::ZSC_OPC_STCHGCB) {
			batch_item.set_val(state_change_callback(batch_item_zpack));
		} else {
			batch_item.set_val(Const::ZSC_REC_UOPC);
		}

		addToBatch(batch_item, response_pack);
	}

	cout << "Each item in batch processed, sending back result packet" << endl;
	response_pack.set_client_ip(response_pack.batch_item(0).client_ip());
	response_pack.set_client_port(response_pack.batch_item(0).client_port());
	string msg = response_pack.SerializeAsString();
	char *buf = (char*) calloc(_msg_maxsize, sizeof(char));
	size_t msz = _msg_maxsize;

#ifdef SCCB
	_stub->sendBack(_addr, msg.c_str(), msz);
	return "";
#else
	return msg.c_str();
#endif
}

int HTWorker::addToBatch(BatchItem item, ZPack &batch) {
	BatchItem* newItem = batch.add_batch_item();
	newItem->set_key(item.key());
	newItem->set_val(item.val());
	newItem->set_client_ip(item.client_ip());
	newItem->set_client_port(item.client_port());
	newItem->set_opcode(item.opcode());
	newItem->set_max_wait_time(item.max_wait_time());
	newItem->set_consistency(item.consistency());
	return 0;
}

string HTWorker::insert_shared(const ZPack &zpack) {

	string result;

	if (zpack.key().empty())
		return Const::ZSC_REC_EMPTYKEY; //-1

	string key = zpack.key();
	int ret = PMAP->put(key, zpack.SerializeAsString());

	if (ret != 0) {

		printf("thread[%lu] DB Error: fail to insert, rcode = %d\n",
				pthread_self(), ret);
		fflush(stdout);

		result = Const::ZSC_REC_NONEXISTKEY; //-92
	} else {

		if (_instant_swap) {
			PMAP->writeFileFG();
		}

		result = Const::ZSC_REC_SUCC; //0, succeed.
	}

	return result;
}

string HTWorker::insert(const ZPack &zpack) {

	string result = insert_shared(zpack);

#ifdef SCCB
	_stub->sendBack(_addr, result.data(), result.size());
	return "";
#else
	return result;
#endif
}

string HTWorker::lookup_shared(const ZPack &zpack) {

	string result;

	if (zpack.key().empty())
		return Const::ZSC_REC_EMPTYKEY; //-1

	string key = zpack.key();
	string *ret = PMAP->get(key);

	if (ret == NULL) {

		printf("thread[%lu] DB Error: lookup found nothing\n", pthread_self());
		fflush(stdout);

		result = Const::ZSC_REC_NONEXISTKEY;
		result.append("Empty");
	} else {

		result = Const::ZSC_REC_SUCC;
		result.append(*ret);
	}

	return result;
}

string HTWorker::lookup(const ZPack &zpack) {

	string result = lookup_shared(zpack);

#ifdef SCCB
	_stub->sendBack(_addr, result.data(), result.size());
	return "";
#else
	return result;
#endif
}

string HTWorker::append_shared(const ZPack &zpack) {

	string result;

	if (zpack.key().empty())
		return Const::ZSC_REC_EMPTYKEY; //-1

	string key = zpack.key();
	int ret = PMAP->append(key, zpack.SerializeAsString());

	if (ret != 0) {

		printf("thread[%lu] DB Error: fail to append, rcode = %d\n",
				pthread_self(), ret);
		fflush(stdout);

		result = Const::ZSC_REC_NONEXISTKEY; //-92
	} else {

		if (_instant_swap) {
			PMAP->writeFileFG();
		}

		result = Const::ZSC_REC_SUCC; //0, succeed.
	}

	return result;
}

string HTWorker::append(const ZPack &zpack) {

	string result = append_shared(zpack);

#ifdef SCCB
	_stub->sendBack(_addr, result.data(), result.size());
	return "";
#else
	return result;
#endif
}

string HTWorker::state_change_callback(const ZPack &zpack) {

	WorkerThreadArg *wta = new WorkerThreadArg(zpack, _addr, _stub);
	PQUEUE->push(wta); //queue the WorkerThreadArg to be used in thread function

	if (!FIRST_ASYNC) {
		pthread_t tid;
		pthread_create(&tid, NULL, threaded_state_change_callback, NULL);
		FIRST_ASYNC = true;
	}

	return "";
}

void *HTWorker::threaded_state_change_callback(void *arg) {

	WorkerThreadArg* pwta = NULL;
	while (true) {
		while (PQUEUE->pop(pwta)) { //dequeue the WorkerThreadArg

			string result = state_change_callback_internal(pwta->_zpack);

			int mslapsed = 0;
			int lease = atoi(pwta->_zpack.lease().c_str());

			//printf("poll_interval: %d\n", poll_interval);

			while (result != Const::ZSC_REC_SUCC) {

				mslapsed += SCCB_POLL_INTERVAL;
				usleep(SCCB_POLL_INTERVAL * 1000);

				if (mslapsed >= lease)
					break;

				result = state_change_callback_internal(pwta->_zpack);
			}

			pwta->_stub->sendBack(pwta->_addr, result.data(), result.size());

			/*pwta->_htw->_stub->sendBack(pwta->_htw->_addr, result.data(),
			 result.size());*/

			delete pwta;
			pwta = NULL;
		}
	}
}

string HTWorker::state_change_callback_internal(const ZPack &zpack) {

	string result;

	if (zpack.key().empty())
		return Const::ZSC_REC_EMPTYKEY; //-1

	string key = zpack.key();
	string *ret = PMAP->get(key);

	if (ret == NULL) {

		printf("thread[%lu] DB Error: lookup found nothing\n", pthread_self());
		fflush(stdout);

		result = Const::ZSC_REC_NONEXISTKEY;
	} else {

		ZPack rltpack;
		rltpack.ParseFromString(*ret);

		if (zpack.val() == rltpack.val()) {

			result = Const::ZSC_REC_SUCC; //0, succeed.
		} else {

			result = Const::ZSC_REC_SCCBPOLLTRY;
		}
	}

	return result;
}

string HTWorker::compare_swap(const ZPack &zpack) {

	if (zpack.key().empty())
		return Const::ZSC_REC_EMPTYKEY; //-1

	string result = compare_swap_internal(zpack);

	string lkpresult = lookup_shared(zpack);

	result.append(erase_status_code(lkpresult));

#ifdef SCCB
	_stub->sendBack(_addr, result.data(), result.size());
	return "";
#else
	return result;
#endif
}

string HTWorker::compare_swap_internal(const ZPack &zpack) {

	string ret;

	/*get Package stored by lookup*/
	string lresult = lookup_shared(zpack);
	ZPack lzpack;
	lresult = erase_status_code(lresult);
	lzpack.ParseFromString(lresult);

	/*get seen_value passed in*/
	string seen_value_passed_in = zpack.val();

	/*get seen_value stored*/
	string seen_value_stored = lzpack.val();

	/*	printf("{%s}:{%s,%s}\n", zpack.key().c_str(), zpack.val().c_str(),
	 zpack.newval().c_str());*/

	/*they are equivalent, compare and swap*/
	if (!seen_value_passed_in.compare(seen_value_stored)) {

		lzpack.set_val(zpack.newval());

		return insert_shared(lzpack);

	} else {

		return Const::ZSC_REC_SRVEXP;
	}
}

string HTWorker::remove_shared(const ZPack &zpack) {

	string result;

	if (zpack.key().empty())
		return Const::ZSC_REC_EMPTYKEY; //-1

	string key = zpack.key();
	int ret = PMAP->remove(key);

	if (ret != 0) {

		printf("thread[%lu] DB Error: fail to remove, rcode = %d\n",
				pthread_self(), ret);
		fflush(stdout);

		result = Const::ZSC_REC_NONEXISTKEY; //-92
	} else {

		if (_instant_swap) {
			PMAP->writeFileFG();
		}

		result = Const::ZSC_REC_SUCC; //0, succeed.
	}

	return result;
}

string HTWorker::remove(const ZPack &zpack) {

	string result = remove_shared(zpack);

#ifdef SCCB
	_stub->sendBack(_addr, result.data(), result.size());
	return "";
#else
	return result;
#endif
}

string HTWorker::erase_status_code(string & val) {

	return val.substr(3);
}

string HTWorker::get_novoht_file() {

	return ConfHandler::NOVOHT_FILE;
}

void HTWorker::init_me() {

	if (PMAP == NULL)
		PMAP = new NoVoHT(get_novoht_file(), 100000, 10000, 0.7);
}

bool HTWorker::get_instant_swap() {

	string swap = ConfHandler::get_zhtconf_parameter(Const::INSTANT_SWAP);

	int flag = atoi(swap.c_str());

	bool result;

	if (flag == 1)
		result = true;
	else if (flag == 0)
		result = false;
	else
		result = false;

	return result;
}
