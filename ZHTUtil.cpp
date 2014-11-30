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
 * ZHTUtil.cpp
 *
 *  Created on: Jun 25, 2013
 *      Author: Tony
 *      Contributor: Xiaobingo, KWang, DZhao
 */

#include "ZHTUtil.h"

#include "Util.h"
#include "ConfHandler.h"

#include <arpa/inet.h>
#include <algorithm>
#include <netdb.h>

#include  "zpack.pb.h"
#include <string>
#include <fstream> // for ifstream
#include <iostream> // for cout and endl
#include <unistd.h> // for unlink()

using namespace iit::datasys::zht::dm;

ZHTUtil::ZHTUtil() {
}

ZHTUtil::~ZHTUtil() {
}


string ZHTUtil::getLocalIP(void){
	// attempt to obtain ifconfig information
	system( "/sbin/ifconfig eth0 2> /dev/null"
	"| /bin/grep -m 1 addr: | /usr/bin/cut -d : -f2"
	"| /usr/bin/cut -d ' ' -f1 > /tmp/sysinfo;" );

	system( "/sbin/ifconfig eth0 2> /dev/null"
	"| /bin/grep -m 1 Bcast: | /usr/bin/cut -d : -f3"
	"| /usr/bin/cut -d ' ' -f1 >> /tmp/sysinfo;" );

	system( "/sbin/ifconfig eth0 2> /dev/null"
	"| /bin/grep -m 1 Mask: | /usr/bin/cut -d : -f4 >> /tmp/sysinfo;" );

	// read ifconfig information from flat-file
	const std::string TBD( "unknown" );
	std::string ipAddr( TBD );
	std::string broadcast( TBD );
	std::string netmask( TBD );

	std::ifstream sysinfo( "/tmp/sysinfo" );

	if ( sysinfo )
	{
	if ( sysinfo.peek() != '\0' ) sysinfo >> ipAddr;
	if ( sysinfo.peek() != '\0' ) sysinfo >> broadcast;
	if ( sysinfo.peek() != '\0' ) sysinfo >> netmask;

	sysinfo.close();

	unlink( "/tmp/sysinfo" );
	}

	//std::cout << "IP = " << ipAddr << std::endl;
	//std::cout << "BCAST = " << broadcast << std::endl;
	//std::cout << "MASK = " << netmask << std::endl;

	return ipAddr;

}


HostEntity ZHTUtil::getHostEntityByKey(const string& msg) {

	ZPack zpack;
	zpack.ParseFromString(msg); //to debug

	uint64_t hascode = HashUtil::genHash(zpack.key());
	size_t node_size = ConfHandler::NeighborVector.size();
	int index = hascode % node_size;

	ConfEntry ce = ConfHandler::NeighborVector.at(index);

	return buildHostEntity(ce.name(), atoi(ce.value().c_str()));

}

HostEntity ZHTUtil::buildHostEntity(const string& host, const uint& port) {

	HostEntity he;

	/*
	 struct sockaddr_in si_other;
	 hostent *record;
	 in_addr *address;
	 string ip_address;

	 record = gethostbyname(host.c_str());
	 address = (in_addr *) record->h_addr;
	 ip_address = inet_ntoa(*address);

	 memset((char *) &si_other, 0, sizeof(si_other));
	 si_other.sin_family = AF_INET;
	 si_other.sin_port = htons(port);
	 if (inet_aton(ip_address.c_str(), &si_other.sin_addr) == 0) {
	 fprintf(stderr, "inet_aton() failed\n");
	 }

	 he.si = si_other;
	 he.host = host;
	 he.port = port;
	 he.valid = true;
	 he.sock = -1;*/

	he.host = host;
	he.port = port;
	he.sock = -1;

	return he;
}




const uint IdHelper::ID_LEN = 20;

IdHelper::IdHelper() {
}

IdHelper::~IdHelper() {
}

uint64_t IdHelper::genId() {

	return HashUtil::genHash(HashUtil::randomString(62).c_str());
}
