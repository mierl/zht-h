/*
 * mutex_test.cpp
 *
 *  Created on: Dec 7, 2014
 *      Author: tony
 */

#include<pthread.h>
#include<iostream>
#include<stdlib.h>
#include <stdint.h>
#include <string>
#include<unistd.h>

#include "Util.h"
using namespace std;
using namespace iit::datasys::zht::dm;

int main(int argc, char* argv[]) {

	int num_mutex = atoi(argv[1]);

	pthread_mutex_t mutex_1[num_mutex];
	pthread_mutex_t mutex_2;
	double start1 = TimeUtil::getTime_msec();

	for (int i = 0; i < num_mutex; i++) {
		pthread_mutex_init(&(mutex_1[i]), NULL);
		//pthread_mutex_init(&(mutex_1), NULL);
		//pthread_mutex_init(&(mutex_2), NULL);
	}

	double end1 = TimeUtil::getTime_msec();

	cout << "Initialize " << num_mutex << " mutexes cost " << end1 - start1
			<< " ms" << endl;

	//pthread_mutex_init(&(mutex_1), NULL);

	int num_ops = atoi(argv[2]);

	double start = TimeUtil::getTime_msec();
	for (int i = 0; i < num_ops; i++) {

		for (int j = 0; j < num_mutex; j++) {

			pthread_mutex_lock(&mutex_1[j]);//(&mutex_1[num_mutex]);
			//usleep(1);
			pthread_mutex_unlock(&mutex_1[j]);//(&mutex_1[num_mutex]);
			//pthread_mutex_lock(&mutex_2);//(&mutex_1[num_mutex]);
			//pthread_mutex_unlock(&mutex_2);//(&mutex_1[num_mutex]);


		}
	}
	double end = TimeUtil::getTime_msec();

	cout << "Execute " << num_ops << " lock/unlock on "<<num_mutex <<" mutexes cost " << end - start
			<< " ms" << endl;
	return 0;
}
