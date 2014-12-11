#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <string.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <iostream>
using namespace std;
int main(int argc, char * argv[]){

	//struct sockaddr_in svrAdd_in;
	int port = 55000;
	struct sockaddr_in svrAdd_in;
	int svrSock = -1;

	memset(&svrAdd_in,0,sizeof(struct sockaddr_in));
	svrAdd_in.sin_family = AF_INET;
	svrAdd_in.sin_addr.s_addr = INADDR_ANY;
	svrAdd_in.sin_port = htons(port);
	struct hostent * hinfo = gethostbyname("localhost");
	printf("IP Address :%s\n",inet_ntoa(*((struct in_addr *)hinfo->h_addr)));
	memcpy(&svrAdd_in.sin_addr, hinfo->h_addr,sizeof(svrAdd_in.sin_addr));
    svrSock = socket(AF_INET,SOCK_STREAM,0);

	printf("%d\n",svrSock);

	if(bind(svrSock,(struct sockaddr*) & svrAdd_in,sizeof(struct sockaddr)) <0 )
	{
		printf("error\n");
	}


	if(listen(svrSock,5) <0)
	{
		printf("error\n");
	}

	sockaddr *in_addr = (sockaddr *) calloc(1,sizeof(struct sockaddr));
	socklen_t in_len = sizeof(struct sockaddr);
	int infd = accept(svrSock, in_addr,&in_len);
	printf("%d\n",infd);

	char * buf = (char * ) malloc(1024);
	memset(buf, '\0', 1024);
	printf("buf-----------%s", buf);
	int rec_size = recv(infd,buf,1024,0);
	cout << "received "<<rec_size << " bytes."<<endl;
	//cout << "strlen(buf): "<<strlen(*buf)<<endl;
	cout << "buf: "<< buf <<endl;
	string str(buf);
	//string str = *buf;
	//printf("%s\n",buf);
	cout << "str(buf) = " << str << endl;

	return 0;
}
