#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <string.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <iostream>
#include <string>
using namespace std;

int main(int argc, char * argv[]){

	 struct sockaddr_in dest;
	 memset(&dest, 0 , sizeof(struct sockaddr_in));

	 dest.sin_family = AF_INET;
	 dest.sin_port = htons(55000);

	 struct hostent * hinfo = gethostbyname("localhost");
	 //char *ip = inet_ntoa (*(struct sockaddr_in *)*hostinfo->h_addr_list);
	 printf("IP Address :%s\n",inet_ntoa(*((struct in_addr *)hinfo->h_addr)));
	 memcpy(&dest.sin_addr, hinfo->h_addr,sizeof(dest.sin_addr));
	 int to_sock = socket(PF_INET, SOCK_STREAM,0);
	 if(to_sock <0){
	 	printf("error\n");
	 	return -1;
	 }

	 int ret_con = connect(to_sock, (struct sockaddr *) &dest, sizeof(sockaddr));
	 printf("%d\n",ret_con);

	 char buf[1024]="123";
	string str = "hello world";
	int sent = send(to_sock, &str,str.length(),0);
	cout << "sending len = "<< sent <<endl;
	// printf("%lu\n",send(to_sock,&str,str.length(),0));

	 return 0;
}
