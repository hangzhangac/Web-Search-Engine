/*************************************************************************
	> File Name: test_index.cpp
    > Full Path: /Users/zhanghang/workspace/WSE/assign2_2/test_index.cpp
	> Author: Hang Zhang
	> Created Time:  10/6 22:07:40 2021
 ************************************************************************/

#include<iostream>
#include<algorithm>
#include<cstdlib>
#include<cstring>
#include<cstdio>
#include<stack>
#include<string>
#include<queue>
#include<set>
#include<map>
#include<cmath>
#include<fstream>
using namespace std;
typedef long long ll;
const int MAXN=200005;
const ll mod=1e9+7;
const int inf=0x3f3f3f3f;
unsigned char buffer[MAXN*100];
int VarDecode(int &i, ifstream& infile){
	int val=0,shift=0;
	int b;
	while(1){
		//infile.read(reinterpret_cast<char*>(&b),sizeof(unsigned char));
		b=buffer[i];
		if(b>=128)break;
		val+=b<<shift;
		shift+=7;
		i++;
	}
	val+=((b-128)<<shift);
	i++;
	return val;
}
void solve(string target,int out_len){

	ifstream infile("final_lexicon.txt");
	long long offset;
	int len;
	string term;
	int offset_len;
	while(1){
		infile>>term>>offset>>offset_len>>len;
		if(term ==target){
			break;
		}
	}
	infile.close();
	infile.open("final_index",ios::binary);
	infile.seekg(offset);
	infile.read((char*)buffer,offset_len);
	infile.close();
	int cur=0;
	int i=0,cnt=0;
	int block_num = (len-1)/64+1;
	int last_block_num = len%64;
	if(last_block_num==0)last_block_num=64;
	ofstream ouf("posting_index_for_a_word.txt");
	vector<int>docid;
	vector<int>freq;
	vector<int>lastdocid,block_size;
	while(i<offset_len&&cnt<block_num){
		int tmp = VarDecode(i,infile);
		cnt++;
		lastdocid.push_back(tmp);
	}
	while(i<offset_len&&cnt<block_num*2){
		int tmp = VarDecode(i,infile);
		cnt++;
		block_size.push_back(tmp);
	}

	cnt = 0;
	int cur_block_num=0;
	int start=i;
	int idx=0;
	int prefix_size=0;
	while(cur_block_num<block_num){
		int tmp_offset = i-start;
		//if(tmp_offset != offset_block[idx]){
			//cout<<"No, offset!"<<endl;
		//}
		if(tmp_offset!=prefix_size){
			cout<<"No, offset!"<<endl;
		}
		int last_doc_id = cur_block_num-1>=0? lastdocid[cur_block_num-1]:0;
		int sum=last_doc_id;
		cur_block_num++;
		int num=64;
		if(cur_block_num==block_num)num=last_block_num;
		for(int j=0;j<num;j++){
			int cur= VarDecode(i,infile);
			docid.push_back(cur+sum);
			sum+=cur;
		}
		if(docid.back()!=lastdocid[idx]){
			cout<<"No, lastdocid!"<<endl;
		}
		for(int j=0;j<num;j++){
			int cur= VarDecode(i,infile);
			freq.push_back(cur);
		}
		prefix_size+=block_size[idx];
		idx++;
	}
	for(int j=0;j<min((int)docid.size(),out_len);j++){
		ouf<<docid[j]<<' '<<freq[j]<<endl;
	}

}

int main(int argc, char* argv[]){
	int num= 944;
	string target="the";
	if(argc>=2)target = argv[1];
	if(argc>=3)num = stoi(argv[2]);
	solve(target,num);


}







