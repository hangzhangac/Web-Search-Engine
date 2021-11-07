/*************************************************************************
	> File Name: parse.cpp
    > Full Path: /Users/zhanghang/workspace/WSE/assign2/parse.cpp
	> Author: Hang Zhang
	> Created Time: 一 10/ 4 13:52:17 2021
 ************************************************************************/

#include<iostream>
#include<algorithm>
#include<cstdlib>
#include<cstring>
#include<cstdio>
#include<string>
#include<vector>
#include<map>
#include<unordered_map>
#include<fstream>
#include"info.hpp"
#include<time.h>
#include<sys/stat.h>
using namespace std;
typedef long long ll;
const int MAXN=100000;
const ll mod=1e9+7;
const int inf=0x3f3f3f3f;
int buffer_length = 100*MAXN;
//int buffer_length = 100;
//./data/g
int number_page  = 10000;

bool cmp(Tuple a,Tuple b){
	if(a.termid ==b.termid)return a.docid<b.docid;
	return a.termid<b.termid;
}
bool check_valid(string &s){
	if(s==""||s.size()>=40)return false;
	int digit_count=0;
	for(int i=0;i<s.size();i++){
		digit_count+=(s[i]>='0'&&s[i]<='9');
	}
	return digit_count<=20;
}
string tolower(string s){
	for(int i=0;i<s.size();i++){
		if(s[i]<='Z'&&s[i]>='A')s[i]+=32;
	}
	return s;
}

class Parser{
public:
	unordered_map<string,int>lexicon; // map from term to termid
	vector<string>doctable; // map from docid to url, here, docid is the index of vector
	vector<int>doc_num_words;// map from docid to the number of terms in that docment.
	string data_path;// the path of .trec file
	ifstream infile;// the ifstream object which will read data from .trec file
	int page_id;// record how many docments have been parsed, so we can give the current docment a docid
	int term_num;// record how many terms have showed up
	Tuple *buffer;// the buffer which caches the postings(termid, docid, docment)
	int buffer_id;// record the number of postings in the buffer
	string delimiters;// the delimiters which we will use to split the text in each docment
	int sorted_file_num;// record the number of intermediate posting files we have generated
	long long num_postings;// Record the number of postings we have generated
	vector<Tuple>target_postings;
	int target_id;
	string target;

	Parser(string path,string target1){
		data_path = path;
		infile.open(data_path);
		page_id = 0;
		term_num=0;
		buffer = new Tuple [buffer_length];
		buffer_id = 0;
		//delimiters = "\n ";
		delimiters = "\n\t ,.?#$%():;^*/!-\'\"=><·+~";
		sorted_file_num=0;
		num_postings = 0;
		target_id= 0;
		target = target1;
	}
	void extract(string& content,unordered_map<int,int>&ms){
		char* pch = strtok ((char*)content.data(),(const char*)delimiters.data());
		while (pch != NULL){
			string word(pch, pch + strlen(pch));
			if(check_valid(word)){
				word = tolower(word);
				int termid = lexicon[word];
				if(termid == 0) {
					lexicon[word]=++term_num;
					termid = term_num;
					if(word==target)target_id = termid;
				}
				ms[termid]++;
			}
			pch = strtok (NULL, (const char*)delimiters.data());
		}
		return;
	}
	int generate_postings(unordered_map<int,int>&ms){
		int docid = page_id;
		int num_words=0;
		num_postings += ms.size();
		for(auto &v:ms){
			Tuple e(v.first,docid,v.second);
			num_words+=v.second;
			buffer[buffer_id++] = e;
			if(e.termid == target_id){
				//cout<<e<<endl;
				target_postings.push_back(e);
			}
			if(buffer_id == buffer_length){
				buffer_id = 0;
			}
		}
		return num_words;

	}
	void parse(){
		cout<<"Start to parse!"<<endl;
		string line;
		int flag=0;
		string content="";
		while(getline(infile, line)){
			if(line == "")continue;
			if(flag==1){
				doctable.push_back(line);
				//doctable[page_id] = line;
				flag=2;//the next line will be text content
			}
			else if(line == "</TEXT>"){
				unordered_map<int,int>ms;
				extract(content,ms);
				int num_words = generate_postings(ms);
				doc_num_words.push_back(num_words);
				flag=0; // the text content ends
				content="";
			}
			else if(flag==2){
				content += line+' ';
			}
			else if(line == "<DOC>"){
				page_id++;
				if(page_id%MAXN==0)cout<<"Has parsed "<<page_id<<" documents"<<endl;
				if(page_id==number_page+1){
					buffer_id=0;
					break;
				}
			}
			else if(line =="<TEXT>"){
				flag=1;// it indicates that the next line will be the url
			}
			//cout<<line<<endl;
		}
		buffer_id=0;
	}
	~Parser(){
		infile.close();
		cout<<lexicon[target]<<endl;
		ofstream ouf("posting_for_a_word.txt");
		for(int i=0;i<target_postings.size();i++){
			Tuple e = target_postings[i];
			if(i==0)ouf<<e.docid<<' '<<e.freq<<endl;
			else ouf<<e.docid<<' '<<e.freq<<endl;
		}
		ouf.close();
		cout<<target<<" "<<target_postings.size()<<endl;
		delete [] buffer;
	}	
};


int main(int argc,char* argv[]){
	clock_t start,end;
	start=clock();
	string target="word";
	string data_path = "../assign2_data/msmarco-docs.trec";
	if(argc>=2)target = argv[1];
	if(argc>=3) number_page = atoi(argv[2]);
	if(argc>=4) data_path = argv[3];
	cout<<"Seeking for the postings for the word \""<<target<<"\""<<endl;
	Parser p(data_path,target);
	p.parse();
	end=clock();
	cout<<"The overall running time for parsing is "<<(double)(end-start)/CLOCKS_PER_SEC/60<<" minutes"<<endl;
}





