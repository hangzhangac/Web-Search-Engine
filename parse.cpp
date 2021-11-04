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
#include<set>
#include<unordered_map>
#include<fstream>
#include"info.hpp"
#include<time.h>
#include<sys/stat.h>
using namespace std;
typedef long long ll;
const int MAXN=100000;
const int inf=0x3f3f3f3f;

bool cmp(Tuple a,Tuple b){ // used to sort the postings
	if(a.termid ==b.termid)return a.docid<b.docid;
	return a.termid<b.termid;
}
void create_dir(string s){ // create a directory whose path is s
	int rc = mkdir((char*)s.data(), 0777);
	if(rc == 0) std::cout << "Created directory "<<s<<" successfully\n";
	else std::cout <<s<< " directory already exists\n";;
}
set<string> load_stopping_words(string stop_words_filepath){
	string line;
	set<string>stopwords;
	ifstream infile(stop_words_filepath);
	while(getline(infile,line)){
		stopwords.insert(line);
	}
	return stopwords;
}
class Parser{
public:
	unordered_map<string,int>lexicon; // map from term to termid
	vector<string>doctable; // map from docid to url, here, docid is the index of vector
	vector<int>doc_num_words;// map from docid to the number of terms in that docment.
	vector<long long> doc_text_offset; //map from docid to the start offset of text of the document in trec file
	ifstream infile;// the ifstream object which will read data from .trec file
	int page_id;// record how many docments have been parsed, so we can give the current docment a docid
	int term_num;// record how many terms have showed up
	Tuple *buffer;// the buffer which caches the postings(termid, docid, docment)
	int buffer_id;// record the number of postings in the buffer
	int buffer_length; //define the size of buffer
	string delimiters;// the delimiters which we will use to split the text in each docment
	string output_dir_path;// the path of directory that will store the intermediate posting files
	string sorted_file_path_stored_in; // the path of txt file which stores the paths of intermediate posting files
	int sorted_file_num;// record the number of intermediate posting files we have generated
	long long num_postings;// Record the number of postings we have generated
	set<string>stopwords;

	Parser(string data_path,string output_dir_path, string sorted_file_path_stored_in,int buffer_length, string stopwords_path=""){
		infile.open(data_path);// open the data file
		page_id = 0;
		term_num = 0;
		buffer = new Tuple [buffer_length]; //allocate memory for buffer
		this->buffer_length = buffer_length;
		buffer_id = 0;
		delimiters = "\t ,.?#$%():;^*/!-\'\"=><·+~"; // the delimiters used to split words
		this->output_dir_path = output_dir_path;
		this->sorted_file_path_stored_in = sorted_file_path_stored_in;
		create_dir(output_dir_path); // create the output dir
		remove((const char*)sorted_file_path_stored_in.data());
		sorted_file_num=0; // record how many intermediate postings file we have generated, we can also use this number to name the sorted posting file.
		num_postings = 0;
		if(stopwords_path!=""){
			stopwords = load_stopping_words(stopwords_path);
		}
	}
	// split the sentence, store the words and their frequency in a hash map
	void extract(string& content,unordered_map<int,int>&ms){ // string content is all text from a document
		char* pch = strtok ((char*)content.data(),(const char*)delimiters.data());
		while (pch != NULL){
			string word(pch, pch + strlen(pch));
			if(word!=""&&stopwords.find(word)==stopwords.end()){
				int termid = lexicon[word]; // get the termid from lexicon
				if(termid == 0) { // the word did not show up before
					lexicon[word]=++term_num;
					termid = term_num;
				}
				ms[termid]++; // record the frequency of the word
			}
			pch = strtok (NULL, (const char*)delimiters.data());
		}
		return;
	}
	void write_out(int length){ // when the buffer is filled, sort the postings and write it out into disk
		if(length==0)return;
		sort(buffer,buffer+length,cmp); // sort the postings in buffer by termid, if termid is equal then by document id
		sorted_file_num++;
		cout<<"Has output "<<sorted_file_num<<" sorted files"<<endl;
		string sorted_file_path = output_dir_path+"/g_"+to_string(sorted_file_num); // the path of output file
		ofstream ouf;
		ouf.open((const char*)sorted_file_path_stored_in.data(),ios::app); // record the path of the output file
		ouf<<sorted_file_path<<endl;
		ouf.close();
		ouf.open(sorted_file_path,std::ofstream::binary);
		ouf.write(reinterpret_cast<const char*>(buffer), sizeof(Tuple)*length); // output the buffer to output file
		ouf.close();
		return;
	}
	// for each document, all words and their frequency are stored in the unordered_map
	// this function is to generate postings from the unordered_map into buffer
	int generate_postings(unordered_map<int,int>&ms){
		int docid = page_id;
		int num_words=0;
		num_postings += ms.size();
		for(auto &v:ms){
			Tuple e(v.first,docid,v.second);
			num_words+=v.second;
			buffer[buffer_id++] = e;
			if(buffer_id == buffer_length){ // if buffer is filled, output the postings into disk
				write_out(buffer_id);
				buffer_id = 0;
			}
		}
		return num_words; //how many words in this document

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
				doc_text_offset.push_back(infile.tellg());
				flag=2;//the next line will be text content
			}
			else if(line == "</TEXT>"){
				unordered_map<int,int>ms;
				extract(content,ms);
				int num_words = generate_postings(ms);
				doc_num_words.push_back(num_words);
				ms.clear();
				flag=0; // the text content ends
				content="";
			}
			else if(flag==2){
				content+=line+' ';
			}
			else if(line == "<DOC>"){
				page_id++;
				if(page_id%MAXN==0)cout<<"Has parsed "<<page_id<<" documents"<<endl;
			}
			else if(line =="<TEXT>"){
				flag=1;// it indicates that the next line will be the url
			}
		}
		write_out(buffer_id);
		buffer_id=0;
	}
	void output(string lexicon_path, string doctable_path){ // output the lexicon and docment table

		ofstream ouf;
		ouf.open(lexicon_path);
		for(auto &v:lexicon){
			ouf<<v.first<<"\t"<<v.second<<endl;
		}
		ouf.close();
		cout<<"-------------------------------"<<endl;
		ouf.open(doctable_path);
		for(int i=0;i<doctable.size();i++){
			ouf<<i+1<<"\t"<<doctable[i]<<"\t"<<doc_num_words[i]<<"\t"<<doc_text_offset[i]<<endl;
		}
		ouf.close();
		return;
	}
	~Parser(){
		infile.close();
		delete [] buffer;
	}	
};


int main(int argc, char *argv[]){
	clock_t start,end;
	start=clock();
	string data_path = "../assign2_data/msmarco-docs.trec";
	string output_dir_path = "./data";
	string sorted_file_path_stored_in = "sortedlist.txt";
	int buffer_length = 100*MAXN;
	if(argc>=2) data_path = argv[1];
	// if(argc>=3) output_dir_path = argv[2];
	if(argc>=3) sorted_file_path_stored_in = argv[2];
	if(argc>=4) buffer_length = atoi(argv[3]);
	Parser p(data_path,output_dir_path,sorted_file_path_stored_in,buffer_length,"stopping_words.txt");
	p.parse();
	p.output("lexicon.txt","doctable.txt");
	end=clock();
	cout<<"The overall running time for parsing is "<<(double)(end-start)/CLOCKS_PER_SEC/60<<" minutes"<<endl;
}
