/*************************************************************************
	> File Name: merge.cpp
    > Full Path: /Users/zhanghang/workspace/WSE/assign2/mergesort_binary/merge.cpp
	> Author: Hang Zhang
	> Created Time:  10/2 22:19:02 2021
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
#include"info.hpp"
#include<time.h>
#include<sys/stat.h>
using namespace std;
typedef long long ll;
const int MAXN=100000;
const ll mod=1e9+7;
const int inf=0x3f3f3f3f;
int buffer_length=100000; // input buffer length
//int output_buffer_length = 1000*1000; 
int output_buffer_length = 1000*10000; // output buffer length
void create_dir(string s){ // create a directory whose path is s
	int rc = mkdir((char*)s.data(), 0777);
	if(rc == 0) std::cout << "Created "<<s<<" success\n";
	else std::cout <<s<< " directory already exists\n";;
}

// input buffer class
class Load{
	public:
		string path; // the path of sorted posting file
		ifstream infile; // used to read data from the sorted posting file
		Tuple* buffer; // the buffer
		int cur_data_len; // denote how many postings are stored in buffer  
		int bid; // the index of the posting in input buffer 
		int read_byte_eachtime; // denote how many bytes we will read from the sorted posting file
		long long file_byte_length; //record the size (in byte) of sorted posting file

	Load(string file_path){
		path = file_path;
		infile.open(path, ios_base::binary|ios_base::app); // open the sorted posting file in binary format
		infile.seekg(0, ios::end);
		file_byte_length = infile.tellg(); //calculate the length of the file
		infile.seekg(0, ios::beg);
		buffer = new Tuple [buffer_length]; // create memory for buffer
		bid=0;
		read_byte_eachtime = sizeof(int)*3*buffer_length; // how many bytes I will read from sorted posting file
		read_data();
	}
	~Load(){
		delete [] buffer;
		cout<<path<<" has completed!"<<endl;
	}

	bool read_data(){ // read data from file to buffer
		cur_data_len = min((long long)read_byte_eachtime, file_byte_length - infile.tellg());
		cur_data_len = cur_data_len/(sizeof(int)*3);
		if(cur_data_len==0)return false;
		infile.read(reinterpret_cast<char*>(buffer), cur_data_len*sizeof(int)*3);
		return true;
	}

	Tuple get_data(){ // get one posting from the buffer
		if(bid == cur_data_len){ // all postings have been taken away, need to read new postings from disk
			if(read_data()) bid=0;
			else return Tuple(-1,-1,-1); // has read all postings from disk
		}
		Tuple cur = buffer[bid];	
		bid++;
		return cur;
	}
};

// merge all intermedia posting files into one target file, and return the target file's path
string solve(vector<string>&paths, int round, int num, string dir){
	vector<Load*>load; 
	Tuple *output_buffer=new Tuple [output_buffer_length];
	int id = 0; // the index for output_buffer_length		

	priority_queue< pair<Tuple,Load*> >q; // the priority queue used to merge k sorted list, the top() of pq is the postings with least termid
	for(auto path: paths){ // create a input buffer for each intermedia posting files
		load.push_back(new Load(path));
	}
	for(auto v: load){
		Tuple tmp = v->get_data();
		if(!tmp.valid())continue;
		q.push({tmp,v});
	}
	string output_file_name = dir+"/sorted_" + to_string(round) + "_" + to_string(num);
	remove((char*)output_file_name.data());
	ofstream outfile(output_file_name,ios_base::binary|ios_base::app);
	while(!q.empty()){ 
		pair<Tuple, Load*> e = q.top();
		q.pop();
		output_buffer[id]=e.first;
		id++;
		if(id == output_buffer_length){ // if the output buffer is filled, output postings into disk
			outfile.write(reinterpret_cast<const char*>(output_buffer),sizeof(Tuple)*id);
			id = 0;
		}
		e.first = e.second->get_data(); // read a new data from the input buffer
		if(!e.first.valid()){
			delete e.second;
			continue;
		}
		q.push(e);
	}
	if(id!=0){ //clear the output buffer
		outfile.write(reinterpret_cast<const char*>(output_buffer),sizeof(Tuple)*id);
	}
	return output_file_name; // return the path of file which stored the merged postings
}

int main(int argc, char *argv[]){
	clock_t start,end;
	start = clock();
	ifstream infile;
	vector<string>paths;
	string sortedlist = "sortedlist.txt";
	if(argc>=2) sortedlist = argv[1]; 
	if(argc>=4) buffer_length = atoi(argv[3]);
	if(argc>=5) output_buffer_length = atoi(argv[4]);
	string dir = "merge_intermediate_result";
	create_dir(dir);
	freopen((char*)sortedlist.data(),"r",stdin); 
	string s;
	while(cin>>s){
		paths.push_back(s);
	}
	fclose(stdin);
	int round = 1;// record how many times we traverse the whole data
	cout<<"Start to merge!"<<endl;
	while(paths.size()>1){// if the number of merged files is not one, merge them
		int num=1; // record how many files we generate after merging
		int i=0;
		vector<string>tmp;
		vector<string>new_path;
		for(int i=0;i<paths.size();i++){
			tmp.push_back(paths[i]);
			if(tmp.size()==16){ // merge 16 files into one file once
				new_path.push_back(solve(tmp,round,num,dir));
				num++;
				tmp.clear();
			}
		}
		if(tmp.size()){
			new_path.push_back(solve(tmp,round,num++,dir));
		}
		paths=new_path;
		round++;
	}
	cout<<"The merged result is in "<<paths[0]<<endl;
	end=clock();
	cout<<"The overall running time for merging the sorted list is "<<(double)(end-start)/CLOCKS_PER_SEC/60<<" minutes"<<endl;
	string mergedresult = "mergedresult.txt";
	if(argc>=3) mergedresult = argv[2];
	freopen((char*)mergedresult.data(),"w",stdout);
	cout<<paths[0]<<endl;
	fclose(stdout);
	return 0;
}
