#include<iostream>
#include<algorithm>
#include<cstdlib>
#include<cstring>
#include<cstdio>
#include<stack>
#include<string>
#include<queue>
#include<fstream>
#include<set>
#include<map>
#include<cmath>
#include"info.hpp"
#include<unordered_map>
#include<time.h>
using namespace std;
typedef long long ll;
const int MAXN=200005;
const ll mod=1e9+7;
const int inf=0x3f3f3f3f;
class IndexBuilder{
public:
	long long file_byte_length; // the size of posting file
	ifstream infile; // read data from posting file
	string output_binary_file_path; // the path of index file
	ofstream ouf; // write data to index file
	int input_buffer_length; 
	int output_buffer_length;
	Tuple *buffer; // the input buffer
	unsigned char *compress; // the output buffer
	int output_buffer_id; 
	long long read_byte;
	long long offset; // record the current offset
	string final_lexicon_path; // the path of final lexicon file
	unordered_map<int,tuple<string, long long, long long, int> >ms; // the final lexicon
	//						 word ,start offset,end offset,num of docments 
	int block_size;
	
	void read_lexicon(string lexicon_path){ // read the lexicon from disk
		clock_t start,end;
		start=clock();
		ifstream lex(lexicon_path);
		string word;
		int termid;
		while (!lex.eof() ){
			lex>>word>>termid;
			if(lex.fail())break;
			ms[termid] = tuple<string, long long, long long, int>(word, 0, 0, 0);
		}
		end=clock();
		cout<<"The running time for reading lexicon is "<<(double)(end-start)/CLOCKS_PER_SEC<<" seconds"<<endl;
	}
	IndexBuilder(string source_path, string output_file, string lexicon_path, string final_lexicon="final_lexicon.txt",int input_buffer_length = 100000,int output_buffer_length = 1000000){
		this->input_buffer_length = input_buffer_length; // the input bffer length
		this ->output_buffer_length = output_buffer_length;// the output buffer length
		block_size=64; // block size
		read_byte = 0; 
		offset = 0;
		buffer = new Tuple [input_buffer_length];// input buffer
		compress = new unsigned char [output_buffer_length]; // output_buffer
		output_buffer_id = 0;
		infile.open(source_path, ios::in|ios::binary);
		infile.seekg(0,ios::end);
		file_byte_length = infile.tellg(); // the size of merged posting file
		infile.seekg(0,ios::beg);
		
		cout<<"The length(in byte) of file is "<<file_byte_length<<endl;
		output_binary_file_path = output_file;
		remove((char*)output_binary_file_path.data());
		ouf.open(output_binary_file_path, ios_base::binary|ios_base::app);
		read_lexicon(lexicon_path);
		final_lexicon_path = final_lexicon;
		cout<<"Construct Successfully!"<<endl;
	}
	vector<unsigned char> varEncode(int num){ // compress a int number by varbyte algorithm
		vector<unsigned char>tmp;
		while(num>127){
			tmp.push_back(num&127);
			num = num >>7;
		}
		tmp.push_back(128+num);
		return tmp;
	}
	vector<unsigned char> generate_block(vector<int>&tmp_doc,vector<int>&tmp_fre){ // generate a block in varbyte format
		vector<unsigned char> tmp_buffer1,tmp_buffer2;
		tmp_buffer1 = to_bytestream(tmp_doc);
		tmp_buffer2 = to_bytestream(tmp_fre);
		for(auto &v:tmp_buffer2){
			tmp_buffer1.push_back(v);
		}
		return tmp_buffer1;
	}
	vector<unsigned char> to_bytestream(vector<int>&a){ // encode a int vector to unsigned char vector by varbyte
		vector<unsigned char> tmp_buffer;
		for(int i=0;i<a.size();i++){
			int val=a[i];
			vector<unsigned char>s= varEncode(val);
			for(auto &v:s){
				tmp_buffer.push_back(v);
			}
		}
		return tmp_buffer;
	}

	// for a term, its docids and frequency are all stored in the doclist and frelist
	// this function is to encode the inverted list of the term to varbyte format
	void reformat_compress(int termid,vector<int>&doclist,vector<int>&frelist){
		if(termid==-1)return;
		int len = doclist.size(); // the number documents where this term appears
		if(len == 0)return;
		vector<unsigned char>tmp_buffer;
		vector<int>tmp_doc; // record the document id for one block
		vector<int>tmp_fre; // record the frequency for one block
		int last_docid=-1;
		vector<vector<unsigned char>>all_blocks;
		vector<int>all_last_docid; // record the last document, it will be stored in metadata
		for(int i=0;i<len;i++){
			int val=0;
			if(i%block_size==0){
				val=doclist[i]-(i-1>=0?doclist[i-1]:0);
				if(last_docid!=-1){
					vector<unsigned char> block = generate_block(tmp_doc,tmp_fre); // generate a block
					all_last_docid.push_back(last_docid); // record the last document id
					all_blocks.push_back(block); // store all blocks
					tmp_doc.clear();tmp_fre.clear(); // clear them to let them record the docid and freq for the next block
				}
			}			
			else{
				val=doclist[i]-doclist[i-1]; // use difference
			}
			int fre = frelist[i];
			last_docid = doclist[i];
			tmp_doc.push_back(val);
			tmp_fre.push_back(fre);
		}
		if(tmp_doc.size()){ // the last block may not be 64-size
			vector<unsigned char> block = generate_block(tmp_doc,tmp_fre);
			all_last_docid.push_back(last_docid);
			all_blocks.push_back(block);
			tmp_doc.clear();tmp_fre.clear();
		}
		vector<int>start; // this vector records each block's offset from the start of the first block. 
		int block_offset_len = 0;
		for(int i=0;i<all_blocks.size();i++){
			start.push_back(block_offset_len);
			int cur_size = all_blocks[i].size();
			block_offset_len += cur_size;
		}
		vector<unsigned char>lastdoc = to_bytestream(all_last_docid); // encode last docment ids to varbyte format
	   	vector<unsigned char>block_offset = to_bytestream(start); // encode the offset of each block to varbyte format
		int overall_byte = lastdoc.size()+block_offset.size()+block_offset_len; // the size (in byte) of the whole inverted list

		inverted_list_to_output_buffer(lastdoc,block_offset,all_blocks); // write the whole inverted list to output buffer

		tuple<string, long long, long long, int>& terminfo = ms[termid]; // update the lexicon
		get<1>(terminfo) = offset; //the start of this inverted list
		get<2>(terminfo) = offset + overall_byte; // the end of this inverted list
		get<3>(terminfo) = len; // the number of postings in this inverted list
		//cout<<offset<<' '<<offset + overall_byte<<endl;
		offset+=overall_byte;
		return;
	}
	void write_array_to_output_buffer(vector<unsigned char>&a){ // write an array to output buffer
		for(auto &v:a){
			compress[output_buffer_id] = v;
			output_buffer_id++;
			if(output_buffer_id==output_buffer_length){ // if the output buffer is filled, output the content into disk
				output_binary_file(output_buffer_id);
				output_buffer_id=0;
			}
		}
		return;

	}
	void inverted_list_to_output_buffer(vector<unsigned char>&lastdoc,vector<unsigned char>&block_offset,vector<vector<unsigned char>>&all_blocks){
		write_array_to_output_buffer(lastdoc); // write last docid to the output buffer
		write_array_to_output_buffer(block_offset); // write the offsets for each block to the output buffer
		for(auto &v:all_blocks){ // write each block to the output buffer
			write_array_to_output_buffer(v);
		}
		return;
	}
		
	void output_binary_file(int bytenum){ // output the content of output buffer to disk
		if(bytenum==0)return;
		ouf.write(reinterpret_cast<char*>(compress),bytenum);
		return;
	}
	void build(){
		cout<<"Start to build index!"<<endl;
		vector<int>doclist;
		vector<int>frelist;
		int last=-1;
		while(read_byte < file_byte_length){

			// read postings from disk
			int cur_read_byte = min(file_byte_length - read_byte, 1LL*input_buffer_length*(int)sizeof(Tuple));
			infile.read(reinterpret_cast<char*>(buffer), cur_read_byte);

			read_byte += cur_read_byte; // record how many bytes have been read
			int cur_length = cur_read_byte/sizeof(Tuple); // record the number of postings in input buffer
			int idx = 0;
			int termid,docid,fre;
			int num_docs = 0;
			while(idx<cur_length){
				termid = buffer[idx].termid;
				docid = buffer[idx].docid;
				fre = buffer[idx].freq;
				idx++;
				if(termid!=last){ // if a new termid appears, go to handle the last termid
					reformat_compress(last,doclist,frelist);
					num_docs = 0;
					frelist.clear();
					doclist.clear();
					last = termid;
				}
				num_docs++;
				doclist.push_back(docid);
				frelist.push_back(fre);
			}
		}
		reformat_compress(last,doclist,frelist); // handle the last term
		output_binary_file(output_buffer_id);
		return;
	}
	void output_final_lexicon(){ // output the final lexicon into disk
		ofstream out(final_lexicon_path);
		for(auto &x:ms){
			auto &v = x.second;
			out<<(get<0>(v))<<"\t"<<(get<1>(v))<<"\t"<<(get<2>(v))<<"\t"<<(get<3>(v))<<endl;
		}
		out.close();
		return;	
	}

	~IndexBuilder(){
		ouf.close();
		infile.close();
		delete [] buffer;
	}
	
};
int main(int argc, char* argv[]){

	clock_t start,end;
	start=clock();

	// the default configuration
	string mergedresult = "mergedresult.txt";
	string index_file_path = "final_index";
	string lexicon_path = "lexicon.txt";
	int input_buffer_length=100000;// the input bffer length
	int output_buffer_length = 1000000;// the output buffer length
	if(argc >= 2) mergedresult = argv[1];
	if(argc >= 3) index_file_path = argv[2];
	if(argc >= 4) lexicon_path = argv[3]; 
	if(argc >= 5) input_buffer_length  = atoi(argv[4]);
	if(argc >= 6) output_buffer_length = atoi(argv[5]);
	freopen((char*)mergedresult.data(),"r",stdin);
	string source;cin>>source; // the merged sorted posting file
	fclose(stdin);
	cout<<source<<endl;
	IndexBuilder a(source,index_file_path,lexicon_path,"final_lexicon.txt",input_buffer_length,output_buffer_length);
	a.build();
	a.output_final_lexicon();
	end=clock();
	cout<<"The overall running time for building index is "<<(double)(end-start)/CLOCKS_PER_SEC/60<<" minutes"<<endl;
	return 0;
}
