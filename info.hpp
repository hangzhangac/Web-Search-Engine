#include<iostream>
using namespace std;

class Tuple{
	public:
		int termid;
		int docid;
		int freq;
		Tuple(){
			termid=docid=freq=0;
		}
		Tuple(int t,int d,int f):termid(t),docid(d),freq(f){

		}
		friend bool operator < (Tuple tuple1, Tuple tuple2){
			if(tuple1.termid == tuple2.termid)return tuple1.docid>tuple2.docid;
			return tuple1.termid > tuple2.termid;
		}
		bool valid(){
			return termid!=-1;
		}
		friend ostream &operator<<( ostream &output, const Tuple &D ){
			output << "termid: " << D.termid << " docid: " << D.docid << " freq: " << D.freq;
			return output;
		}
};



