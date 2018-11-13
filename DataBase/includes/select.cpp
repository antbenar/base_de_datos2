#include <iostream>
#include <vector>
#include <fstream>
#include <boost/serialization/vector.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/algorithm/string.hpp>

#include <sstream>
#include <string>
#include <iomanip>
#include <algorithm>

#include "select.h" 

using namespace std;

class SELECT{

private:
	vector<string> print_header_table( ifstream & ifs ){
		vector<string> header;
		string data;
	
		getline(ifs, data);
		stringstream ssdata;
		ssdata << data;
	
		cout << endl;
		while ( ssdata >> data ) {
			cout << setw(15)<< data;
			header.push_back( data );	
			ssdata >> data;
		}
		cout << endl << endl;
	
		return header;		
	}


	void print_data_table( ifstream & ifs ){
		string data;
		while (getline(ifs, data)) {
			stringstream ssdata;
			ssdata << data;
		
			while ( ssdata >> data ) 
				cout << setw(15)<< data;	
			
			cout << endl ;
		}
		cout << endl;
	}


	void print_one_row( ifstream & ifs,  vector<string> header, pair<string, string> where_condition ){
		int i;
		for( i = 0; i<header.size(); ++i)
			if( header[i] == where_condition.first ) break;
	
		string data, data_temp;
	
		while (getline(ifs, data)) {
			data_temp = data;
			stringstream ssdata;
			ssdata << data;

			for( int j = 0; j <= i; ++j)	
				ssdata >> data;
			
			if( data == where_condition.second ){
				stringstream ssdata;
				ssdata << data_temp;
				while ( ssdata >> data ) 
					cout << setw(15)<< data;
				cout<< endl << endl;
				return;	
			}	
		}
	}
	
	
	vector< pair<string, string> > Parse_where ( string where_line ){
		vector< pair<string, string> > where_condition;
		stringstream ss;
		string condition,col,value;
	
		boost::erase_all(where_line, ",");
		ss << where_line;

		while( ss >> condition ){	
			if( condition == "=" ){
				ss >> value;
				where_condition.push_back( make_pair( col , value ) );
				continue;
			}
			col = condition;
		}
		
		return where_condition;
	}


	void select_operation( vector <string> cols_select, string from_table, vector< pair<string, string> > where_condition ){
		std::ifstream ifs ( path + "tables/" + from_table + ".dbf", std::ifstream::in);
		string data;
	
		if ( cols_select[0] == "*" ){
			vector<string> header = print_header_table(ifs);
		
			if( !where_condition.empty() ) print_one_row( ifs, header, where_condition[0] );
			else print_data_table(ifs);
		}

		ifs.close();
	}

	void Parse_Select( string rest_of_line ){
		stringstream ss;
		ss << rest_of_line;
	
		vector <string> cols_to_select;
		string name_col, from_table, where;
	
		//extraer columnas a leer del select -------- SELECT
		while( ss >> name_col ){	
			if( name_col == "FROM" )
				break;
			boost::erase_all(name_col, ",");
			cols_to_select.push_back(name_col);
		}
	
		//identificar tabla -------- FROM
		ss >> from_table;			
	
		//analizar si hay restricciones ------- WHERE
		vector< pair<string, string> > where_condition;
		ss >> where;				
		if( where == "WHERE"){
			getline(ss, rest_of_line);
			where_condition = Parse_where(rest_of_line);
		}
		
		select_operation( cols_to_select, from_table, where_condition);
	}
	
	
public:
	
	SELECT ( string rest_of_line ){
		Parse_Select(rest_of_line);
	}

};
