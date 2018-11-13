#include <iostream>
#include <vector>
#include <map>
#include <fstream>
#include <boost/serialization/vector.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>

#include <sstream>
#include <string>
#include <iomanip>
#include <algorithm>
#include <stdio.h>
#include <stdlib.h>     
#include <time.h>  

//#include "includes/select.h"
//#include "includes/update.cpp"
using namespace std;


string path = "DataBases/";


class db{
private:
	friend class boost::serialization::access; 	

    template<class Archive>
    void serialize(Archive & ar, const unsigned int version)
    {
    	//ar & name;   
    }
    
    void save_db(const char * filename){// make an archive		
		std::ofstream ofs( filename, ios_base::binary );
		boost::archive::binary_oarchive ar(ofs);
		ar << *this;;
    }
    
    void restore_db(const char * filename){// open the archive
		std::ifstream ifs(filename, ios_base::binary);
		boost::archive::binary_iarchive ia(ifs);
		ia >> *this;
	}
  
public:
	db(){};
		
	db(string db_name) {
		path = "DataBases/";
		path += db_name+"/";
		boost::filesystem::create_directory(path);//creamos una nueva carpeta con la db
		boost::filesystem::create_directory(path+"tables/");	 		//creamos subcarpeta donde iran las tablas
	}
	
	void use_db(string db_name){
		path = "DataBases/";
		path += db_name+"/";
	}
	
	void create_table( string table_name, string rest_of_line ){  		
		
		boost::erase_all(rest_of_line, ",");
		string cols_type (rest_of_line.begin()+2, rest_of_line.end()-1);		//recortamos el string sin parentesis

		ofstream myfile ( path + "tables/" + table_name + ".dbf", ofstream::out);
		if (myfile.is_open()){
			myfile << cols_type+"\n";
			myfile.close();
		}
		else cout << "Unable to open file";
	}
	
	void insert_into_table( string table_name, string rest_of_line ){  				
		//quitamos los signos
		boost::erase_all(rest_of_line, ",");
		string data_to_insert (rest_of_line.begin()+2, rest_of_line.end()-1);	

		ofstream myfile ( path + "tables/" + table_name + ".dbf", ofstream::out| ios::app);
		if (myfile.is_open()){
			myfile << data_to_insert+"\n";
			myfile.close();
		}
		else cout << "Unable to open file";
	}
	
	
	void insert_auto( string table_name, string rest_of_line ){  
		vector<string> string_to_auto;
		vector< pair<int, int> > random_limits;
		string data, temp1, temp2;
		int cantidad_inserts, count_random=0, rand1, rand2;
		srand (time(NULL));
		
		ofstream myfile ( path + "tables/" + table_name + ".dbf", ofstream::out| ios::app);
		
		//quitamos los signos
		boost::erase_all(rest_of_line, ",");
		string data_to_insert (rest_of_line.begin()+2, rest_of_line.end()-1);	
		
		// don't skip whitespace
		stringstream buffer( data_to_insert );
		
		buffer >> cantidad_inserts;
		
 		string_to_auto.push_back("");
 		
		while( !buffer.eof() ){
			buffer >> data;

			if( data == "random(" ){
				string_to_auto.push_back("random_"+to_string(count_random) );
				
				buffer >> rand1;
				buffer >> temp2 >> rand2;
				buffer >> temp2;			//-----boto el " )"

				random_limits.push_back(make_pair(rand1, rand2));
				++count_random;
			}
			else string_to_auto.push_back(data);
		}
		
		for( int i =1; i<= cantidad_inserts; ++i){
			for( int j =0; j< string_to_auto.size(); ++j){
				if( string_to_auto[j]== "random_0"){
					myfile << to_string( rand() % (random_limits[0].second - random_limits[0].first+1) + random_limits[0].first ) +" ";
				}
				else if( string_to_auto[j]== "random_0"){
					myfile << to_string( rand() % (random_limits[0].second - random_limits[0].first+1) + random_limits[0].first ) +" ";
				}
				else myfile << string_to_auto[j]+ to_string(i) +" ";	
			}
			myfile << "\n";
		}	
		
		myfile.close();
	}
	
};  

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
				//return;
				cout<< endl;	
			}		
		}
		cout << endl;
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

class UPDATE{
private:

	pair<string, string> Parse_where ( string where_line ){
		pair<string, string> where_condition_;
		stringstream ss;
		string condition,col,value;
	
		boost::erase_all(where_line, ",");
		ss << where_line;

		while( ss >> condition ){	
			if( condition == "=" ){
				ss >> value;
				where_condition_ = make_pair( col , value );
				continue;
			}
			col = condition;
		}
		
		return where_condition_;
	}
	
	map<string,int> copy_header_table( ifstream & ifs, ofstream & ofs){	//retorna el numero de columna del set y de la col del where
		map<string,int> num_cols;
		int pos=0;
		string data, aux;
	
		getline(ifs, data);
		stringstream ssdata;
		ssdata << data;

		while ( ssdata >> aux ) {
			if( aux == where_condition.first ) {
				num_cols["where"] = pos;
			}
			if( aux == set_line.first ) {
				num_cols["set"] = pos;
			}
			++pos;
			ssdata >> aux;
		}
		
		ofs << data + "\n";
		
		return num_cols;		
	}

	void copy_updated_table( ifstream & ifs, ofstream & ofs, map<string,int> pos ){
	
		string data, data_temp;

		while (getline(ifs, data)) {
			data_temp = data;
			stringstream ssdata;
			ssdata << data;

			for( int j = 0; j <= pos["where"]; ++j)	
				ssdata >> data;
			
			if( data == where_condition.second ){
				stringstream ssdata;
				ssdata << data_temp;
				
				for( int j = 0; j < pos["set"]; ++j){
					ssdata >> data;
					ofs << data + ' ';
				}
				
				ofs << set_line.second;
				ssdata >> data;		//-----saltamos lo que acabamos de guardar
				
				if(!ssdata.eof()){
					getline( ssdata , data );
					ofs << data;
				}
				ofs << "\n";
				//break;
			}	
			else{
				ofs << data_temp + "\n";
			}
		}
		
		
	}
	
	
	void update_operation(){
		ifstream ifs ( path + "tables/" + from_table + ".dbf", std::ifstream::in);
		ofstream ofs ( path + "tables/aux.dbf", ofstream::out| ios::app);
		string data;

		map<string,int> pos = copy_header_table( ifs , ofs );
		copy_updated_table( ifs , ofs , pos );
		
		remove( ( path +"tables/" + from_table + ".dbf").c_str() );
		rename( ( path +"tables/aux.dbf").c_str(), ( path +"tables/" + from_table + ".dbf").c_str() );
		ofs.close();
		ifs.close();
	}
	
	
	void Parse_Update( string rest_of_line ){ //UPDATE name_table SET edad = 25 WHERE id = 19
		stringstream ss;
		ss << rest_of_line;
	
		string where, temp1, temp2;
	
		//identificar tabla -------- FROM
		ss >> from_table;		
	
		//Identificar la columna y el nuevo valor ------- SET columna = valor
		ss >> temp1 >> temp1 >> temp2 >> temp2; //DE AQUI SOLO ME QUEDO CON COLUMNA Y VALOR
		set_line = make_pair( temp1, temp2 );
	
		//analizo restricciones ------- WHERE
		ss >> where;				
		getline(ss, rest_of_line);
		where_condition = Parse_where(rest_of_line);

		update_operation();
	}
	
	string from_table;
	pair<string, string> where_condition;
	pair<string, string> set_line;
	
public:
	UPDATE ( string rest_of_line ){
		Parse_Update(rest_of_line);
	}


};


class DELETE{
private:

	pair<string, string> Parse_where ( string where_line ){
		pair<string, string> where_condition_;
		stringstream ss;
		string condition,col,value;
	
		boost::erase_all(where_line, ",");
		ss << where_line;

		while( ss >> condition ){	
			if( condition == "=" ){
				ss >> value;
				where_condition_ = make_pair( col , value );
				continue;
			}
			col = condition;
		}
		
		return where_condition_;
	}
	
	int copy_header_table( ifstream & ifs, ofstream & ofs){	//retorna el numero de columna del where
		int pos=0;
		string data, aux;
	
		getline(ifs, data);
		stringstream ssdata;
		ssdata << data;

		while ( ssdata >> aux ) {
			if( aux == where_condition.first ) 
				break;
			
			++pos;
			ssdata >> aux;
		}
		
		ofs << data + "\n";
		
		return pos;		
	}

	void copy_new_table( ifstream & ifs, ofstream & ofs, int pos ){
	
		string data, data_temp;
	
		while (getline(ifs, data)) {
			data_temp = data;
			stringstream ssdata;
			ssdata << data;

			for( int j = 0; j <= pos; ++j)	
				ssdata >> data;

			if( data != where_condition.second )
				ofs << data_temp + "\n";
			
		}
		
	}
	
	
	void delete_operation(){
		ifstream ifs ( path + "tables/" + from_table + ".dbf", std::ifstream::in);
		ofstream ofs ( path + "tables/aux.dbf", ofstream::out| ios::app);
		string data;

		int pos = copy_header_table( ifs , ofs );
		copy_new_table ( ifs , ofs , pos );
		
		remove( ( path +"tables/" + from_table + ".dbf").c_str() );
		rename( ( path +"tables/aux.dbf").c_str(), ( path +"tables/" + from_table + ".dbf").c_str() );
		ofs.close();
		ifs.close();
	}
	
	
	void Parse_Delete( string name, string rest_of_line ){ 
		stringstream ss;
		ss << rest_of_line;
		string where;
		
		//analizo restricciones ------- WHERE
		ss >> where;				
		getline(ss, rest_of_line);
		where_condition = Parse_where(rest_of_line);

		delete_operation();
	}
	
	string from_table;
	pair<string, string> where_condition;
	
public:
	DELETE ( string name, string rest_of_line ): from_table(name){
		Parse_Delete(name, rest_of_line);
	}
};


class sql_query{

private:
void print_help() {
	cout<<endl<<endl;
	cout << "   -   CREATE_DATA_BASE bd "  <<endl;
	cout << "   -   USE_DATA_BASE students_db "  <<endl;
	cout << "   -   CREATE_TABLE name_table (id INT, nombre VARCHAR, edad INT)"  <<endl;
	cout << "   -   CREATE_TABLE auto_table (id AUTO-INT, nombre AUTO-VARCHAR, edad RANDOM-INT, Ciudad AUTO-VARCHAR)"  <<endl;
	cout << "   -   INSERT name_table (18, 'Jose', 4)"  <<endl;
	cout << "   -   INSERT_AUTO auto_table (100,  , nombre_, random( 6 - 15 ), Ciudad_)"  <<endl;
	cout << "   -   SELECT * FROM name_table WHERE id = 28"  <<endl;
	cout << "   -   UPDATE name_table SET edad = 25 WHERE id = 19"  <<endl;
	cout << "   -   DELETE name_table WHERE id = 19"  <<endl <<endl;
}

db* currently_db;

public:	

sql_query(string query_): currently_db(NULL){	//CONSTRUCTOR, PARSE DEL INPUT
	
	
	stringstream ss;
	ss << query_;
	
	string func, name, rest_of_line;
	
	while( ss >> func ){
		if( func == "SELECT" ){
			getline(ss, rest_of_line);
			SELECT select(rest_of_line);
		}
		else if( func == "INSERT" ){
			ss >> name;
			getline(ss, rest_of_line);
			currently_db->insert_into_table(name, rest_of_line);
		}
		else if( func == "INSERT_AUTO" ){
			ss >> name;
			getline(ss, rest_of_line);
			currently_db->insert_auto(name, rest_of_line);
		}
		else if( func == "UPDATE" ){
			getline(ss, rest_of_line);
			UPDATE update(rest_of_line);
		}
		else if( func == "DELETE" ){
			ss >> name;
			getline(ss, rest_of_line);
			DELETE delete_(name, rest_of_line);
		}
		else if( func == "CREATE_DATA_BASE" ){
			ss >> name;
			currently_db = new db(name);
		}
		else if( func == "USE_DATA_BASE" ){
			ss >> name;
			currently_db->use_db(name);
		}
		else if( func == "CREATE_TABLE" ){
			ss >> name;
			getline(ss, rest_of_line);
			currently_db->create_table(name, rest_of_line);
		}
		else if( func == "HELP" || func == "help" ){
			print_help();
		}
	}
}
	
};





int main (){

	string query;
	
	while (true){
		cout << "  mysql -> ";
		getline (cin,query);
		sql_query query2( query );
		
	}
	/*
	//sql_query query1("CREATE_DATA_BASE students_db");
	sql_query query2("USE_DATA_BASE students_db");
	//sql_query query3("CREATE_TABLE name_table (id INT, nombre VARCHAR, edad INT)");
	//sql_query query4("INSERT name_table (18, 'Jose', 4)");
	//sql_query query5("SELECT * FROM name_table");
	//sql_query query6("SELECT nombre, edad FROM name_table");
	sql_query query7("SELECT * FROM auto_table");
	//sql_query query8("UPDATE name_table SET edad = 24 WHERE id = 20");
	//sql_query query9("UPDATE name_table SET nombre = 'Julio' WHERE id = 19");
	//sql_query query10("DELETE name_table WHERE id = 19");
	//sql_query query11("CREATE_TABLE auto_table (id AUTO-INT, nombre AUTO-VARCHAR, edad RANDOM-INT, Ciudad AUTO-VARCHAR)");
	//sql_query query12("INSERT_AUTO auto_table (100,  , nombre_, random( 6 - 15 ), Ciudad_)");
	//sql_query query12("INSERT_AUTO auto_table (100,  , nombre_, Ciudad_)");
	*/
	/*
	USE_DATA_BASE bd
	 CREATE_TABLE persona ( dni AUTO_INT, nombre AUTO_VARCHAR, departamento RANDOM_INT(1-100) 	
	 INSERT_AUTO persona (10000,  , nombre_, random( 1 - 100 ))
	 SELECT * FROM persona
	 SELECT * FROM persona WHERE departamento = 24
	 
	UPDATE persona SET departamento = 1111 WHERE departamento = 1
	DELETE persona WHERE departamento = 24
	*/
	return 0;
}


/************************************************************************************************************
*
*	La db contiene tablas, los datos de estas y el nombre de la db. Posteriormente será con indices.
*	sql_query ejecuta las consultas.
*
*	Creado por Anthony Benavides Arce el 11 / 09 / 18
*
*
/************************************************************************************************************/
