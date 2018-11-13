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
using namespace std;

extern string path;


class SELECT{
private:
	vector<string> print_header_table( ifstream & ifs );
	void print_data_table( ifstream & ifs );
	void print_one_row( ifstream & ifs,  vector<string> header, pair<string, string> where_condition );
	vector< pair<string, string> > Parse_where ( string where_line );
	void select_operation( vector <string> cols_select, string from_table, vector< pair<string, string> > where_condition );
	void Parse_Select( string rest_of_line );
public:
	SELECT ( string rest_of_line );
};
