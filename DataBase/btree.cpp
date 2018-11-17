#include <iostream>
#include <vector>
#include <math.h>  
#define M 5 //grado del arbol, puede tener M hijos y M-1 claves
#define mid 2
using namespace std;

template< class T>
class Node
{
public:
	vector<T> data;
	vector<Node*> hijos;
	Node* parent;
	
	vector<vector<int>> ubicacion;
	
	Node() :parent(NULL) {}
	Node(T value) :parent(NULL) { data.push_back(value); }
	~Node() { data.clear(); hijos.clear(); }
};

template< class T>
class BTree {
private:
	inline int specific_search_Node(T key, Node<T>* node_to_search);
	Node<T>* split_child(Node<T>*& node_to_split);
	void balance(Node<T>*& node_to_balance);
public:
	Node<T>* root;

	BTree();
	BTree(T initial_value);
	int search_Node(Node<T>* cur_node, T value_to_find, Node<T>*& node_to_save);
	int search_specific_node(Node<T>* cur_node, T value_to_find, Node<T>*& node_to_save);
	bool insert(T new_value);
	void print(Node<T>* cur_node);
	void erase_Node(T x);
};

template< class T>
BTree<T>::BTree() :root(NULL) {}

template< class T>
BTree<T>::BTree(T initial_value) : root(new Node<T>(initial_value)) {}

template< class T>
inline int BTree<T>::specific_search_Node(T key, Node<T>* node_to_search) {
	int i;
	for (i = 0; i < node_to_search->data.size() && i < M - 1; i++) {
		if (key < node_to_search->data[i])
			return i;
	}
	return i;
}

template< class T>
Node<T>* BTree<T>::split_child(Node<T>*& node_to_split) {
	Node<T>* right_child = new Node<T>;
	right_child->parent = node_to_split->parent;
	if (node_to_split->hijos.size() != 0) {
		(right_child->hijos).assign((node_to_split->hijos).begin() + mid + 1, (node_to_split->hijos).end());
		(node_to_split->hijos).erase((node_to_split->hijos).begin() + mid + 1, (node_to_split->hijos).end());
	}
	(right_child->data).assign((node_to_split->data).begin() + mid + 1, (node_to_split->data).end());
	(node_to_split->data).erase((node_to_split->data).begin() + mid, (node_to_split->data).end());
	return right_child;
}

template< class T>
void BTree<T>::balance(Node<T>*& Node_to_insert) {
	T key = Node_to_insert->data[mid];
	Node<T> * right_child = split_child(Node_to_insert);

	if (Node_to_insert == root) {	//caso en el que la raiz esta full
		Node<T>* new_root = new Node<T>(key);
		new_root->hijos.push_back(Node_to_insert);
		new_root->hijos.push_back(right_child);
		Node_to_insert->parent = new_root;
		right_child->parent = new_root;
		root = new_root;
	}
	else {
		int position = specific_search_Node(key, Node_to_insert->parent);
		(Node_to_insert->parent->data).insert((Node_to_insert->parent->data).begin() + position, key);

		if (position == 0) //caso en el que se crea el nuevo hijo al inicio del vector hijos, a diferencia de los demas insert este va a la izquierda.
			(Node_to_insert->parent->hijos).insert((Node_to_insert->parent->hijos).begin()+1, right_child);
		else 
			(Node_to_insert->parent->hijos).insert((Node_to_insert->parent->hijos).begin() + position + 1, right_child);

		if ( (Node_to_insert->parent->data).size() == M)//como hace overflow hay que balancear el arbol		
			balance( (Node_to_insert->parent) );
	}
}


template< class T>
int BTree<T>::search_Node(Node<T>* cur_node, T value_to_find, Node<T>*& node_to_save) {
	int i;
	if (cur_node->hijos.size() == 0) { //veo si esta es una hoja o no
		node_to_save = cur_node;
		for (i = 0; i < cur_node->data.size(); i++) {
			if (value_to_find == cur_node->data[i])
				return -1;

			if (value_to_find < cur_node->data[i])
				return i;
		}
		return i;
	}

	for (i = 0; i < cur_node->data.size(); i++) { 	//bajo por el arbol hasta encontrar el mismo dato o hasta encontrar una hoja, veo si esta en la hoja arriba
		if (value_to_find == cur_node->data[i]) {
			node_to_save = cur_node;
			return -1;
		}

		if (value_to_find < cur_node->data[i]) {
			return BTree<T>::search_Node(cur_node->hijos[i], value_to_find, node_to_save);
		}
	}
	return BTree<T>::search_Node(cur_node->hijos[i], value_to_find, node_to_save);

}

template< class T>
bool BTree<T>::insert(T new_value) {
	if (root == NULL) {
		root = new Node<T>(new_value);		
	}
	else {
		Node<T>* Node_to_insert;
		int posToInsert;
		posToInsert = BTree<T>::search_Node(root, new_value, Node_to_insert);   

		if (posToInsert == -1) return false; //ya estaba insertado 

		Node_to_insert->data.insert(Node_to_insert->data.begin() + posToInsert, new_value); //se coloca en la posicion que debe ir, siempre se inserta en una hoja

		if (Node_to_insert->data.size() == M) {//como hace overflow hay que balancear el arbol		
			balance(Node_to_insert);
			return true;
		}
	}
	return true;
}

template< class T>
void BTree<T>::print(Node<T>* cur_node) {//inorden
	cout << endl;
	int i;
	for (i = 0; i < cur_node->data.size(); i++)
	{
		if (cur_node->hijos.size() > 0)//no es hoja?
			print(cur_node->hijos[i]);

		cout << " " << cur_node->data[i];
	}
	if (cur_node->hijos.size() > 0)//no es hoja?
		print(cur_node->hijos[i]);

	cout << endl;
}



int main(int argc, char *argv[]) {
	BTree<int> a;
	a.insert(4);
	a.insert(5);
	a.insert(6);
	a.insert(3);
	a.insert(7);//
	a.insert(8);
	a.insert(9);
	a.insert(10);
	a.insert(11);//
	a.insert(12);
	a.insert(13);
	a.insert(14);
	a.insert(15);
	a.insert(16);//
	a.insert(17);
	a.insert(18);
	a.insert(19);

	/*a.insert(3);
	a.insert(5);
	a.insert(7);
	a.insert(8);
	a.insert(4);
	a.insert(0);
	a.insert(1);
	a.insert(2);*/
	
	a.print(a.root);
	return 0;
}







//https://www.sanfoundry.com/cpp-program-implement-b-tree/
//https://xcodigoinformatico.blogspot.com/2012/09/arbol-b-codigo-c-btree-code.html
//http://btechsmartclass.com/DS/U5_T3.html

