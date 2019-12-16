/* Master.cpp */
/*
  此文件为Master.h的实现文件
*/

#include "Master.h"
#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <fstream>

using namespace std;


/*
  此函数为int转换成string函数专用
*/
string intToString(int temp) {    //int转换成string函数
	char str[100];
	for (int i = 0; i < 100; i++) str[i] = '\0';
	if (temp == 0) {
		str[0] = '0';
		string str1 = str;
		return str1;
	} else {
		int i;
		for (i = 0; temp != 0; i++) {
			int temp_ = temp % 10;
			str[i] = '0' + temp_;
			temp = temp / 10;
		}
		char str_[100];
		for (int j = 0; j < 100; j++) str_[j] = '\0';
		for (int j = i - 1, k = 0; j >= 0; j--, k++)
			str_[k] = str[j];
		
		string str1 = str_;
		cout << "查看string" << str1 << endl;
		return str1;
	}
}

vector <string> readAllQuery(string &fileName) {    //fileName为查询文件绝对路径，例如/home/test，不指定具体文件格式,返回查询文件名
	
	vector <string> result;   //子查询语句string
	string Dir = "./subQuery/";   //Master节点的子查询文件固定保存在subQuery下面
	string file = (Dir + fileName);  //file保存文件的绝对路径
	ifstream in(file.c_str());    //创建读文件对象
	string temp;
	string Str = "";
	if (!in.is_open()) {
		cout << "打开文件失败" << endl;
		return result;
	}
	while (getline(in, temp)) {   //找到第一个子查询的位置
		if (temp == "---") break;
	}
	while (getline(in, temp)) {
		if (temp == "---") {       //关闭旧写入对象，创建新写入对象
			result.push_back(Str);
			cout << "子查询语句" << Str << endl;
			Str = "";
		} else {                       //写入到对应文件对象
			Str = Str + temp;
		}
	}
	return result;
}


/*
  数据分解函数
*/
vector <string> dataDecompose(string &dataFile, string &last) {  //dataFile为数据文件名，last为数据后缀名，例如txt，区分大小写，空格表示无后缀
	string Dir = "./data/"; //原始数据默认存放在./data里面
	string subDir = "./subData/";  //子数据文件存放目录
	string cmdString = "java -cp dis-triplebitCHN-1.0-SNAPSHOT-jar-with-dependencies.jar placer.strategy.HashStrategy ";  //jar包默认放在根目录下
	//string cmdString = "java -cp dis-triplebit-1.0-SNAPSHOT-jar-with-dependencies.jar placer.strategy.HashStrategy ";  //jar包默认放在根目录下
	vector <string> result;
	
	//shell命令
	cmdString += Dir;
	cmdString += dataFile;
	if (last != " ") cmdString += ("." + last);
	cmdString += " ";
	cmdString += subDir;
	
	int pid;
	pid = system(cmdString.c_str());
	
	if (pid == 0) {
		cout << "数据分解成功" << endl;
		cout << "命令为：" << cmdString << endl;
		
		for (int i = 0; i < 3; i++) {    //分解的数据名字，不带格式，3代表数据分解的个数，即slave节点数
			string str = intToString(i);
			string str1 = dataFile + "-" + str;
			result.push_back(str1);
		}
	} else {
		cout << "数据分解失败" << endl;
		cout << "命令为：" << cmdString << endl;
	}
	return result;
}


/*
  查询语句分解函数
*/
string queryDecompose(string &queryFile) {       //queryFile为查询文件名，无后缀，例如lubm2,结果保存在。/subquery的目录下的queryFile文件中
	
	string Dir = "./Query/";
	string subDir = "./subQuery/";
	string result;
	string cmdString = "java -jar ./dis-triplebitCHN-1.0-SNAPSHOT-jar-with-dependencies.jar";
	//string cmdString = "java -jar dis-triplebit-1.0-SNAPSHOT-jar-with-dependencies.jar";
	cmdString += " ";
	cmdString += Dir;
	cmdString += queryFile;
	cmdString += " ";
	cmdString += subDir;
	cmdString += queryFile;
	
	int pid;
	pid = system(cmdString.c_str());
	
	if (pid == 0) {
		cout << "查询语句分解成功" << endl;
		cout << "查询分解命令为：" << cmdString << endl;
		result = queryFile;
	} else {
		cout << "查询语句分解失败" << endl;
		cout << "查询分解命令为：" << cmdString << endl;
		result = " ";
	}
	return result;
}

/*
 执行spark部分，并获取结果
 resultFile为查询结果文件名，保存在Maseter节点的./queryResult中的对应slave目录下
*/
vector <string> executeSpark(vector <string> &resultFile, vector <string> &slaveName) {
	vector <string> result;    //结果
	string Dir = "./queryResult/";
	
	//结果文件读取生成Vector
	vector <vector<string>> resultVec;
	
	
	return result;
}

/*
  提供给上层web的接口
*/
vector <string> allOperationOfQuery(string &QueryFile) {  //QueryFile为文件名，不带后缀
	
	vector <string> result;   //结果
	vector <string> slaveName;  //slave节点名字
	string allSubQuery;      //子查询汇总文件
	vector <string> subQuery;  //子查询文件
	
	//赋值slave节点名
	slaveName.push_back("slave1");
	slaveName.push_back("slave2");
	slaveName.push_back("slave3");
	
	//查询分解
	allSubQuery = queryDecompose(QueryFile);
	
	//将子查询总文件分解成多个文件
	subQuery = readAllQuery(allSubQuery);
	
	//将子查询文件分发到各个节点
	for (int i = 0; i < slaveName.size(); i++) {
		for (int j = 0; j < subQuery.size(); j++)
			sendQuery(subQuery.at(j), slaveName.at(i));
	}
	
	//执行查询文件
	result = executeQuery(subQuery, slaveName);
	
	return result;
}


/*
 在某个节点将数据文件加载成数据库
 dataFile是数据文件名，固定保存在。/subData目录下
 slaveName是节点名
 last是数据文件后缀格式，若为空代表无后缀
*/
int createSingleDatabase(string &dataFile, string &slaveName, string &last) {
	
	//string slaveAPI = "/opt/Grace/bin/lrelease/buildTripleBitFromCHN";   //创建数据库API所在
	string slaveAPI = "/opt/Grace/bin/lrelease/buildTripleBitFromN3";   //创建数据库API所在
	string dataDir = "/opt/Grace/data/"; //数据文件所在目录
	string databaseDir = "/opt/Grace/mydatabase";  //数据库文件目录
	string cmdString = "ssh";                      //shell命令
	cmdString += " ";
	cmdString += slaveName;
	cmdString += " ";
	cmdString += "\"";
	cmdString += slaveAPI;
	cmdString += " ";
	cmdString += dataDir;
	cmdString += dataFile;
	if (last != " ") cmdString += ("." + last);  //若last不为空，加上后缀
	cmdString += " ";
	cmdString += databaseDir;
	cmdString += "\"";
	
	int pid;
	const char *cmdChar = cmdString.c_str();
	pid = system(cmdChar);
	
	if (pid == 0) {
		cout << slaveName << " 创建数据库成功" << endl;
		cout << cmdString << endl;
	} else {
		cout << slaveName << " 数据库创建失败" << endl;
		cout << cmdString << endl;
	}
	return 0;
}

/**
 * 由于rawFacts文件格式问题，使用原来的java实现需要对文件进行多次读写
 * 考虑到可能存在的性能问题，把原来数据分解功能用该函数进行简单实现
 * @param rawFacts ID数据文件
 */
void realDataDecompose(TempFile &rawFacts) {
	MemoryMappedFile mappedIn;
	assert(mappedIn.open(rawFacts.getFile().c_str()));
	ID s, p, o;
	
	const char *reader = mappedIn.getBegin(), *limit = mappedIn.getEnd();
	
	
}

/**
 * 该函数专门用来解析演示用的数据集，非常dirty的操作
 * @param fileName 原始数据集
 * @param builder builder类实例
 */
void parserTriplesFile(string fileName, TripleBitBuilder *builder, TempFile &rawfacts) {
	std::ifstream in(fileName);
	std::string str;
	//in.open(fileName);
	while (getline(in, str)) {
		int pos1 = 0, pos2 = str.find("\t", 0);
		std::string subject = str.substr(pos1, pos2 - pos1);
		pos1 = pos2 + 1;
		pos2 = str.find("\t", pos1);
		std::string predicate = str.substr(pos1, pos2 - pos1);
		pos1 = pos2 + 1;
		int len = str.length();
		std::string object;
		if (str[len - 1] == '\r')
			object = str.substr(pos1, len - pos1 - 1);
		else
			object = str.substr(pos1, len - pos1);
		cout << object << "|" << predicate << "|" << subject << endl;
		if (predicate.size() && subject.size() && object.size())
			builder->NTriplesParse((char *) subject.c_str(), (char *) predicate.c_str(),
			                       (char *) object.c_str(), rawFacts);
	}
	in.close();
}

/**
 * 将原始数据文件映射成ID文件
 * @param dataFile 原始数据文件
 * @param targetDir 分解后的数据要存放的目录
 */
void startBuild(const string &dataFile, const string &targetDir) {
	if (!OSFile::directoryExists(targetDir)) {
		OSFile::mkdir(targetDir);
	}
	
	TripleBitBuilder *builder = new TripleBitBuilder(targetDir);
	TempFile rawFacts("./test");
	if (LANG) {
		// Created by peng on 2019-12-15, 16:35:09
		// chinese case
		parserTriplesFile(dataFile, builder, rawfacts);
	} else {
		// Created by peng on 2019-12-15, 16:35:21
		// english case
		
		ifstream in((char *) fileName.c_str());
		if (!in.is_open()) {
			cerr << "Unable to open " << fileName << endl;
			return ERROR;
		}
		if (!builder->N3Parse(in, fileName.c_str(), rawFacts)) {
			in.close();
			return ERROR;
		}
		in.close();
	}
	// Created by peng on 2019-12-15, 16:50:15
	// 将table中的数据写到磁盘上
	delete uriTable;
	uriTable = NULL;
	delete preTable;
	preTable = NULL;
	delete staReifTable;
	staReifTable = NULL;
	
	// Created by peng on 2019-12-15, 16:50:40
	// rawfacts文件中存放的是
	// IDIDID (小端方式，紧挨着存放)
	// IDIDID
	// 的形式，由原始数据文件经过映射生成。
	rawFacts.flush();
	
	// Created by peng on 2019-12-15, 21:37:58
	// FIXME：此处应该调用数据分解的代码
	system("dis-triplebitCHN-1.0-SNAPSHOT-jar-with-dependencies.jar")
	
	// Created by peng on 2019-12-15, 21:38:54
	// FIXME：数据分解之后应该生成多个rawFacts文件，每个slave节点一个，
	// FIXME：在slave节点直接调用完整的resolveTriples函数(bitmap插入未被删除的函数)
	builder->resolveTriples(rawFacts, rawFacts);
	rawFacts.discard();
	
	builder->endBuild();
	delete builder;
}

/*
 将初始数据文件进行分解，并生成分布式数据库
 dataFile为初始数据文件名，不带后缀，默认保存在./data目录下
 slaveName为节点名
 last为文件后缀格式名，如txt
*/
int createDatabase(string &dataFile, vector <string> &slaveName, string &last) {
	vector <string> subDataFile;     //子数据文件名，不带后缀
	
	// Created by peng on 2019-12-15, 15:36:10
	// TODO: 数据分解前需要做ID全局映射

//分解数据文件
	subDataFile = dataDecompose(dataFile, last);

//发送数据文件
	for (int i = 0; i < subDataFile.size(); i++) {
		sendData(subDataFile.at(i), slaveName.at(i), last);
	}

//创建数据库，可用多线程
	for (int i = 0; i < subDataFile.size(); i++) createSingleDatabase(subDataFile.at(i), slaveName.at(i), last);
	return 1;
}


/*
  开启slave节点上的服务器监听
*/

void startServer(vector <string> slaveName) {
	
	string cmdString = "ssh";
	
	string Dir = "/opt/Build/bin/lrelease/searchOldDatabase";
	
	for (int i = 0; i < slaveName.size(); i++) {
		
		int pid;
		string str = cmdString + " ";
		str += slaveName.at(i);
		str += " ";
		str += "\"";
		str += Dir;
		str += "\"";
		
		pid = system(str.c_str());
		if (pid == 0) {
			cout << slaveName.at(i) << "开启监听成功" << endl;
		} else cout << slaveName.at(i) << "开启监听失败" << endl;
	}
}

/* Master.cpp */
