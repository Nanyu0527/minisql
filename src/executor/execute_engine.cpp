#include "executor/execute_engine.h"
#include "glog/logging.h"

ExecuteEngine::ExecuteEngine() {

}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context);
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context);
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context);
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context);
    case kNodeShowTables:
      return ExecuteShowTables(ast, context);
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context);
    case kNodeDropTable:
      return ExecuteDropTable(ast, context);
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context);
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context);
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context);
    case kNodeSelect:
      return ExecuteSelect(ast, context);
    case kNodeInsert:
      return ExecuteInsert(ast, context);
    case kNodeDelete:
      return ExecuteDelete(ast, context);
    case kNodeUpdate:
      return ExecuteUpdate(ast, context);
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context);
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context);
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context);
    case kNodeExecFile:
      return ExecuteExecfile(ast, context);
    case kNodeQuit:
      return ExecuteQuit(ast, context);
    default:
      break;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
//#ifdef ENABLE_EXECUTE_DEBUG
  //LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
  
//#endif
  string name = ast->child_->val_;
  fstream text_file;
  text_file.open(textname,ios::in);
  string read;
  //LOG(ERROR) << "DATABASE ALREADY EXISTS" << endl;
  if(!text_file.good()){
    LOG(ERROR) << "Can't open record.txt" << endl;
    return DB_FAILED;
  }
  //LOG(ERROR) << "DATABASE ALREADY EXISTS" << endl;
  while(getline(text_file,read)){
    if(read.find(name)!=string::npos){
      LOG(ERROR) << "DATABASE ALREADY EXISTS" << endl;
      return DB_FAILED;
    }
  }
  //LOG(ERROR) << "DATABASE ALREADY EXISTS" << endl;
  text_file.close();
  text_file.open(textname,ios::app);
  DBStorageEngine *newdb = new DBStorageEngine(name,true);
  dbs_[name] = newdb;
  text_file<<name<<endl;
  // cout<<name;
  text_file.close();
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  //pSyntaxNode ast_child = ast->child_;
  int flag = 1;
  string name = ast->child_->val_;
  string buff = "../../storage.txt";
  fstream text_file;
  fstream store;
  store.open(buff,ios::out);
  text_file.open(textname,ios::in);
  string read;
  if(!text_file){
    LOG(ERROR) << "Can't open record.txt" << endl;
    return DB_FAILED;
  }
  if(!store){
    LOG(ERROR) << "Can't open storage.txt" << endl;
    return DB_FAILED;
  }
  while(getline(text_file,read)){
    if(read.find(name)!=string::npos){
      flag = 0;
      continue;
    }
    store<<read<<endl;
  }
  store.close();
  text_file.close();
  if(flag == 1){
    remove(buff.c_str());
    LOG(ERROR) << "DATABASE NOT EXISTS" << endl;
    return DB_FAILED;
  }
  store.open(buff,ios::in);
  text_file.open(textname,ios::out);
  if(!text_file){
    LOG(ERROR) << "Can't open second record.txt" << endl;
    return DB_FAILED;
  }
  if(!store){
    LOG(ERROR) << "Can't open second storage.txt" << endl;
    return DB_FAILED;
  }
  while(getline(store,read)){
    text_file<<read<<endl;
  }
  text_file.close();
  store.close();
  remove(buff.c_str());
  remove(name.c_str());
  if(name == current_db_){
    current_db_ = "";
  }
  dbs_.erase(name);

  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  fstream text_file;
  text_file.open(textname,ios::in);
  if(!text_file){
    LOG(ERROR) << "Can't open record.txt" << endl;
    return DB_FAILED;
  }
  string read;
  while(getline(text_file,read)){
    cout<<"database name :"<<read<<endl;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  fstream text_file;
  text_file.open(textname,ios::in);
  if(!text_file){
    LOG(ERROR) << "Can't open record.txt" << endl;
    return DB_FAILED;
  }
  string read;
  while(getline(text_file,read)){
    if(read.find(ast->child_->val_)!=string::npos){
      current_db_ = ast->child_->val_;
      DBStorageEngine *newdb = new DBStorageEngine(ast->child_->val_,false);
      dbs_[ast->child_->val_] = newdb;
      return DB_SUCCESS;
    }
  }
  LOG(ERROR) << "Can't Find Database" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
if(current_db_==""){
    LOG(ERROR) << "DATABASE NOT EXISTS" << std::endl;
    return DB_FAILED;
  }
  string table_name;
  vector<TableInfo *> tables_info;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables_info);
  for(auto t = tables_info.begin();t!=tables_info.end();t++){
    cout<<"table_name :"<<(*t)->GetTableName()<<endl;
    Schema *schema_ = (*t)->GetSchema();
    vector<Column *>columns_ = schema_->GetColumns();
    for(auto i = columns_.begin() ; i!=columns_.end();i++){
      cout<<"column name: "<<(*i)->GetName();
      if((*i)->GetType() ==kTypeInt){
        cout<<" int ";
      }
      else if((*i)->GetType() ==kTypeChar){
        cout<<" char "<<(*i)->GetLength()<<" ";
      }
      else if((*i)->GetType() ==kTypeFloat){
        cout<<" float ";
      }
      if((*i)->IsUnique()){
        cout<<"unique";
      }
      cout<<endl;
    }
    vector<uint32_t>pri = (*t)->Getpri();
    if(pri.size()!=0){
      cout<<"Primary Key : ";
      for(auto t = pri.begin();t!=pri.end();t++){
        cout<<columns_[*t]->GetName()<<" ";
      }
      cout<<endl;
    }
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if(current_db_==""){
    LOG(ERROR) << "DATABASE NOT EXISTS" << std::endl;
    return DB_FAILED;
  }

  pSyntaxNode child = ast->child_;
  string table_name = child->val_;
  std::vector<TableInfo *> tables_ ;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables_);
  for(auto i = tables_.begin();i!=tables_.end();i++){
    if(table_name == (*i)->GetTableName()){
      LOG(ERROR) << "TABLE ALREADY EXISTS" << std::endl;
      return DB_FAILED;
    }
  }
  pSyntaxNode node_column = child->next_;
  pSyntaxNode p = node_column->child_;
  //SimpleMemHeap heap_c;
  vector<string>col_name;
  vector<uint32_t>pri_pos;//从0计数
  vector<Column*> columns_;
  uint32_t column_index = 0;
  
  while(p!=nullptr){
    if(p->type_ == kNodeColumnList){//主键
      pSyntaxNode p1 = p->child_;
      while(p1!=nullptr){
        string col_ = p1->val_;
        auto t = col_name.begin();
        for( ; t != col_name.end();t++){
          if(*t==col_){
            pri_pos.push_back(t-col_name.begin());
            break;
          }
        }
        //cout<<"haha"<<endl;
        if(t == col_name.end()){
          LOG(ERROR) << "PRIMARY KEY NAME NOT EXIST" << std::endl;
          return DB_FAILED;
        }
        p1=p1->next_;
      }
      for(auto t = pri_pos.begin();t!=pri_pos.end();t++){
        //cout<<"pri = "<<*t<<endl;
      }
      break;
    }
    bool isunique = false;
    //string isunq = p->val_;
    //cout<<"heihei"<<endl;
    if(p->val_!=NULL){
      isunique = true;
    }
    pSyntaxNode pch = p->child_;
    if(pch==nullptr){
      LOG(ERROR)<<"operation error 1"<<std::endl;
      return DB_FAILED;
    }
    string node_name = pch->val_;
    col_name.push_back(node_name);
    pSyntaxNode ps = pch->next_;
    if(ps==nullptr){
      LOG(ERROR)<<"operation error"<<std::endl;
      return DB_FAILED;
    }
    string type = ps->val_;
    TypeId type_ ;
    if(type == "int")
      type_ = kTypeInt;
    else if(type == "char")
      type_ =kTypeChar;
    else if(type == "float"){
      type_ =kTypeFloat;
    }
    if(type=="char"){
      uint32_t len = atoi(ps->child_->val_);
      Column *column_ =  new Column(node_name, type_, len, column_index, false, isunique);
      //cout<<"char"<<isunique<<endl;
      columns_.push_back(column_);
    }
    else{
      //cout<<node_name<<endl;
      Column *column_ =  new Column(node_name, type_, column_index, false, isunique);
      //cout<<column_->GetName()<<endl;//过了
      //cout<<"notchar"<<isunique<<endl;
      columns_.push_back(column_);
    }
    column_index++;
    p=p->next_;
  }
  //MemHeap _heap;
  //Transaction *txn = nullptr;
  //cout<<columns_[0]->GetName()<<endl;
  Schema *schema_ = new Schema(columns_);
  TableInfo *table_info;
  dbs_[current_db_]->catalog_mgr_->CreateTable(table_name,schema_,nullptr,table_info,pri_pos);
  // vector<Column*> columns = table_info->GetSchema()->GetColumns();
  // cout<<columns[0]->GetName()<<endl;
  //table_id_t = dbs_[current_db_]->catalog_mgr_->
  //dbs_[current_db_]->catalog_mgr_->GetTable()

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if(current_db_==""){
    LOG(ERROR) << "DATABASE NOT EXISTS" << std::endl;
    return DB_FAILED;
  }
  string name = ast->child_->val_;
  //cout<<name<<endl;
  dbs_[current_db_]->catalog_mgr_->DropTable(name);
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}
