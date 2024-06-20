#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  // ASSERT(false, "Not Implemented yet");
  
  //uint32_t i=size;
  uint32_t offset=0;
  MACH_WRITE_UINT32(buf+offset,89849);
  offset+=sizeof(uint32_t);
  uint32_t len = table_meta_pages_.size();
  MACH_WRITE_UINT32(buf+offset,len);
  offset+=sizeof(uint32_t);
  for(auto t = table_meta_pages_.begin();t!=table_meta_pages_.end();t++){
    MACH_WRITE_UINT32(buf+offset,t->first);
    offset+=sizeof(uint32_t);
    MACH_WRITE_INT32(buf+offset,t->second);
    offset+=sizeof(int32_t);
  }
  len = index_meta_pages_.size();
  MACH_WRITE_UINT32(buf+offset,len);
  offset+=sizeof(uint32_t);
  for(auto t = index_meta_pages_.begin();t!=index_meta_pages_.end();t++){
    MACH_WRITE_UINT32(buf+offset,t->first);
    offset+=sizeof(uint32_t);
    MACH_WRITE_INT32(buf+offset,t->second);
    offset+=sizeof(int32_t);
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  // ASSERT(false, "Not Implemented yet");
  //uint32_t sizee = 0;
  uint32_t offset = 0;
  uint32_t magic = MACH_READ_UINT32(buf);
  CatalogMeta *m = NewInstance(heap);
  offset+=sizeof(uint32_t);
  //sizee+=sizeof(uint32_t);

  if(magic!=89849){
    //cout<<magic<<endl;
    printf("Catalog deserialization failed\n");
    return 0;
  }
  std::map<table_id_t, page_id_t> table_p;
  uint32_t len = MACH_READ_UINT32(buf+offset);
  offset+=sizeof(uint32_t);
  //cout<<"len = "<<len<<endl;
  //sizee+=sizeof(uint32_t);
  for(uint32_t t = 0; t < len ; t++){
    table_id_t ti = MACH_READ_UINT32(buf+offset);
    offset+=sizeof(uint32_t);
    //sizee+=sizeof(uint32_t);
    page_id_t pi = MACH_READ_INT32(buf+offset);
    offset+=sizeof(int32_t);
    //sizee+=sizeof(int32_t);
    table_p[ti]=pi;
  }
  std::map<index_id_t, page_id_t> index_p;
  len = MACH_READ_UINT32(buf+offset);
  offset+=sizeof(uint32_t);
  //sizee+=sizeof(uint32_t);
  for(uint32_t t = 0; t < len ; t++){
    index_id_t ti = MACH_READ_UINT32(buf+offset);
    offset+=sizeof(uint32_t);
    //sizee+=sizeof(uint32_t);
    page_id_t pi = MACH_READ_INT32(buf+offset);
    offset+=sizeof(int32_t);
    //sizee+=sizeof(int32_t);
    index_p[ti]=pi;
  }
  //CatalogMeta *c = ALLOC_P(heap,CatalogMeta)();
  m->table_meta_pages_ = table_p;
  m->index_meta_pages_ = index_p;
  return m;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  // ASSERT(false, "Not Implemented yet");
  uint32_t offset=0;
  offset+=sizeof(uint32_t);
  offset+=sizeof(uint32_t);
  offset+=(table_meta_pages_.size()+index_meta_pages_.size())*(sizeof(uint32_t)+sizeof(int32_t));
  return offset;
}

CatalogMeta::CatalogMeta() {}


CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
        : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
          log_manager_(log_manager), heap_(new SimpleMemHeap()) {
  if(init == true){
    //cout<<"no"<<endl;
    // next_table_id_ = 0;
    // next_index_id_ = 0;
    catalog_meta_ = CatalogMeta::NewInstance(heap_);
    //page_id_t page_id = 0;
    Page *P = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_->SerializeTo(P->GetData());
    buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID,true);
  }
  else if(init == false){
    //cout<<"hello"<<endl;
    //cout<<next_table_id_<<endl;
    Page *P = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
    char* buf = P->GetData();
    catalog_meta_ = catalog_meta_->DeserializeFrom(buf,heap_);
    std::unordered_map<table_id_t,std::string> table_id_to_name;
    //cout<<catalog_meta_->table_meta_pages_.size()<<endl;
    //cout<<catalog_meta_->GetNextTableId()<<endl;
    for(auto t = catalog_meta_->table_meta_pages_.begin();t!=catalog_meta_->table_meta_pages_.end();t++){
      Page *p = buffer_pool_manager->FetchPage(t->second);
      TableMetadata *table_meta ;
      table_meta->DeserializeFrom(p->GetData(),table_meta,heap_);
      table_names_[table_meta->GetTableName()] = table_meta->GetTableId();
      TableInfo *table_info = table_info->Create(heap_);
      TableHeap *table_heap_ = TableHeap::Create(buffer_pool_manager_,table_meta->GetFirstPageId(),table_meta->GetSchema(),log_manager_,lock_manager_,heap_);
      table_info->Init(table_meta,table_heap_);
      tables_[table_meta->GetTableId()] = table_info;
      table_id_to_name[table_meta->GetTableId()] = table_meta->GetTableName();
    }
    for(auto t = catalog_meta_->index_meta_pages_.begin();t!=catalog_meta_->index_meta_pages_.end();t++){
      Page *p = buffer_pool_manager->FetchPage(t->second);
      IndexMetadata *index_meta;
      index_meta->DeserializeFrom(p->GetData(),index_meta,heap_);
      index_names_[table_id_to_name[index_meta->GetTableId()]][index_meta->GetIndexName()] = index_meta->GetIndexId();
      IndexInfo *index_info = index_info->Create(heap_);
      TableInfo* table_info = tables_[index_meta->GetTableId()];
      index_info->Init(index_meta,table_info,buffer_pool_manager);
      indexes_[index_meta->GetIndexId()] = index_info;
    }
    buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID,true);
  }
  
  // ASSERT(false, "Not Implemented yet");
}

CatalogManager::~CatalogManager() {
  Page *p;
  p = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(p->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,true);
  FlushCatalogMetaPage();
  delete heap_;
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info,vector<uint32_t>primary_key) {
  if(table_names_.count(table_name)!=0){
    return DB_TABLE_ALREADY_EXIST;
  }
  page_id_t table_mata_page_id;
 
  table_id_t table_id = catalog_meta_->GetNextTableId();
  Page *p;
  p=buffer_pool_manager_->NewPage(table_mata_page_id);
  if(p==nullptr){
    return DB_FAILED;
  }
  table_info = TableInfo::Create(heap_);
  TableMetadata *metadata = TableMetadata::Create(table_id,table_name,table_mata_page_id,schema,heap_,primary_key);
  //page_id_t table_mata_page_id;
  // auto table_meta_page = buffer_pool_manager_->NewPage(table_mata_page_id);
  // metadata->SerializeTo(table_meta_page->GetData());
  p->RLatch();
  metadata->SerializeTo(p->GetData());
  p->RUnlatch();
  buffer_pool_manager_->FlushPage(table_mata_page_id);
  buffer_pool_manager_->UnpinPage(table_mata_page_id,false);
  catalog_meta_->table_meta_pages_[table_id] = table_mata_page_id;
  //FlushCatalogMetaPage();
  // Page *P1 = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  // catalog_meta_->SerializeTo(P1->GetData());
  TableHeap *table_heap_ = TableHeap::Create(buffer_pool_manager_,schema,txn,log_manager_,lock_manager_,heap_);
  table_info->Init(metadata,table_heap_);
  tables_[table_id] = table_info;
  table_names_[table_name] = table_id;
  //cout<<table_id<<endl;
  // ASSERT(false, "Not Implemented yet");
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  auto table_idt = table_names_.find(table_name);
  if( table_idt == table_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  // if(index_names_.find(table_name)==index_names_.end()){
  //   std::cout<<"MYERROR1"<<endl;
  //   return DB_TABLE_NOT_EXIST;
  // }
  if(tables_.find(table_idt->second)==tables_.end()){
    return DB_TABLE_NOT_EXIST;
    std::cout<<"MYERROR"<<endl;
  } 
  table_info = tables_[table_names_[table_name]];
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  //cout<<tables_.size()<<endl;
  for(auto t = tables_.begin();t!=tables_.end();t++){
    //cout<<"yep"<<endl;
    tables.push_back(t->second);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  auto table_id_ = table_names_.find(table_name);
  if (table_id_ == table_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  auto indexmp = index_names_.find(table_name);
  if(indexmp != index_names_.end()&&indexmp->second.find(index_name) != indexmp->second.end()){
    return DB_INDEX_ALREADY_EXIST;
  }
  table_id_t table_id = table_names_[table_name];
  TableInfo *table_info = tables_[table_id];
  Schema *schema_ = table_info->GetSchema();
  for(auto t = index_keys.begin();t!= index_keys.end();t++){
    std::vector<Column *> columns = schema_->GetColumns();
    auto tt = columns.begin();
    for( ;tt!= columns.end();tt++){
      if(*t==(*tt)->GetName()){
        break;
      }
    }
    if(tt == columns.end()){
      return DB_COLUMN_NAME_NOT_EXIST;
    }
  }
  Page *p;
  page_id_t ipid;
  p=buffer_pool_manager_->NewPage(ipid);
  if(p==nullptr){
    return DB_FAILED;
  }
  //table_id_t table_id;
  //cout<<"before insert:"<<catalog_meta_->index_meta_pages_.size()<<endl;
  index_id_t index_id = catalog_meta_->GetNextIndexId();
  //cout<<"ID:"<<index_id<<endl;
  auto t1 = table_names_.find(table_name);
  if(t1==table_names_.end()){
    return DB_FAILED;
  }
  table_id = t1->second;
  auto t2 = tables_.find(table_id);
  if(t2==tables_.end()){
    return DB_FAILED;
  }
  TableInfo *tinfo = t2->second;
  Schema *sch = tinfo->GetSchema();
  vector<uint32_t>keymap;
  uint32_t Idx;
  for(auto t = index_keys.begin();t!=index_keys.end();t++){
    dberr_t ero = sch->GetColumnIndex(*t,Idx);
    if(ero != DB_SUCCESS){
      return ero;
    }
    keymap.push_back(Idx);
  }
  IndexMetadata *metadata = IndexMetadata::Create(index_id,index_name,t1->second,keymap,heap_);
  p->RLatch();
  //cout<<"meta1"<<metadata->GetIndexId()<<endl;
  metadata->SerializeTo(p->GetData());
  //cout<<"meta2"<<metadata->GetIndexId()<<endl;
  p->RUnlatch();
  buffer_pool_manager_->FlushPage(ipid);
  buffer_pool_manager_->UnpinPage(ipid,false);
  IndexInfo *iinfo = IndexInfo::Create(heap_);
  iinfo->Init(metadata,tinfo,buffer_pool_manager_);
  index_info = iinfo;
  //cout<<"info:"<<index_info->GetIndex()->getID()<<endl;//出问题
  catalog_meta_->index_meta_pages_[index_id] = ipid;
  
  index_names_[table_name][index_name] = index_id;
  indexes_[index_id] = iinfo;
  //cout<<"after insert:"<<catalog_meta_->index_meta_pages_.size()<<endl;
  return DB_SUCCESS;
  // ASSERT(false, "Not Implemented yet");
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  auto findt = table_names_.find(table_name);
  if(findt == table_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  auto findit = index_names_.find(table_name);
  if(findit == index_names_.end()){
    return DB_INDEX_NOT_FOUND;
  }
  auto findii = findit->second.find(index_name);
  if(findii == findit->second.end()){
    return DB_INDEX_NOT_FOUND;
  }
  auto idx = findii->second;
  // if(indexes_.find(idx)==indexes_.end()){
  //   return DB_INDEX_NOT_FOUND;
  // }
  index_info = indexes_.find(idx)->second;
  // ASSERT(false, "Not Implemented yet");
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  if(index_names_.find(table_name) == index_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  std::unordered_map<std::string, index_id_t>idxmp = index_names_.find(table_name)->second;
  for(auto t = idxmp.begin();t!=idxmp.end();t++){
    if(indexes_.find(t->second)!=indexes_.end()){
      indexes.push_back(indexes_.find(t->second)->second);
    }
  }
  return DB_SUCCESS;
}


dberr_t CatalogManager::DropTable(const string &table_name) {
  auto it = table_names_.find(table_name);
  if(it==table_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  auto idxmp=index_names_[table_name];
  for(auto t = idxmp.begin();t!=idxmp.end();t++){
    DropIndex(table_name, t->first);
  }
  delete tables_[it->second];
  buffer_pool_manager_->UnpinPage(catalog_meta_->table_meta_pages_.find(it->second)->second, false); //减除固定，设为非脏页
  buffer_pool_manager_->DeletePage(catalog_meta_->table_meta_pages_.find(it->second)->second);  //删除table数据
  tables_.erase(it->second);
  table_names_.erase(table_name);
  index_names_.erase(table_name);
  catalog_meta_->table_meta_pages_.erase(it->second);

  return DB_SUCCESS;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  auto tt =  table_names_.find(table_name);
  if(tt ==  table_names_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  std::unordered_map<std::string, index_id_t>imap = index_names_[table_name];
  auto it = imap.find(index_name);
  if(it == imap.end()){
    return DB_INDEX_NOT_FOUND;
  }
  delete indexes_[it->second];
  buffer_pool_manager_->UnpinPage(catalog_meta_->index_meta_pages_.find(it->second)->second, false); //减除固定，设为非脏页
  buffer_pool_manager_->DeletePage(catalog_meta_->table_meta_pages_.find(it->second)->second);  //删除table数据
  imap.erase(index_name);
  if(imap.begin()==imap.end()){
    index_names_.erase(table_name);
  }
  catalog_meta_->index_meta_pages_.erase(it->second);
  indexes_.erase(it->second);
  return DB_SUCCESS;
}


dberr_t CatalogManager::FlushCatalogMetaPage() const {
  Page *p = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  p->RLatch();
  catalog_meta_->SerializeTo(p->GetData());
  p->RUnlatch();
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,true);
  return DB_FAILED;
}
//不用写
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  TableInfo *table_info = TableInfo::Create(heap_);
  Page *p = buffer_pool_manager_->FetchPage(page_id);
  ASSERT(p != nullptr,"PAGE_NOT_EXIST");
  TableMetadata *table_meta;
  table_meta->DeserializeFrom(p->GetData(),table_meta,table_info->GetMemHeap());
  //std::pair<std::map<table_id_t, page_id_t>::iterator, bool> load_result = catalog_meta_->table_meta_pages_.emplace(table_id, page_id);
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_,table_meta->GetFirstPageId(),table_meta->GetSchema(),log_manager_,lock_manager_,heap_);
  table_info->Init(table_meta,table_heap);
  tables_[table_id] = table_info;
  table_names_[table_meta->GetTableName()] = table_meta->GetTableId();
  buffer_pool_manager_->UnpinPage(page_id,false);
  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  IndexInfo *index_info = IndexInfo::Create(heap_);
  Page *p = buffer_pool_manager_->FetchPage(page_id);
  ASSERT(p != nullptr,"PAGE_NOT_EXIST");
  IndexMetadata *index_meta;
  index_meta->DeserializeFrom(p->GetData(),index_meta,index_info->GetMemHeap());
  table_id_t table_id = index_meta->GetTableId();
  TableInfo *table_info = tables_[table_id];
  index_info->Init(index_meta,table_info,buffer_pool_manager_);
  indexes_[index_id] = index_info;
  index_names_[table_info->GetTableName()][index_info->GetIndexName()] = index_id;
  buffer_pool_manager_->UnpinPage(page_id,false);
  return DB_FAILED;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  auto tt = tables_.find(table_id);
  if(tt == tables_.end()){
    return DB_TABLE_NOT_EXIST;
  }
  table_info = tables_[table_id];
  return DB_SUCCESS;
}