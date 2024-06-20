#include "catalog/table.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
  uint32_t offset=0;
  MACH_WRITE_UINT32(buf+offset,344528);
  offset+=sizeof(uint32_t);
  MACH_WRITE_UINT32(buf+offset,table_id_);
  offset+=sizeof(uint32_t);
  MACH_WRITE_UINT32(buf+offset,table_name_.length());
  offset+=sizeof(uint32_t);
  MACH_WRITE_STRING(buf+offset,table_name_);
  offset+=table_name_.length()*sizeof(char);
  MACH_WRITE_UINT32(buf+offset,root_page_id_);
  offset+=sizeof(uint32_t);
  //对主键进行序列化
  MACH_WRITE_UINT32(buf+offset,primary_key.size());
  offset+=sizeof(uint32_t);
  for(uint32_t i = 0 ;i < primary_key.size(); i++){
    MACH_WRITE_UINT32(buf+offset,primary_key[i]);
    offset+=sizeof(uint32_t);
  }

  uint32_t ofst =schema_->SerializeTo(buf+offset);
  offset+=ofst;
  return offset;
}

uint32_t TableMetadata::GetSerializedSize() const {
  uint32_t offset = 0;
  offset+=4*sizeof(uint32_t);
  offset+=sizeof(uint32_t)*primary_key.size();
  offset+=table_name_.length()*sizeof(char);
  offset+=schema_->GetSerializedSize();
  return offset;
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  uint32_t sizee=0;
  uint32_t magic = MACH_READ_UINT32(buf);
  buf+=sizeof(uint32_t);
  sizee+=sizeof(uint32_t);
  if(magic!=344528){
    printf("Index deserialization failed\n");
    return 0;
  }
  uint32_t t_d = MACH_READ_UINT32(buf);
  buf+=sizeof(uint32_t);
  sizee+=sizeof(uint32_t);
  uint32_t len = MACH_READ_UINT32(buf);
  buf+=sizeof(uint32_t);
  sizee+=sizeof(uint32_t);
  std::string s ;
  for(uint32_t i = 0 ; i < len;i++){
    s.push_back(buf[i]);
  }
  buf+=len;
  sizee+=len;
  uint32_t i_d = MACH_READ_UINT32(buf);
  buf+=sizeof(uint32_t);
  sizee+=sizeof(uint32_t);
  vector<uint32_t>pri;
  uint32_t lent = MACH_READ_UINT32(buf);
  buf+=sizeof(uint32_t);
  sizee+=sizeof(uint32_t);
  for(uint32_t i = 0;i<lent;i++){
    uint32_t element = MACH_READ_UINT32(buf);
    buf+=sizeof(uint32_t);
    sizee+=sizeof(uint32_t);
    pri.push_back(element);
  }
  TableSchema *_schema;
  //MemHeap heap* = 
  uint32_t ofst = _schema->DeserializeFrom(buf,_schema,heap);
  buf+=ofst;
  sizee+=ofst;
  table_meta = ALLOC_P(heap,TableMetadata)(t_d,s,i_d,_schema,pri);
  return sizee;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name,
                                     page_id_t root_page_id, TableSchema *schema, MemHeap *heap,vector<uint32_t>primary_key) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new(buf)TableMetadata(table_id, table_name, root_page_id, schema,primary_key);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema,vector<uint32_t>primary_key)
        : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema),primary_key(primary_key) {}
