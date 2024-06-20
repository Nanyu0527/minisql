#include "catalog/indexes.h"

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name,
                                     const table_id_t table_id, const vector<uint32_t> &key_map,
                                     MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new(buf)IndexMetadata(index_id, index_name, table_id, key_map);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
  uint32_t offset=0;
  MACH_WRITE_UINT32(buf+offset,344528);
  offset+=sizeof(uint32_t);
  MACH_WRITE_UINT32(buf+offset,index_id_);
  offset+=sizeof(uint32_t);
  MACH_WRITE_UINT32(buf+offset,index_name_.length());
  offset+=sizeof(uint32_t);
  MACH_WRITE_STRING(buf+offset,index_name_);
  offset+=index_name_.length()*sizeof(char);
  MACH_WRITE_UINT32(buf+offset,table_id_);
  offset+=sizeof(uint32_t);
  MACH_WRITE_UINT32(buf+offset,key_map_.size());
  offset+=sizeof(uint32_t);
  for(uint32_t i = 0;i < key_map_.size() ; i++){
    MACH_WRITE_UINT32(buf+offset,key_map_[i]);
    offset+=sizeof(uint32_t);
  }
  return offset;
}

uint32_t IndexMetadata::GetSerializedSize() const {
  uint32_t offset = 0;
  offset+=5*sizeof(uint32_t);
  offset+=index_name_.length()*sizeof(char);
  offset+=sizeof(uint32_t)*key_map_.size();
  return offset;
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  uint32_t sizee=0;
  uint32_t magic = MACH_READ_UINT32(buf);
  buf+=sizeof(uint32_t);
  sizee+=sizeof(uint32_t);
  if(magic!=344528){
    printf("Index deserialization failed\n");
    return 0;
  }
  uint32_t i_d = MACH_READ_UINT32(buf);
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
  uint32_t t_d = MACH_READ_UINT32(buf);
  buf+=sizeof(uint32_t);
  sizee+=sizeof(uint32_t);
  std::vector <uint32_t> key_m;
  len = MACH_READ_UINT32(buf);
  buf+=sizeof(uint32_t);
  sizee+=sizeof(uint32_t);
  //sizee+=sizeof(uint32_t);
  for(uint32_t t = 0; t < len ; t++){
    uint32_t i = MACH_READ_UINT32(buf);
    key_m.push_back(i);
    buf+=sizeof(uint32_t);
    sizee+=sizeof(uint32_t);
  }
  index_meta =  ALLOC_P(heap,IndexMetadata)(i_d,s,t_d,key_m);
  return sizee;
}