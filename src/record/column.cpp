#include "record/column.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), table_ind_(index),
          nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt :
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat :
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}
Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), len_(length),
          table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other) : name_(other->name_), type_(other->type_), len_(other->len_),
                                      table_ind_(other->table_ind_), nullable_(other->nullable_),
                                      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const {
  //uint32_t i=size;
  uint32_t offset=0;
  // MACH_WRITE_UINT32(buf,Size);
  // offset=sizeof(uint32_t);
  MACH_WRITE_UINT32(buf+offset,210928);
  offset+=sizeof(uint32_t);
  MACH_WRITE_UINT32(buf+offset,name_.length());
  offset+=sizeof(uint32_t);
  MACH_WRITE_STRING(buf+offset,name_);
  offset+=name_.length()*sizeof(char);
  MACH_WRITE_TO(TypeId,buf+offset,type_);
  offset+=sizeof(TypeId);
  if(type_==kTypeChar){
    MACH_WRITE_UINT32(buf+offset,len_);
    offset+=sizeof(uint32_t);
  }
  MACH_WRITE_UINT32(buf+offset,table_ind_);
  offset+=sizeof(uint32_t);
  MACH_WRITE_TO(bool,buf+offset,nullable_);
  offset+=sizeof(bool);
  MACH_WRITE_TO(bool,buf+offset,unique_);
  offset+=sizeof(bool);
  return offset;
}

uint32_t Column::GetSerializedSize() const {
  if(name_.length()==0){
    return 0;
  }
  uint32_t offset;
  offset=sizeof(uint32_t)*3;
  offset+=2*sizeof(bool);
  offset+=sizeof(TypeId);
  offset+=name_.length()*sizeof(char);
  if(type_==kTypeChar){
    offset+=sizeof(uint32_t);
  }
  return offset;
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  // replace with your code here
  uint32_t sizee=0;
  uint32_t magic = MACH_READ_UINT32(buf);
  buf+=sizeof(uint32_t);
  sizee+=sizeof(uint32_t);

  if(magic!=210928){
    printf("Column deserialization failed\n");
    return 0;
  }

  uint32_t len = MACH_READ_UINT32(buf);
  buf+=sizeof(uint32_t);
  sizee+=sizeof(uint32_t);
  std::string s ;
  for(uint32_t i = 0 ; i < len;i++){
    s.push_back(buf[i]);
  }
  buf+=len;
  sizee+=len;
  TypeId typ = MACH_READ_FROM(TypeId, buf);
  buf+=sizeof(TypeId);
  sizee+=sizeof(TypeId);
  uint32_t Len=0;
  if(typ==kTypeChar){
    Len = MACH_READ_UINT32(buf);
    buf+=sizeof(uint32_t);
    sizee+=sizeof(uint32_t);
  }
  uint32_t table = MACH_READ_UINT32(buf);
  buf+=sizeof(uint32_t);
  sizee+=sizeof(uint32_t);
  uint32_t nable = MACH_READ_FROM(bool, buf);
  buf+=sizeof(bool);
  sizee+=sizeof(bool);
  uint32_t unq = MACH_READ_FROM(bool,buf);
  sizee+=sizeof(bool);
  if(typ==kTypeChar){
    column = ALLOC_P(heap,Column)(s, typ, Len, table, nable, unq);
  }
  else{
    column = ALLOC_P(heap,Column)(s, typ, table, nable, unq);
  }
  return sizee;
}
