#include "record/row.h"

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  uint32_t offset = 0;
  MACH_WRITE_UINT32(buf + offset,   210928 );
  
  offset += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf + offset, fields_.size());
  offset += sizeof(uint32_t);
  for(auto i = fields_.begin();i != fields_.end();i++)
  {
    char *arrays = new char[100];
    char *buf_tmp = arrays;
    uint32_t offset_tmp = (*i)->SerializeTo(buf_tmp);
    for(uint32_t j = 0;j < offset_tmp;j++)
    {
      *(buf+offset+j) = *(buf_tmp+j);
    }
    offset += offset_tmp;
    delete[] arrays;
  }
  return offset;
  // replace with your code here
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  uint32_t offset = 0;
  buf += sizeof(uint32_t);
  offset += sizeof(uint32_t);
  buf += sizeof(uint32_t);
  offset += sizeof(uint32_t);
  for(uint32_t i=0;i<schema->GetColumnCount();i++)
  {
    const Column* current_column = schema->GetColumn(i);
    TypeId current_typeid = current_column->GetType();
    Field *field_tmp = nullptr;
    
    field_tmp->DeserializeFrom(buf,current_typeid,&field_tmp,true,heap_);
    uint32_t type_size = field_tmp->DeserializeFrom(buf,current_typeid,&field_tmp,false,heap_);
    fields_.push_back(field_tmp);
    buf += type_size;
    offset += type_size;
  }
  return offset;
  
  // replace with your code here
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  // replace with your code here
  uint32_t offset = 2 * sizeof(uint32_t);
  for(auto i = fields_.begin();i != fields_.end();i++)
  {
    offset += (*i)->GetSerializedSize();
  }
  return offset;
}
/*
#include "record/row.h"
#include<iostream>
using namespace std;

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  uint32_t offset = 0;
  if(buf==nullptr)
    std::cout << "In Seri" << std::endl;
  //std::cout << buf[0] << std::endl;
  //std::cout << "In Seri" << std::endl;
  MACH_WRITE_UINT32(buf + offset,   (uint32_t)210928 );
  //std::cout << "In Seri2" << std::endl;
  offset += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf + offset, fields_.size());
  offset += sizeof(uint32_t);
  for(auto i = fields_.begin();i != fields_.end();i++)
  {
    char *arrays = new char[100];
    char *buf_tmp = arrays;
    uint32_t offset_tmp = (*i)->SerializeTo(buf_tmp);
    for(uint32_t j = 0;j < offset_tmp;j++)
    {
      *(buf+offset+j) = *(buf_tmp+j);
    }
    offset += offset_tmp;
    delete[] arrays;
  }
  return offset;
  // replace with your code here
}*/
/*
uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  uint32_t offset = 0;
  buf += sizeof(uint32_t);
  offset += sizeof(uint32_t);
  buf += sizeof(uint32_t);
  offset += sizeof(uint32_t);
  for(uint32_t i=0;i<schema->GetColumnCount();i++)
  {
    const Column* current_column = schema->GetColumn(i);
    TypeId current_typeid = current_column->GetType();
    Field *field_tmp = nullptr;
    
    field_tmp->DeserializeFrom(buf,current_typeid,&field_tmp,true,heap_);
    uint32_t type_size = field_tmp->DeserializeFrom(buf,current_typeid,&field_tmp,false,heap_);
    fields_.push_back(field_tmp);
    buf += type_size;
    offset += type_size;
  }
  return offset;
  
  // replace with your code here
}*/

/*
uint32_t Row::GetSerializedSize(Schema *schema) const {
  uint32_t flen, size=0;

  flen = fields_.size();
  std::cout << "flen:" << flen << std::endl;
  for(int i = 0; i < (int)flen; i++){
    size += sizeof(uint32_t);
    size += fields_[i]->GetSerializedSize();
    std::cout << "size" << i << ": " << size << std::endl;
  }

  return size + sizeof(uint32_t);
}
*/
/*
uint32_t Row::GetSerializedSize(Schema *schema) const {
  //1.Calculate the FieldNumber
  uint32_t ofs = 0;
  ofs += sizeof(uint32_t);
  //2.Calculate the Sizeof NullBitMap
  ofs += this->GetFieldCount();
  
  //3.Calculate the Size of the Field
  for (uint32_t i = 0; i < this->GetFieldCount(); i++) {
    if (fields_[i]->IsNull() != true) {
      ofs += fields_[i]->GetSerializedSize();
    }
  }
  return ofs;
}
*/