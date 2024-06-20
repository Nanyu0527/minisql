/*#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t Ofst=0;
  MACH_WRITE_UINT32(buf+Ofst,200715);
  Ofst+=sizeof(uint32_t);
  uint32_t SIZE = columns_.size();
  MACH_WRITE_UINT32(buf+Ofst,SIZE);
  Ofst+=sizeof(uint32_t);
  for(auto i = columns_.begin();i!=columns_.end();i++){
    char *space = new char[100];
    char *b = space;
    uint32_t offset = (*i)->SerializeTo(b);
    for(uint32_t j=0;j<offset;j++){
      *(buf+Ofst+j) = *(b+j);
    }
    Ofst += offset;
    delete[] space;
  }
  return Ofst;
  // replace with your code here
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t Ofst=2*sizeof(uint32_t);//magic num
  for(auto i = columns_.begin();i!=columns_.end();i++){
    uint32_t offset = (*i)->GetSerializedSize();
    // uint32_t subnum = sizeof(uint32_t);
    // offset-=subnum;
    Ofst+=offset;
  }
  // replace with your code here
  return Ofst;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {

  uint32_t Off = 0;
  uint32_t magicn = MACH_READ_UINT32(buf);
  if(magicn!=200715){
    printf("Schema Deserialization failed\n");
    return 0;
  }
  Off+=sizeof(uint32_t);
  buf+=sizeof(uint32_t);
  uint32_t SIZE = MACH_READ_UINT32(buf);
  std::vector<Column *> columns;
  Off+=sizeof(uint32_t);
  buf+=sizeof(uint32_t);
  //printf("%d",SIZE);
  while(SIZE>0){
    //char *b=NULL;
    Column* tmp =nullptr;
    uint32_t offset=tmp->DeserializeFrom(buf, tmp, heap);
    columns.push_back(tmp);
    //uint32_t offset=schema->columns_[i]->DeserializeFrom(buf, schema->columns_[i], heap);
    buf+=offset;
    Off+=offset;
    SIZE--;
  }
  schema=ALLOC_P(heap,Schema)(columns);
  // replace with your code here
  return 0;
}*/
#include "record/schema.h"
#include<iostream>
using namespace std;
uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t ofs = 0;

  std::vector<Column *> columns_ = this->GetColumns();
  
  //1.Write the Magic Number
  MACH_WRITE_UINT32(buf, SCHEMA_MAGIC_NUM);
  ofs += 4;
  //2.Write the size of the columns
  MACH_WRITE_UINT32(buf + ofs, (columns_.size()));
  ofs += 4;
  //3.Write the Columns the into the buf
  for (uint32_t i = 0; i < columns_.size(); i++) {
    //Write the Serialized Size of the Each Size
    MACH_WRITE_UINT32(buf + ofs, (columns_[i]->GetSerializedSize()));
    ofs += 4;
    //Write the Serialized the Column into the buf
    columns_[i]->SerializeTo(buf + ofs);
    ofs += columns_[i]->GetSerializedSize();
  }
  
  return ofs;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  std::vector<Column *> columns_ = this->GetColumns();
  uint32_t LengthOfTable = columns_.size();
  uint32_t Size = 0;
  //1.Calculate the Magic Number and SizeOf(Columns)
  Size += 2*sizeof(uint32_t);
  //2.Calculate the Total Column
  for (uint32_t i = 0; i < LengthOfTable; i++) {
      //The SerializedSize
        Size += sizeof(uint32_t);
      Size += columns_[i]->GetSerializedSize();
  }
  return Size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  // replace with your code here
  if (buf == NULL) return 0;
  uint32_t ofs = 0;
  std::vector<Column *> columns_;//Which will used to construct in the schema

  //1.Read the Magic_Number
  uint32_t Magic_Number = MACH_READ_FROM(uint32_t, (buf));
  ofs += 4;
  
  //If does not match---Error
  if (Magic_Number != 200715) {
    std::cerr << "MagicNumber Does not match" << std::endl;
  }

  //2.Read the SizeOfColumns From the buf
  uint32_t LengthOfTable = MACH_READ_FROM(uint32_t, (buf + ofs));
  ofs += 4;
  //3.Read the Columns in the Schema
  for (uint32_t i = 0; i < LengthOfTable; i++) {
    //uint32_t SerializedSize = MACH_READ_FROM(uint32_t, (buf + ofs));
    ofs += 4;
    Column *tmp = nullptr;
    ofs += Column::DeserializeFrom(buf + ofs, tmp, heap);
    columns_.push_back(tmp);
  }
  void *mem = heap->Allocate(sizeof(Schema));
  schema = new (mem)Schema(columns_);
  return ofs;
}