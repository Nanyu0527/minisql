/*#include "page/bitmap_page.h"

template<size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  page_offset = next_free_page_;
  page_allocated_++;
  return false;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  return false;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  return false;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return false;
}

template
class BitmapPage<64>;

template
class BitmapPage<128>;

template
class BitmapPage<256>;

template
class BitmapPage<512>;

template
class BitmapPage<1024>;

template
class BitmapPage<2048>;

template
class BitmapPage<4096>;*/
#include "page/bitmap_page.h"

template<size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if(page_allocated_>MAX_CHARS*8)
    return false;
  uint32_t i;
  for(i=0; i<= 8*MAX_CHARS; i++)
  {
    if(IsPageFree(i))
      break;
  }
  if(i > 8*MAX_CHARS)
    return false;
  page_offset=i;
  page_allocated_++;
  //next_free_page_++;
  uint32_t byteIndex = page_offset / 8;
  uint32_t bitIndex = page_offset % 8;
  bytes[byteIndex] |= 1 << (7-bitIndex);
  /*for(i = next_free_page_/8;i < MAX_CHARS ;i++){
    unsigned char c = bytes[i];
    if(c==C) continue;
    unsigned char buff=bytes[i];
    for(uint32_t j = 0;j < 8;buff=buff<<1,j++){
      unsigned char tmp = buff>>7;
      if(tmp==0){
        x=j;
        unsigned char t=128;
        for(uint32_t k = 0;k < j;k++){
          t/=2;
        }
        bytes[i] = bytes[i]|t;
        break;
      }
    }
    break;
  }
  
  next_free_page_=8*i+x;*/
  return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if(page_offset>=8*MAX_CHARS){
    return false;
  }
  uint32_t byteIndex = page_offset / 8;
  uint32_t bitIndex = page_offset % 8;
  unsigned char c;
  c = bytes[byteIndex] & (1 << (7-bitIndex));
  if(!c)
    return false;
  else
  {
    bytes[byteIndex] &= ~(1 << (7-bitIndex));
    page_allocated_--;
    //next_free_page_ = page_offset;
    return true;
  }
  
  //uint32_t i = page_offset/8;
  //uint32_t j = page_offset%8;
  //unsigned char c=128;

  //if((bytes[i]&(c>>j))==0){
   // return false;
  //}
  
  //c=c>>j;
  //c=~c;
  //bytes[i] = bytes[i]&c;
  //return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if(page_offset>=8*MAX_CHARS){
    return false;
  }
  uint32_t byteIndex = page_offset / 8;
  uint32_t bitIndex = page_offset % 8;
  unsigned char c;
  c = bytes[byteIndex] & (1 << (7-bitIndex));
  if(!c)
    return true;
  else
    return false;
  /*uint32_t i = page_offset/8;
  uint32_t j = page_offset%8;
  unsigned char c=128;
  if(i>=MAX_CHARS){
    return false;
  }
  c=c>>j;
  if((bytes[i] & c) != 0)
  return false;

  return true;*/
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return false;
}

template
class BitmapPage<64>;

template
class BitmapPage<128>;

template
class BitmapPage<256>;

template
class BitmapPage<512>;

template
class BitmapPage<1024>;

template
class BitmapPage<2048>;

template
class BitmapPage<4096>;