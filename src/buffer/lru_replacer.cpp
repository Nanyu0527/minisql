#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) {
  size=0;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(!lru_list_.empty()){
    int count=-1;
    *frame_id = lru_list_.back();
    for(auto t = lru_list_.begin();t != lru_list_.end();t++){
      if(*t==*frame_id){
        count++;
      }
      }
      if(count!=0){
        lru_list_.remove(*frame_id);
        size-=count;
    }
    lru_list_.remove(*frame_id);
    return true;
  }
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  int count=0;
  if(!lru_list_.empty()){
    for(auto t = lru_list_.begin();t != lru_list_.end();t++){
      if(*t==frame_id){
        count++;
      }
      }
      if(count!=0){
        lru_list_.remove(frame_id);
        size-=count;//减去重复的
        size++;
    }
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  int count=0;
  if(!lru_list_.empty()){
    for(auto t = lru_list_.begin();t != lru_list_.end();t++){
      if(*t==frame_id){
        count=1;
        break;
      }
    }
  }
  lru_list_.push_front(frame_id);
  size+=count;
}

size_t LRUReplacer::Size() {
  return lru_list_.size()-size;
}