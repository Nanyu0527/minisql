#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page: page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  Page *page = nullptr;
  frame_id_t frame_id = -1;
  for(auto pi=page_table_.begin(); pi!=page_table_.end(); pi++)
  {
    if(pi->first == page_id)
    {
      frame_id = pi->second;
      page = pages_ + frame_id;
      page->pin_count_++;
      replacer_->Pin(frame_id);
      return page;
    }
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  if(!free_list_.empty())
  {
    frame_id = free_list_.back();  // can also be front
    free_list_.pop_back();
    page = pages_ + frame_id;
  }
  else
  {
    bool victim_page = replacer_->Victim(&frame_id);
    if(!victim_page)
      return nullptr;
    page = pages_ + frame_id;

    if(page->IsDirty())
    {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->is_dirty_ = false;
    }
    page->pin_count_ = 0;
  }
  
  // 2.     If R is dirty, write it back to the disk.
  
  // 3.     Delete R from the page table and insert P.
  page_table_.erase(page->GetPageId());
  page_table_[page_id] = frame_id;
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  page->page_id_ = page_id;
  page->ResetMemory();  // Zeroes out the original data of old page
  disk_manager_->ReadPage(page_id, page->GetData());  // Read new data of new page
  page->pin_count_++;
  
  return page;
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  Page *page;
  frame_id_t frame_id = -1;
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  bool all_pinned = true;
  for(unsigned int i=0; i<pool_size_; i++)
  {
    if(pages_[i].GetPinCount() == 0)
    {
      all_pinned = false;
      break;
    }
  }
  if(all_pinned)
    return nullptr;
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  if(!free_list_.empty())
  {
    frame_id = free_list_.back();
    free_list_.pop_back();
    page = pages_ + frame_id;
  }
  else
  {
    bool victim_page = replacer_->Victim(&frame_id);
    if(!victim_page)
      return nullptr;
    page = pages_ + frame_id;
    page->pin_count_ = 0;
    if(page->IsDirty())
    {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
      page->is_dirty_ = false;
    }
  }
  page_id = AllocatePage();

  // 3.   Update P's metadata, zero out memory and add P to the page table.
  page_table_.erase(page->GetPageId());
  page_table_[page_id] = frame_id;
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->ResetMemory();
  // 4.   Set the page ID output parameter. Return a pointer to P.
  return page;
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  Page *page;
  frame_id_t frame_id = -1;
  // 1.   Search the page table for the requested page (P).
  bool exist = false;
  for(auto pi=page_table_.begin(); pi!=page_table_.end(); pi++)
  {
    if(pi->first == page_id)
    {
      exist = true;
      frame_id = pi->second;
      break;
    }
  }
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if(!exist)
  {
    return true;
  }
  else
  {
    page = pages_ + frame_id;
    if(!pages_[frame_id].GetPinCount())
      return false;
    
    DeallocatePage(page_id);
    page_table_.erase(page_id);
    page->page_id_ = INVALID_PAGE_ID;
    page->pin_count_ = 0;
    page->is_dirty_ = false;
    page->ResetMemory();
    
    free_list_.push_back(frame_id);
  }
  return true;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  auto pi=page_table_.find(page_id);
  if(pi == page_table_.end())
    return false;
  
  Page *page;
  frame_id_t frame_id = pi->second;
  page = pages_ + frame_id;

  if(is_dirty)
    page->is_dirty_ = true;
  
  if(page->GetPinCount() == 0)
    return false;
  page->pin_count_--;
  if(page->GetPinCount() == 0)
    replacer_->Unpin(frame_id);

  return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  auto pi=page_table_.find(page_id);
  if(pi == page_table_.end())
    return false;
  
  Page *page;
  frame_id_t frame_id = pi->second;
  page = pages_ + frame_id;

  disk_manager_->WritePage(page_id, page->GetData());
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}