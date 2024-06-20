#include <algorithm>
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_leaf_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
  SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  // use binary search to speed up
  int begin = 0, end = GetSize()-1;
  while(begin < end)
  {
    int mid = (begin + end) / 2;
    if(comparator(array_[mid].first, key) < 0)
      begin = mid + 1;
    else
      end = mid;
  }
  return begin;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  assert(index >= 0 && index < GetSize());
  return array_[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  assert(index >= 0 && index < GetSize());
  return array_[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  // handling special cases
  if(GetSize() == 0 || comparator(key, array_[GetSize()-1].first) > 0)
    array_[GetSize()] = {key, value};
  else if(comparator(key, array_[0].first) < 0)
  {
    // NOTE: not memcopy() !! two sources have overlap
    // memmove(array_+1, array_, reinterpret_cast<size_t>(GetSize()*sizeof(MappingType)));
    for(int i=GetSize(); i>0; i--)
      array_[i] = array_[i-1];
    array_[0] = {key, value};
  }
  else
  {
    int pos = KeyIndex(key, comparator);
    // memmove(array_+pos+1, array_+pos, reinterpret_cast<size_t>((GetSize()-pos) * sizeof(MappingType)));
    for(int i=GetSize(); i>pos; i--)
      array_[i] = array_[i-1];
    array_[pos] = {key, value};
  }
  IncreaseSize(1);
  assert(GetSize() <= GetMaxSize());
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  int size = GetSize()/2;
  MappingType *src = array_ + GetSize() - size;
  recipient->CopyNFrom(src, size);
  IncreaseSize(-1*size);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  int current_size = GetSize();
  assert(current_size + size <= GetMaxSize());
  // memcpy(array_+current_size, items, size * sizeof(MappingType));
  for(int i=0; i<size; i++)
    array_[i+current_size] = items[i];
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value, const KeyComparator &comparator) const {
  if(GetSize()==0 || comparator(array_[0].first, key)>0 || comparator(array_[GetSize()-1].first, key)<0)
    return false;
  
  int pos = KeyIndex(key, comparator);
  if(comparator(array_[pos].first, key) == 0)
  {
    value = array_[pos].second;
    return true;
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  ValueType value;
  if(Lookup(key, value, comparator))
  {
    int pos = KeyIndex(key, comparator);
    // memmove(array_+pos-1, array_+pos, reinterpret_cast<size_t>((GetSize()-pos) * sizeof(MappingType)));
    for(int i=pos; i<GetSize()-1; i++)
      array_[i] = array_[i+1];
    IncreaseSize(-1);
  }
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  recipient->CopyNFrom(array_, GetSize());
  SetSize(0);
  recipient->SetNextPageId(GetNextPageId());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  // memmove(array_, array_+1, reinterpret_cast<size_t>(GetSize() * sizeof(MappingType)));
  recipient ->CopyLastFrom(array_[0]);
  for(int i=0; i<GetSize()-1; i++)
    array_[i] = array_[i+1];
  IncreaseSize(-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  assert(GetSize() < GetMaxSize());
  array_[GetSize()] = item;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  auto item = array_[GetSize()-1];
  IncreaseSize(-1);
  recipient->CopyFirstFrom(item);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  assert(GetSize() + 1 < GetMaxSize());
  // memmove(array_+1, array_, reinterpret_cast<size_t>(GetSize() * sizeof(MappingType)));
  IncreaseSize(1);
  for(int i=GetSize()-1; i>0; i--)
    array_[i] = array_[i-1];
  array_[0] = item;
}

template
class BPlusTreeLeafPage<int, int, BasicComparator<int>>;

template
class BPlusTreeLeafPage<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTreeLeafPage<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTreeLeafPage<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTreeLeafPage<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTreeLeafPage<GenericKey<64>, RowId, GenericComparator<64>>;