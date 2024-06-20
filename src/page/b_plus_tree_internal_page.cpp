#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_internal_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  assert(index >=0 && index < GetMaxSize());
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  // note the first key is invalid!!!
  assert(index >=0 && index < GetMaxSize());
  array_[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  // the first value is valid!
  for(int i=0; i<GetSize(); i++)
  {
    if(array_[i].second == value)
      return i;
  }
  return GetSize();
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  assert(index >=0 && index < GetMaxSize());
  return array_[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  // if(comparator(key, array_[1].first) < 0)
  //   return array_[0].second;
  // else if(comparator(key, array_[GetSize()-1].first) > 0)
  //   return array_[GetSize()-1].second;
  // else
  // {
  //   // binary search
  //   int begin = 1, end = GetSize() - 1;
  //   while(begin <= end)
  //   {
  //     int mid = (begin + end) / 2;
  //     if(comparator(array_[mid].first, key) < 0)
  //       begin = mid + 1;
  //     else if(comparator(array_[mid].first, key) > 0)
  //       end = mid - 1;
  //     else
  //       return array_[mid].second;
  //   }
  //   // seems the most reasonable if not found
  //   return array_[begin].second;
  // }
  // // never used
  // return array_[GetSize()-1].second;
  
  // int size = GetSize();
  // if (size == 1 || comparator(key, array_[1].first) < 0){
  //   return array_[0].second;
  // }

  // int left = 1;
  // int right = size; // `right` is the first 'impossible' index
  // while (left < right - 1){
  //   int mid = (left + right) / 2;
  //   int compare_result = comparator(key, array_[mid].first);

  //   if (compare_result == -1){
  //     right = mid;
  //   }
  //   if (compare_result == 1){
  //     left = mid;
  //   }
  //   if (compare_result == 0){
  //     return array_[mid].second;
  //   }
  // }

  // return array_[left].second;
  int i;
  for(i=1; i<GetSize(); i++){
    if(comparator(array_[i].first, key) > 0)
      break;
  }
  return array_[i-1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  assert(GetSize() == 0);
  IncreaseSize(2);
  array_[0].second = old_value;
  array_[1].first = new_key;
  array_[1].second = new_value;
}

/**
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  // to avoid frequently call method GetSize(), may affect time
  int cur_size = GetSize();
  IncreaseSize(1);
  int value_index = ValueIndex(old_value);
  assert(value_index < cur_size);
  //memmove(array_+value_index+2, array_+value_index+1, (cur_size-value_index-1) * sizeof(MappingType));
  for(int i = cur_size; i> value_index + 1; i--)
    array_[i] = array_[i-1];
  array_[value_index+1] = {new_key, new_value};
  return cur_size+1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  int half_size = GetSize() / 2;
  MappingType *item = array_ + GetSize() - half_size;
  recipient->CopyNFrom(item, half_size, buffer_pool_manager);
  // reset parent id of its child of each moved pair of {type, value}

  IncreaseSize(-1*half_size);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  int cur_size = GetSize();
  IncreaseSize(size);
  // memcpy(array_+cur_size, items, size * sizeof(MappingType));
  for(int i=0; i<size; i++)
  {
    array_[i+cur_size] = items[i];
    Page *page_ = buffer_pool_manager->FetchPage(items[i].second);
    assert(page_ != nullptr);
    BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page_->GetData());
    node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(node->GetPageId(), true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  // memmove(array_+index, array_+index+1, (GetSize()-index-1) * sizeof(MappingType));
  assert(index >=0 && index <= GetSize());
  for(int i=index; i<GetSize()-1; i++)
    array_[i] = array_[i+1];
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  assert(GetSize() == 1);
  IncreaseSize(-1);
  return array_[0].second;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  SetKeyAt(0, middle_key);
  recipient->CopyNFrom(array_, GetSize(), buffer_pool_manager);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  recipient->CopyLastFrom({middle_key, array_[0].second}, buffer_pool_manager);
  Remove(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  IncreaseSize(1);
  array_[GetSize()-1] = pair;
  Page *child_page = buffer_pool_manager->FetchPage(pair.second);
  assert(child_page != nullptr);
  BPlusTreePage *child = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child->GetPageId(), true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  recipient->CopyFirstFrom({middle_key, array_[GetSize()-1].second}, buffer_pool_manager);
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  // memmove(array_+1, array_, static_cast<size_t>(GetSize() * sizeof(MappingType)));
  IncreaseSize(1);
  for(int i=GetSize()-1; i>0; i--)
    array_[i] = array_[i-1];
  array_[0] = pair;
  SetKeyAt(1, pair.first); // keyat(0) is not considered indeed!

  Page *child_page = buffer_pool_manager->FetchPage(pair.second);
  assert(child_page != nullptr);
  BPlusTreePage *child = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child->GetPageId(), true);
}

template
class BPlusTreeInternalPage<int, int, BasicComparator<int>>;

template
class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;

template
class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;

template
class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;

template
class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;

template
class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;