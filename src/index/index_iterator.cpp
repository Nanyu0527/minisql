#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "index/index_iterator.h"


INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *first_leaf, int32_t tuple_num, BufferPoolManager *bpm)  : current_page(first_leaf), current_tuple(tuple_num), current_bpm(bpm){

}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {

}

INDEX_TEMPLATE_ARGUMENTS const MappingType &INDEXITERATOR_TYPE::operator*() {
  return current_page->GetItem(current_tuple);
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  current_tuple++;
  if(current_tuple == current_page->GetSize() && current_page->GetNextPageId() != INVALID_PAGE_ID)
  {
    page_id_t next_page_id = current_page->GetNextPageId();
    Page *next_page = current_bpm->FetchPage(next_page_id);
    B_PLUS_TREE_LEAF_PAGE_TYPE *next_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(next_page->GetData());
    current_page = next_node;
    current_bpm->UnpinPage(next_page->GetPageId(), false);
    current_tuple = 0;
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  return (itr.current_page == current_page && itr.current_tuple == current_tuple);
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
  return (itr == *this) ? false : true;
}

template
class IndexIterator<int, int, BasicComparator<int>>;

template
class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

template
class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

template
class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

template
class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

template
class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;