#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Transaction *txn) 
  :table_heap(table_heap), row(new Row(rid)), txn(txn)
{
  if(rid.GetPageId() != INVALID_PAGE_ID) {
    table_heap->GetTuple(row, txn);
  }
}

TableIterator::TableIterator(const TableIterator &other)
  :table_heap(other.table_heap), row(other.row), txn(other.txn)
{

}

TableIterator::~TableIterator() {

}

bool TableIterator::operator==(const TableIterator &itr) const {
  return false;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return false;
}

const Row &TableIterator::operator*() {
  return *row;
}

Row *TableIterator::operator->() {
  return row;
}

TableIterator &TableIterator::operator++() {
  BufferPoolManager *buffer_pool_manager_ = table_heap->buffer_pool_manager_;
  auto cur_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  RowId next_rid;
  if(!cur_page->GetNextTupleRid(row->GetRowId(), &next_rid))
  {
    while(cur_page->GetNextPageId() != INVALID_PAGE_ID)
    {
      auto next_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(cur_page->GetNextPageId()));
      buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
      cur_page = next_page;
      if(cur_page->GetFirstTupleRid(&next_rid))
        break;
    }
  }
  row->SetRowId(next_rid);
  if(*this != table_heap->End())
  {
    table_heap->GetTuple(row, txn);
  }

  buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
  return *this;
}

TableIterator TableIterator::operator++(int) {
  TableIterator clone(*this);
  ++*this;
  return TableIterator(*this);
}
