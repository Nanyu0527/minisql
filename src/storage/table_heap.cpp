#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  uint32_t row_size = row.GetSerializedSize(schema_);
  if(row_size >= PAGE_SIZE) {
    std::cout << row_size << std::endl;
    return false;
  }
  auto cur_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  if (cur_page == nullptr) {
    return false;
  }

  while (!cur_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_))
  {
    page_id_t next_page_id = cur_page->GetNextPageId();
    if(next_page_id != INVALID_PAGE_ID) {
      buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
      cur_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
    }
    else {
      auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
      if(new_page == nullptr) {
        buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
        return false;
      }
      cur_page->SetNextPageId(next_page_id);
      new_page->Init(next_page_id, cur_page->GetNextPageId(), log_manager_, txn);
      buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), true);
      cur_page = new_page;
    }
  }
  buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), true);
  //RowId rid = row.GetRowId();
  //row.SetRowId(rid);
  return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if(page == nullptr)
  {
    return false;
  }
  Row orow(rid);
  int update_status = page->UpdateTuple(row, &orow, schema_, txn, lock_manager_, log_manager_);
  Row row1 = row;
  if(update_status == UPDATE_FAIL)
    update_status = false;
  else if(update_status == NO_SPACE)
  {
    if (page->MarkDelete(rid, txn, lock_manager_, log_manager_))
    {
      page->ApplyDelete(rid, txn, log_manager_);
      if(this->InsertTuple(row1, txn))
        update_status = true;
      else
        update_status = false;
    }
    else
      update_status = false;
  }
  else
    update_status = true;

  buffer_pool_manager_->UnpinPage(page->GetPageId(), update_status);
  return update_status;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // Step2: Delete the tuple from the page.
  assert(page != nullptr);
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  do
  {
    buffer_pool_manager_->DeletePage(page->GetPageId());
    //buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));
  } while (page->GetPageId() != INVALID_PAGE_ID);
  
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  RowId rid = row->GetRowId();
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    //txn->SetState(TransactionState::ABORTED);
    return false;
  }
  bool res = page->GetTuple(row, schema_, txn, lock_manager_);
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return res;
}

TableIterator TableHeap::Begin(Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  RowId rid;
  page->GetFirstTupleRid(&rid);
  buffer_pool_manager_->UnpinPage(first_page_id_, false);
  return TableIterator(this, rid, txn);
}

TableIterator TableHeap::End() {
  return TableIterator(this, RowId(INVALID_PAGE_ID, -1), nullptr);
}
