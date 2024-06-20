#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"


class TableHeap;

class TableIterator {

public:
  // you may define your own constructor based on your member variables
  explicit TableIterator(TableHeap *table_heap, RowId rid, Transaction *txn);

  explicit TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  inline bool operator==(const TableIterator &itr) const;

  inline bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator++();

  TableIterator operator++(int);

private:
  TableHeap *table_heap;
  Row *row;
  Transaction *txn;
};

#endif //MINISQL_TABLE_ITERATOR_H
