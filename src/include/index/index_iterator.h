// #ifndef MINISQL_INDEX_ITERATOR_H
// #define MINISQL_INDEX_ITERATOR_H

// #include "page/b_plus_tree_leaf_page.h"

// #define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

// INDEX_TEMPLATE_ARGUMENTS
// class IndexIterator {
// public:
//   // you may define your own constructor based on your member variables
//   explicit IndexIterator();

//   ~IndexIterator();

//   /** Return the key/value pair this iterator is currently pointing at. */
//   const MappingType &operator*();

//   /** Move to the next key/value pair.*/
//   IndexIterator &operator++();

//   /** Return whether two iterators are equal */
//   bool operator==(const IndexIterator &itr) const;

//   /** Return whether two iterators are not equal. */
//   bool operator!=(const IndexIterator &itr) const;

// private:
//   // add your own private member variables here
// };


// #endif //MINISQL_INDEX_ITERATOR_H
#ifndef MINISQL_INDEX_ITERATOR_H
#define MINISQL_INDEX_ITERATOR_H

#include "page/b_plus_tree_leaf_page.h"

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  explicit IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *first_leaf, int32_t tuple_num, BufferPoolManager *bpm);

  ~IndexIterator();

  /** Return the key/value pair this iterator is currently pointing at. */
  const MappingType &operator*();

  /** Move to the next key/value pair.*/
  IndexIterator &operator++();

  /** Return whether two iterators are equal */
  bool operator==(const IndexIterator &itr) const;

  /** Return whether two iterators are not equal. */
  bool operator!=(const IndexIterator &itr) const;

private:
  B_PLUS_TREE_LEAF_PAGE_TYPE *current_page;
  int32_t current_tuple;
  BufferPoolManager *current_bpm;

  // add your own private member variables here
};


#endif //MINISQL_INDEX_ITERATOR_H