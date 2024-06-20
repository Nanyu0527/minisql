#include <string>
#include "glog/logging.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
        : index_id_(index_id),
          root_page_id_(INVALID_PAGE_ID),
          buffer_pool_manager_(buffer_pool_manager),
          comparator_(comparator),
          leaf_max_size_(leaf_max_size),
          internal_max_size_(internal_max_size) {

}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy() {
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> &result, Transaction *transaction) {
   cout << key << endl;
   if(IsEmpty())
     {cout << "is_empty" << endl; return false;}
  // Page *leaf_page = FindLeafPage(key, false);
  // LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  // ValueType value;

  // if(leaf_node->Lookup(key, value, comparator_))
  // {
  //   result.push_back(value);
  //   buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
  //   return true;
  // }
  // buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
  // return false;
  
  Page *page = FindLeafPage(key);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType value;
  ASSERT(leaf_page!=nullptr,"leaf_page is null!");
  bool ret = leaf_page->Lookup(key,value,comparator_);
  buffer_pool_manager_->UnpinPage(page->GetPageId(),false);//没有对该页进行修改，不是脏页
  result.push_back(value);
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if(IsEmpty())
  {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  Page* root_page = buffer_pool_manager_->NewPage(root_page_id_);
  // throw an exception
  if(root_page == nullptr)
    throw std::string("out of memery");
  LeafPage *root = reinterpret_cast<LeafPage *>(root_page->GetData());
  // update b+ tree's root page id
  UpdateRootPageId(1);
  root->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  root->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(root->GetPageId(), true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  Page *leaf_page = FindLeafPage(key, false);
  LeafPage *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  ValueType v;
  if(leaf->Lookup(key, v, comparator_))
  {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    return false;
  }

  if(leaf->GetSize() < leaf->GetMaxSize())  // have space
    leaf->Insert(key, value, comparator_);
  else  // split
  {
    LeafPage *new_leaf = Split(leaf);
    if(comparator_(key, new_leaf->KeyAt(0)) < 0)
      leaf->Insert(key, value, comparator_);
    else
      new_leaf->Insert(key, value, comparator_);
    InsertIntoParent(leaf, new_leaf->KeyAt(0), new_leaf, transaction);
    buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  /*page_id_t page_id;
  Page *new_page = buffer_pool_manager_->NewPage(page_id);
  // throw exception
  if(new_page == nullptr)
    throw std::string("out of memory");
  N *new_node;
  if(node->IsLeafPage())
  {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(node);
    new_node = reinterpret_cast<LeafPage *>(new_page->GetData());
    new_node->Init(page_id);
    node->MoveHalfTo(new_node);
    // update link list of leaf_node
    new_node->SetNextPageId(node->GetNextPageId());
    node->SetNextPageId(new_node->GetPageId());
  }
  else
  {
    new_node = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_node->Init(page_id);
    node->MoveHalfTo(new_node, buffer_pool_manager_);
  }

  return new_node;*/
  // 1, ask a new page
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(new_page_id);  // pinned
  if (new_page == nullptr) {
    throw std::string("out of memory");
  }
  N *new_node;
  if (node->IsLeafPage()) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *new_leaf_node = reinterpret_cast<LeafPage *>(new_page);
    new_leaf_node->Init(new_page_id, leaf_node->GetParentPageId(),leaf_max_size_);
    leaf_node->MoveHalfTo(new_leaf_node);
    // 叶子节点更新双向链表
    new_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
    leaf_node->SetNextPageId(new_leaf_node->GetPageId());
    new_node = reinterpret_cast<N *>(new_leaf_node);
  } else {
    // 内部节点不需要设置双向链表
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *new_internal_node = reinterpret_cast<InternalPage *>(new_page);
    new_internal_node->Init(new_page_id, internal_node->GetParentPageId(),internal_max_size_);
    internal_node->MoveHalfTo(new_internal_node,buffer_pool_manager_);
    new_node = reinterpret_cast<N *>(new_internal_node);
  }
  return new_node;
}

/**
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if(old_node->IsRootPage())
  {
    page_id_t new_root_page_id;
    Page *new_root_page = buffer_pool_manager_->NewPage(new_root_page_id);  //pin
    if(new_root_page == nullptr)
      throw std::string("out of memory");
    InternalPage *new_root = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    root_page_id_ = new_root_page_id;
    new_root->Init(root_page_id_, INVALID_PAGE_ID, 4);
    UpdateRootPageId(0);
    // update parent pointer and children pointers
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
  }
  else
  {
    page_id_t parent_id = old_node->GetParentPageId();
    Page *parent_page = buffer_pool_manager_->FetchPage(parent_id);  //pin
    InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
    new_node->SetParentPageId(parent_id);
    parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    if(parent->GetSize() > parent->GetMaxSize())
    {
      InternalPage *new_parent = Split<InternalPage>(parent);
      InsertIntoParent(parent, new_parent->KeyAt(0), new_parent, transaction);
      buffer_pool_manager_->UnpinPage(new_parent->GetPageId(), true);
    }
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  }
  // buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if(IsEmpty())
    return ;
  Page *leaf_page = FindLeafPage(key, false);
  assert(leaf_page != nullptr);
  LeafPage *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int size_after_del = leaf->RemoveAndDeleteRecord(key, comparator_);
  if(size_after_del < leaf->GetMinSize())
    CoalesceOrRedistribute(leaf, transaction);
  
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
}

/**
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if(node->IsRootPage())
  {
    return AdjustRoot(node);
  }
  // find the sibling of current node(default: left sibling)
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  assert(parent_page != nullptr);
  InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int node_index = parent->ValueIndex(node->GetPageId());
  int sibling_index = node_index ? (node_index - 1) : (node_index + 1);
  Page *sibling_page = buffer_pool_manager_->FetchPage(parent->ValueAt(sibling_index));
  N *sibling = reinterpret_cast<N *>(sibling_page->GetData());
  // now sibling is sibling page of node

  if(node->GetSize() + sibling->GetSize() <= node->GetMaxSize())
  {
    if(sibling_index > node_index)
      std::swap(node, sibling);
    int middle_index = parent->ValueIndex(node->GetPageId());
    Coalesce(&sibling, &node, &parent, middle_index, transaction);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
    return true;
  }
  
  int middle_index = parent->ValueIndex(node->GetPageId());
  Redistribute(sibling, node, middle_index);
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
  return false;
}

/**
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  if((*node)->IsLeafPage())
  {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(*node);
    LeafPage *leaf_neighbor_node = reinterpret_cast<LeafPage *>(*neighbor_node);
    leaf_node->MoveAllTo(leaf_neighbor_node);
  }
  else
  {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(*node);
    InternalPage *internal_neighbor_node = reinterpret_cast<InternalPage *>(*neighbor_node);
    internal_node->MoveAllTo(internal_neighbor_node, (*parent)->KeyAt(index), buffer_pool_manager_);
  }
  
  buffer_pool_manager_->UnpinPage((*node)->GetPageId(), true);
  buffer_pool_manager_->DeletePage((*node)->GetPageId());
  (*parent)->Remove(index);
  // recursively
  if((*parent)->GetSize() < (*parent)->GetMinSize()) // note <= here! implementation of GetMinSize()?
    return CoalesceOrRedistribute(*parent, transaction);
  return false;
}

/**
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  if(!node->IsLeafPage())
  {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *internal_neighbor_node = reinterpret_cast<InternalPage *>(neighbor_node);
    if(index == 0)
    {
      int node_index = parent->ValueIndex(internal_neighbor_node->GetPageId());
      KeyType next_middle_key = internal_neighbor_node->KeyAt(1);
      internal_neighbor_node->MoveFirstToEndOf(internal_node, parent->KeyAt(index+1), buffer_pool_manager_);
      parent->SetKeyAt(node_index, next_middle_key);
    }
    else
    {
      int node_index = parent->ValueIndex(internal_node->GetPageId());
      KeyType next_middle_key = internal_neighbor_node->KeyAt(internal_neighbor_node->GetSize() - 1);
      internal_neighbor_node->MoveLastToFrontOf(internal_node, parent->KeyAt(index), buffer_pool_manager_);
      parent->SetKeyAt(node_index, next_middle_key);
    }
  }
  else
  {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *leaf_neighbor_node = reinterpret_cast<LeafPage *>(neighbor_node);
    if(index == 0)
    {
      leaf_neighbor_node->MoveFirstToEndOf(leaf_node);
      int node_index = parent->ValueIndex(leaf_neighbor_node->GetPageId());
      parent->SetKeyAt(node_index, leaf_neighbor_node->KeyAt(0));
    }
    else
    {
      leaf_neighbor_node->MoveLastToFrontOf(leaf_node);
      int node_index = parent->ValueIndex(leaf_node->GetPageId());
      parent->SetKeyAt(node_index, leaf_node->KeyAt(0));
    }
  }
}

/**
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  // case 2
  if(old_root_node->IsLeafPage())
  {
    assert(old_root_node->GetSize() == 0);
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  }
  else if(old_root_node->GetSize() == 1)
  {
    InternalPage *old_root = reinterpret_cast<InternalPage *>(old_root_node);
    root_page_id_ = old_root->RemoveAndReturnOnlyChild();
    UpdateRootPageId(0);
    Page *new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    assert(new_root_page != nullptr);
    InternalPage *new_root = reinterpret_cast<InternalPage *>(new_root_page->GetData());

    new_root->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
  }
  return false;  // return value ?
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  Page *page = FindLeafPage(KeyType{}, true);
  LeafPage *left_leaf = reinterpret_cast<LeafPage *>(page->GetData());
  buffer_pool_manager_->UnpinPage(left_leaf->GetPageId(),false);
  
  return INDEXITERATOR_TYPE(left_leaf, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page* page = FindLeafPage(key, false);
  LeafPage* leaf = reinterpret_cast<LeafPage *>(page->GetData());

  int index = leaf->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(leaf, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
  Page *page = FindLeafPage(KeyType{}, true);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  while(leaf_page->GetNextPageId() != INVALID_PAGE_ID)
  {
    page_id_t next_page_id = leaf_page->GetNextPageId();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),false);
    Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
    leaf_page = reinterpret_cast<LeafPage *>(next_page->GetData());
  }
  return INDEXITERATOR_TYPE(leaf_page, leaf_page->GetSize(), buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  if(IsEmpty())
    return nullptr;
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);  //pin
  assert(page != nullptr);
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  // search
  while(!node->IsLeafPage())
  {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(node);
    page_id_t child_page_id;
    child_page_id = leftMost ? internal_node->ValueAt(0) : internal_node->Lookup(key, comparator_);

    Page *child_page = buffer_pool_manager_->FetchPage(child_page_id);  //pin
    BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    page = child_page;
    node = child_node;
    buffer_pool_manager_->UnpinPage(child_page_id, false);
  }
  // Unpin
  buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  assert(page != nullptr);
  IndexRootsPage *header_page = reinterpret_cast<IndexRootsPage *>(page->GetData());
  if(insert_record)
    header_page->Insert(index_id_, root_page_id_);
  else
    header_page->Update(index_id_, root_page_id_);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId()
          << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> "
          << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
              << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}

template
class BPlusTree<int, int, BasicComparator<int>>;

template
class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;
