/*#include <cstring>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "page/table_page.h"
#include "record/field.h"
#include "record/row.h"
#include "record/schema.h"

char *chars[] = {
        const_cast<char *>(""),
        const_cast<char *>("hello"),
        const_cast<char *>("world!"),
        const_cast<char *>("\0")
};

Field int_fields[] = {
        Field(TypeId::kTypeInt, 188),
        Field(TypeId::kTypeInt, -65537),
        Field(TypeId::kTypeInt, 33389),
        Field(TypeId::kTypeInt, 0),
        Field(TypeId::kTypeInt, 999),
};
Field float_fields[] = {
        Field(TypeId::kTypeFloat, -2.33f),
        Field(TypeId::kTypeFloat, 19.99f),
        Field(TypeId::kTypeFloat, 999999.9995f),
        Field(TypeId::kTypeFloat, -77.7f),
};
Field char_fields[] = {
        Field(TypeId::kTypeChar, chars[0], strlen(chars[0]), false),
        Field(TypeId::kTypeChar, chars[1], strlen(chars[1]), false),
        Field(TypeId::kTypeChar, chars[2], strlen(chars[2]), false),
        Field(TypeId::kTypeChar, chars[3], 1, false)
};
Field null_fields[] = {
        Field(TypeId::kTypeInt), Field(TypeId::kTypeFloat), Field(TypeId::kTypeChar)
};

TEST(TupleTest, FieldSerializeDeserializeTest) {
  char buffer[PAGE_SIZE];
  memset(buffer, 0, sizeof(buffer));
  // Serialize phase
  char *p = buffer;
  MemHeap *heap = new SimpleMemHeap();
  for (int i = 0; i < 4; i++) {
    p += int_fields[i].SerializeTo(p);
  }
  for (int i = 0; i < 3; i++) {
    p += float_fields[i].SerializeTo(p);
  }
  for (int i = 0; i < 4; i++) {
    p += char_fields[i].SerializeTo(p);
  }
  // Deserialize phase
  uint32_t ofs = 0;
  Field *df = nullptr;
  for (int i = 0; i < 4; i++) {
    ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeInt, &df, false, heap);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(int_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(int_fields[4]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(int_fields[1]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(int_fields[2]));
    heap->Free(df);
    df = nullptr;
  }
  for (int i = 0; i < 3; i++) {
    ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeFloat, &df, false, heap);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(float_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(float_fields[3]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[1]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(float_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(float_fields[2]));
    heap->Free(df);
    df = nullptr;
  }
  for (int i = 0; i < 3; i++) {
    ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeChar, &df, false, heap);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(char_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(char_fields[3]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[2]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(char_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(char_fields[2]));
    heap->Free(df);
    df = nullptr;
  }
}

TEST(TupleTest, RowTest) {
  SimpleMemHeap heap;
  TablePage table_page;
  // create schema
  std::vector<Column *> columns = {
          ALLOC_COLUMN(heap)("id", TypeId::kTypeInt, 0, false, false),
          ALLOC_COLUMN(heap)("name", TypeId::kTypeChar, 64, 1, true, false),
          ALLOC_COLUMN(heap)("account", TypeId::kTypeFloat, 2, true, false)
  };
  std::vector<Field> fields = {
          Field(TypeId::kTypeInt, 188),
          Field(TypeId::kTypeChar, const_cast<char *>("minisql"), strlen("minisql"), false),
          Field(TypeId::kTypeFloat, 19.99f)
  };
  auto schema = std::make_shared<Schema>(columns);
  Row row(fields);
  table_page.Init(0, INVALID_PAGE_ID, nullptr, nullptr);
  table_page.InsertTuple(row, schema.get(), nullptr, nullptr, nullptr);
  RowId first_tuple_rid;
  ASSERT_TRUE(table_page.GetFirstTupleRid(&first_tuple_rid));
  ASSERT_EQ(row.GetRowId(), first_tuple_rid);
  Row row2(row.GetRowId());
  ASSERT_TRUE(table_page.GetTuple(&row2, schema.get(), nullptr, nullptr));
  std::vector<Field *> &row2_fields = row2.GetFields();
  ASSERT_EQ(3, row2_fields.size());
  for (size_t i = 0; i < row2_fields.size(); i++) {
    ASSERT_EQ(CmpBool::kTrue, row2_fields[i]->CompareEquals(fields[i]));
  }
  ASSERT_TRUE(table_page.MarkDelete(row.GetRowId(), nullptr, nullptr, nullptr));
  table_page.ApplyDelete(row.GetRowId(), nullptr, nullptr);
}
*/
#include <cstring>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "page/table_page.h"
#include "record/field.h"
#include "record/row.h"
#include "record/schema.h"

char *chars[] = {const_cast<char *>(""), const_cast<char *>("hello"), const_cast<char *>("world!"),
                 const_cast<char *>("\0")};

Field int_fields[] = {
    Field(TypeId::kTypeInt, 188), Field(TypeId::kTypeInt, -65537), Field(TypeId::kTypeInt, 33389),
    Field(TypeId::kTypeInt, 0),   Field(TypeId::kTypeInt, 999),
};
Field float_fields[] = {
    Field(TypeId::kTypeFloat, -2.33f),
    Field(TypeId::kTypeFloat, 19.99f),
    Field(TypeId::kTypeFloat, 999999.9995f),
    Field(TypeId::kTypeFloat, -77.7f),
};
Field char_fields[] = {Field(TypeId::kTypeChar, chars[0], strlen(chars[0]), false),
                       Field(TypeId::kTypeChar, chars[1], strlen(chars[1]), false),
                       Field(TypeId::kTypeChar, chars[2], strlen(chars[2]), false),
                       Field(TypeId::kTypeChar, chars[3], 1, false)};
Field null_fields[] = {Field(TypeId::kTypeInt), Field(TypeId::kTypeFloat), Field(TypeId::kTypeChar)};

TEST(TupleTest, FieldSerializeDeserializeTest) {
  char buffer[PAGE_SIZE];
  memset(buffer, 0, sizeof(buffer));
  // Serialize phase
  char *p = buffer;
  MemHeap *heap = new SimpleMemHeap();
  for (int i = 0; i < 4; i++) {
    p += int_fields[i].SerializeTo(p);
  }
  for (int i = 0; i < 3; i++) {
    p += float_fields[i].SerializeTo(p);
  }
  for (int i = 0; i < 4; i++) {
    p += char_fields[i].SerializeTo(p);
  }
  // Deserialize phase
  uint32_t ofs = 0;
  Field *df = nullptr;
  for (int i = 0; i < 4; i++) {
    ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeInt, &df, false, heap);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(int_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(int_fields[4]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(int_fields[1]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(int_fields[2]));
    heap->Free(df);
    df = nullptr;
  }
  for (int i = 0; i < 3; i++) {
    ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeFloat, &df, false, heap);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(float_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(float_fields[3]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[1]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(float_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(float_fields[2]));
    heap->Free(df);
    df = nullptr;
  }
  for (int i = 0; i < 3; i++) {
    ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeChar, &df, false, heap);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(char_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(char_fields[3]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[2]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(char_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(char_fields[2]));
    heap->Free(df);
    df = nullptr;
  }
}

TEST(TupleTest, RowTest) {
  SimpleMemHeap heap;
  TablePage table_page;
  // create schema
  std::vector<Column *> columns = {ALLOC_COLUMN(heap)("id", TypeId::kTypeInt, 0, false, false),
                                   ALLOC_COLUMN(heap)("name", TypeId::kTypeChar, 64, 1, true, false),
                                   ALLOC_COLUMN(heap)("account", TypeId::kTypeFloat, 2, true, false)};
  std::vector<Field> fields = {Field(TypeId::kTypeInt, 188),
                               Field(TypeId::kTypeChar, const_cast<char *>("minisql"), strlen("minisql"), false),
                               Field(TypeId::kTypeFloat, 19.99f)};
  auto schema = std::make_shared<Schema>(columns);
  Row row(fields);
  table_page.Init(0, INVALID_PAGE_ID, nullptr, nullptr);
  table_page.InsertTuple(row, schema.get(), nullptr, nullptr, nullptr);
  RowId first_tuple_rid;
  ASSERT_TRUE(table_page.GetFirstTupleRid(&first_tuple_rid));
  ASSERT_EQ(row.GetRowId(), first_tuple_rid);
  Row row2(row.GetRowId());
  ASSERT_TRUE(table_page.GetTuple(&row2, schema.get(), nullptr, nullptr));
  std::vector<Field *> &row2_fields = row2.GetFields();
  ASSERT_EQ(3, row2_fields.size());
  for (size_t i = 0; i < row2_fields.size(); i++) {
    ASSERT_EQ(CmpBool::kTrue, row2_fields[i]->CompareEquals(fields[i]));
  }
  ASSERT_TRUE(table_page.MarkDelete(row.GetRowId(), nullptr, nullptr, nullptr));
  table_page.ApplyDelete(row.GetRowId(), nullptr, nullptr);
}

TEST(TupleTest, ColTest) {
  SimpleMemHeap heap;
  TablePage table_page;
  char *space = new char[1000];
  char *buf = space;
  // create schema
  std::vector<Column *> columns = {ALLOC_COLUMN(heap)("id", TypeId::kTypeInt, 0, false, false),
                                   ALLOC_COLUMN(heap)("name", TypeId::kTypeChar, 64, 1, true, false),
                                   ALLOC_COLUMN(heap)("account", TypeId::kTypeFloat, 2, true, false)};
  std::vector<Field> fields = {Field(TypeId::kTypeInt, 188),
                               Field(TypeId::kTypeChar, const_cast<char *>("minisql"), strlen("minisql"), false),
                               Field(TypeId::kTypeFloat, 19.99f)};
  auto schema = std::make_shared<Schema>(columns);

  Column col0(columns[0]->GetName(), columns[0]->GetType(), columns[0]->GetTableInd(), columns[0]->IsNullable(), false);
  Column col1(columns[1]->GetName(), columns[1]->GetType(), columns[1]->GetLength(), columns[1]->GetTableInd(),
              columns[1]->IsNullable(), false);
  Column col2(columns[2]->GetName(), columns[2]->GetType(), columns[2]->GetTableInd(), columns[2]->IsNullable(), false);

  Column *test_col = nullptr;

  col0.SerializeTo(buf);
  buf += col0.GetSerializedSize();
  col1.SerializeTo(buf);
  buf += col1.GetSerializedSize();
  col2.SerializeTo(buf);
  buf += col2.GetSerializedSize();
  buf = space;
  Column::DeserializeFrom(buf, test_col, &heap);
  std::cout<<buf<<endl;
  buf += test_col->GetSerializedSize();
  ASSERT_TRUE(test_col->GetName() == col0.GetName());
  ASSERT_TRUE(test_col->GetType() == col0.GetType());

  Column::DeserializeFrom(buf, test_col, &heap);
  buf += test_col->GetSerializedSize();
  ASSERT_TRUE(test_col->GetName() == col1.GetName());
  ASSERT_TRUE(test_col->GetType() == col1.GetType());

  Column::DeserializeFrom(buf, test_col, &heap);
  buf += test_col->GetSerializedSize();
  ASSERT_TRUE(test_col->GetName() == col2.GetName());
  ASSERT_TRUE(test_col->GetType() == col2.GetType());

  delete[] space;
}
TEST(TupleTest, SchemaTest) {
  SimpleMemHeap heap;
  TablePage table_page;
  char *space = new char[1000];
  char *buf = space;
  // create schema
  std::vector<Column *> columns = {ALLOC_COLUMN(heap)("id", TypeId::kTypeInt, 0, false, false),
                                   ALLOC_COLUMN(heap)("name", TypeId::kTypeChar, 64, 1, true, false),
                                   ALLOC_COLUMN(heap)("account", TypeId::kTypeFloat, 2, true, false)};
  std::vector<Field> fields = {Field(TypeId::kTypeInt, 188),
                               Field(TypeId::kTypeChar, const_cast<char *>("minisql"), strlen("minisql"), false),
                               Field(TypeId::kTypeFloat, 19.99f)};
  auto schema = std::make_shared<Schema>(columns);

  Column col0(columns[0]->GetName(), columns[0]->GetType(), columns[0]->GetTableInd(), columns[0]->IsNullable(), false);
  Column col1(columns[1]->GetName(), columns[1]->GetType(), columns[1]->GetLength(), columns[1]->GetTableInd(),
              columns[1]->IsNullable(), false);
  Column col2(columns[2]->GetName(), columns[2]->GetType(), columns[2]->GetTableInd(), columns[2]->IsNullable(), false);

  Schema *test_Schema = nullptr;

   Schema schema1(columns);
   ASSERT_TRUE(schema1.SerializeTo(buf)==schema1.GetSerializedSize());
  buf += schema1.GetSerializedSize();
  buf = space;

  Schema::DeserializeFrom(buf, test_Schema, &heap);

  buf += test_Schema->GetSerializedSize();
  std::vector<Column *> columns_ = test_Schema->GetColumns();
  ASSERT_TRUE(columns_[0]->GetName() == col0.GetName());
  ASSERT_TRUE(columns_[0]->GetName() == col0.GetName());

  Schema::DeserializeFrom(buf, test_Schema, &heap);
  buf += test_Schema->GetSerializedSize();

  ASSERT_TRUE(columns_[1]->GetName() == col1.GetName());
  ASSERT_TRUE(columns_[1]->GetName() == col1.GetName());

  Schema::DeserializeFrom(buf, test_Schema, &heap);
  buf += test_Schema->GetSerializedSize();

  ASSERT_TRUE(columns_[2]->GetName() == col2.GetName());
  ASSERT_TRUE(columns_[2]->GetName() == col2.GetName());

  delete[] space;
}