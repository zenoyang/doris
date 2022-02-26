// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <parallel_hashmap/phmap.h>

#include "gutil/hash/string_hash.h"
#include "runtime/string_value.h"
#include "olap/decimal12.h"
#include "olap/uint24.h"
#include "vec/columns/column.h"
#include "vec/columns/column_impl.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/predicate_column.h"
#include "vec/core/types.h"
#include "util/slice.h"

namespace doris::vectorized {

/**
 * used to keep column with dict in storage layer
 * todo(zeno) desc
 */
template <typename T>
class ColumnDictionary final : public COWHelper<IColumn, ColumnDictionary<T>> {
private:
    friend class COWHelper<IColumn, ColumnDictionary>;

    ColumnDictionary() {}
    ColumnDictionary(const size_t n) : indices(n) {}
    ColumnDictionary(const ColumnDictionary& src) : indices(src.indices.begin(), src.indices.end()) {}

public:
    using Self = ColumnDictionary;
    using value_type = T;
    using Container = PaddedPODArray<value_type>;
    using DictContainer = PaddedPODArray<StringValue>;

    bool is_numeric() const override { return false; }

    bool is_predicate_column() const override { return false; }

    bool is_column_dictionary() const override { return true; }

    size_t size() const override {
        return indices.size();
    }

    [[noreturn]] StringRef get_data_at(size_t n) const override {
        LOG(FATAL) << "get_data_at not supported in ColumnDictionary";
    }

    void insert_from(const IColumn& src, size_t n) override {
        LOG(FATAL) << "insert_from not supported in ColumnDictionary";
    }

    void insert_range_from(const IColumn& src, size_t start, size_t length) override {
        LOG(FATAL) << "insert_range_from not supported in ColumnDictionary";
    }

    void insert_indices_from(const IColumn& src, const int* indices_begin, const int* indices_end) override {
        LOG(FATAL) << "insert_indices_from not supported in ColumnDictionary";
    }

    void pop_back(size_t n) override {
        LOG(FATAL) << "pop_back not supported in ColumnDictionary";
    }

    void update_hash_with_value(size_t n, SipHash& hash) const override {
        LOG(FATAL) << "update_hash_with_value not supported in ColumnDictionary";
    }

    void insert_data(const char* pos, size_t /*length*/) override {
        indices.push_back(unaligned_load<T>(pos));
    }

    void insert_data(const T value) { indices.push_back(value); }

    void insert_default() override {
        // todo(zeno) impl default
        LOG(FATAL) << "insert_default not supported in ColumnDictionary";
    }

    void clear() override {
        // todo(zeno) log clean
//        LOG(INFO) << "[zeno] ColumnDictionary::clear ... indices size: " << indices.size();
        indices.clear();

        // todo(zeno) need clear dict ?
//        dict.clear();
//        dict_inited = false;
    }

    size_t byte_size() const override {
        //         return data.size() * sizeof(T);
        // todo(zeno)
        return -1;
    }

    size_t allocated_bytes() const override {
        //        return byte_size();
        // todo(zeno)
        return -1;
    }

    void protect() override {}

    void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                         IColumn::Permutation& res) const override {
        LOG(FATAL) << "get_permutation not supported in ColumnDictionary";
    }

    void reserve(size_t n) override {
        indices.reserve(n);
    }

    [[noreturn]] const char* get_family_name() const override {
        LOG(FATAL) << "get_family_name not supported in ColumnDictionary";
    }

    [[noreturn]] MutableColumnPtr clone_resized(size_t size) const override {
        LOG(FATAL) << "clone_resized not supported in ColumnDictionary";
    }

    void insert(const Field& x) override {
        LOG(FATAL) << "insert not supported in ColumnDictionary";
    }

    [[noreturn]] Field operator[](size_t n) const override {
        LOG(FATAL) << "operator[] not supported in ColumnDictionary";
    }

    void get(size_t n, Field& res) const override {
        LOG(FATAL) << "get field not supported in ColumnDictionary";
    }

    [[noreturn]] UInt64 get64(size_t n) const override {
        LOG(FATAL) << "get field not supported in ColumnDictionary";
    }

    [[noreturn]] Float64 get_float64(size_t n) const override {
        LOG(FATAL) << "get field not supported in ColumnDictionary";
    }

    [[noreturn]] UInt64 get_uint(size_t n) const override {
        LOG(FATAL) << "get field not supported in ColumnDictionary";
    }

    [[noreturn]] bool get_bool(size_t n) const override {
        LOG(FATAL) << "get field not supported in ColumnDictionary";
    }

    [[noreturn]] Int64 get_int(size_t n) const override {
        LOG(FATAL) << "get field not supported in ColumnDictionary";
    }

    // it's impossable to use ComplexType as key , so we don't have to implemnt them
    [[noreturn]] StringRef serialize_value_into_arena(size_t n, Arena& arena,
                                                      char const*& begin) const {
        LOG(FATAL) << "serialize_value_into_arena not supported in ColumnDictionary";
    }

    [[noreturn]] const char* deserialize_and_insert_from_arena(const char* pos) {
        LOG(FATAL) << "deserialize_and_insert_from_arena not supported in ColumnDictionary";
    }

    [[noreturn]] int compare_at(size_t n, size_t m, const IColumn& rhs,
                                int nan_direction_hint) const {
        LOG(FATAL) << "compare_at not supported in ColumnDictionary";
    }

    void get_extremes(Field& min, Field& max) const {
        LOG(FATAL) << "get_extremes not supported in ColumnDictionary";
    }

    bool can_be_inside_nullable() const override { return true; }

    bool is_fixed_and_contiguous() const override { return true; }

    size_t size_of_value_if_fixed() const override { return sizeof(T); }

    [[noreturn]] StringRef get_raw_data() const override {
        LOG(FATAL) << "get_raw_data not supported in ColumnDictionary";
    }

    [[noreturn]] bool structure_equals(const IColumn& rhs) const override {
        LOG(FATAL) << "structure_equals not supported in ColumnDictionary";
    }

    [[noreturn]] ColumnPtr filter(const IColumn::Filter& filt, ssize_t result_size_hint) const override {
        LOG(FATAL) << "filter not supported in ColumnDictionary";
    };

    [[noreturn]] ColumnPtr permute(const IColumn::Permutation& perm, size_t limit) const override {
        LOG(FATAL) << "permute not supported in ColumnDictionary";
    };

    Container& get_data() {
        return indices;
    }

    const Container& get_data() const {
        return indices;
    }

    [[noreturn]] ColumnPtr replicate(const IColumn::Offsets& replicate_offsets) const override {
        LOG(FATAL) << "replicate not supported in ColumnDictionary";
    };

    [[noreturn]] MutableColumns scatter(IColumn::ColumnIndex num_columns,
                                        const IColumn::Selector& selector) const override {
        LOG(FATAL) << "scatter not supported in ColumnDictionary";
    }

    Status filter_by_selector(const uint16_t* sel, size_t sel_size, IColumn* col_ptr) override {
        // todo(zeno) log clean
//        LOG(INFO) << "[zeno] ColumnDictionary::filter_by_selector sel_size " << sel_size;
        auto* res_col = reinterpret_cast<vectorized::ColumnString*>(col_ptr);
        for (size_t i = 0; i < sel_size; i++) {
            uint16_t n = sel[i];
            auto& index = reinterpret_cast<T&>(indices[n]);
            auto* word = dict.get_value(index);
            res_col->insert_data(word->ptr, word->len);
        }
        return Status::OK();
    }

    void replace_column_data(const IColumn&, size_t row, size_t self_row = 0) override {
        LOG(FATAL) << "should not call replace_column_data in ColumnDictionary";
    }

    void replace_column_data_default(size_t self_row = 0) override {
        LOG(FATAL) << "should not call replace_column_data_default in ColumnDictionary";
    }

    void insert_many_dict_data(const int32_t* data_array, size_t start_index, const uint32_t* start_offset_array,
                               const uint32_t* len_array, char* dict_data, size_t data_num, uint32_t dict_num) override {
        if (!is_dict_inited()) {
            // todo(zeno) log clean
//            LOG(INFO) << "[zeno] ColumnDictionary::insert_many_dict_data init_dict, dict_num: " << dict_num;
            dict.reserve(dict_num);
            for (uint32_t i = 0; i < dict_num; ++i) {
                uint32_t start_offset = start_offset_array[i];
                uint32_t str_len = len_array[i];
                auto sv = StringValue(dict_data + start_offset, str_len);
                dict.insert_value(&sv);
            }
            dict_inited = true;

            // todo(zeno) sort test
            if (!is_dict_sorted()) {
                sort();
            }
        }
//        else {    // todo(zeno) for debug
//            LOG(INFO) << "[zeno] ColumnDictionary::insert_many_dict_data not need init_dict";
//        }

        // todo(zeno) log clean
//        LOG(INFO) << "[zeno] ColumnDictionary::insert_many_dict_data insert_data data_num: " << data_num;
        for (int i = 0; i < data_num; i++, start_index++) {
            int32_t codeword = data_array[start_index];
            insert_data(codeword);
        }
    }

    bool is_dict_inited() const { return dict_inited; }

    bool is_dict_sorted() const { return dict_sorted; }

    ColumnPtr convert_to_predicate_column() {
        // todo(zeno) log clean
        LOG(INFO) << "[zeno] ColumnDictionary::convert_to_predicate_column indices.size: " << indices.size();
        auto res = vectorized::PredicateColumnType<StringValue>::create();
        for (size_t i = 0; i < indices.size(); ++i) {
            auto& index = reinterpret_cast<T&>(indices[i]);
            auto* word = dict.get_value(index);
            res->insert_data(word->ptr, word->len);
        }
        // todo(zeno) clear dict col?
        dict.clear();
        return res;
    }

    T get_index(const StringValue& word) const {
        return dict.get_index(word);
    }

    void sort() {
        // todo(zeno) log clean
        LOG(INFO) << "[zeno] Dictionary::sort";
        DictContainer new_dict;
        dict.replicate(new_dict);
        StringValue::Comparator comparator;
        std::sort(new_dict.begin(), new_dict.end(), comparator);
        dict.update_inverted_index(new_dict);
        for (size_t i = 0; i < size(); ++i) {
            StringValue* value = dict.get_value(i);
            T index = dict.get_index(*value);
            indices[i] = index;
        }
        dict_sorted = true;
    }

    class Dictionary {
    public:
        Dictionary() = default;

        void reserve(size_t n) {
            // todo(zeno) log clean
//            LOG(INFO) << "[zeno] Dictionary::reserve n: " << n;
            dict_data.reserve(n);
            inverted_index.reserve(n);
        }

        inline void insert_value(StringValue* word) {
            // todo(zeno) log clean
//            LOG(INFO) << "[zeno] Dictionary::insert_value word: " << word->to_string();
            dict_data.push_back_without_reserve(*word);
//            inverted_index[*word] = inverted_index.size();
            inverted_index.emplace(*word, inverted_index.size());
            // todo(zeno) log clean
//            LOG(INFO) << "[zeno] Dictionary::insert_value size: " << dict_data.size() << " " << inverted_index.size();
        }

        inline T get_index(const StringValue& word) const {
            // todo(zeno) log clean
//            LOG(INFO) << "[zeno] Dictionary::get_index word: " << word.to_string();
            auto it = inverted_index.find(word);
            if (it != inverted_index.end()) {
                return it->second;
            }
            return -1;  // todo(zeno)
        }

        inline StringValue* get_value(T code) {
            // todo(zeno) log clean
//            LOG(INFO) << "[zeno] Dictionary::get_value code: " << code;
            return &dict_data[code];
        }

        void clear() {
            // todo(zeno) log clean
            LOG(INFO) << "[zeno] Dictionary::clear, size: " << dict_data.size() << " " << inverted_index.size();
            dict_data.clear();
            inverted_index.clear();
        }

        void update_inverted_index(DictContainer& new_dict) {
            // todo(zeno) log clean
            LOG(INFO) << "[zeno] Dictionary::update_inverted_index";
            for (size_t i = 0; i < new_dict.size(); ++i) {
                inverted_index.emplace(new_dict[i], (T)i);
            }
        }

//        void sort() {
//            size_t dict_size = dict_data.size();
//
//            // copy dict to new_dict
//            DictContainer new_dict;
//            new_dict.reserve(dict_size);
//            for (size_t i = 0; i < dict_size; ++i) {
//                new_dict.push_back_without_reserve(dict_data[i]);
//            }
//
//            // sort new dict
//            std::sort(new_dict.begin(), new_dict.end(), comparator);
//
//            // update inverted_index;
//            for (size_t i = 0; i < dict_size; ++i) {
//                inverted_index.emplace(new_dict[i], (T)i);
//            }
//
//            // update index
//        }

        void replicate(DictContainer& new_dict) {
            // todo(zeno) log clean
            LOG(INFO) << "[zeno] Dictionary::replicate";
            size_t size = dict_data.size();
            new_dict.reserve(size);
            for (size_t i = 0; i < size; ++i) {
                new_dict.push_back_without_reserve(dict_data[i]);
            }
        }

    private:
        struct HashOfStringValue {
            size_t operator()(const StringValue& value) const {
                return HashStringThoroughly(value.ptr, value.len);
            }
        };

        DictContainer dict_data;
        phmap::flat_hash_map<StringValue, T, HashOfStringValue> inverted_index;
    };

private:
    bool dict_inited = false;
    bool dict_sorted = false;
    Dictionary dict;
    Container indices;
};

} // namespace