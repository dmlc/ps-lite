#pragma once
#include "base/shared_array_inl.h"
#include "base/sparse_matrix.h"
#include "base/parallel_sort.h"
#include "data/slot_reader.h"
#include "base/crc32c.h"
#include <limits>
namespace ps {

/**
 * @brief Mapping a sparse matrix with general indices into continuous indices
 * starting from 0
 */
template<typename I, typename V>
class Localizer {
 public:
  Localizer() { }
  ~Localizer() { }
  /**
   * @brief count unique items
   *
   * temporal results will be stored to accelerate RemapIndex().
   *
   * @param idx the item list in any order
   * @param uniq_idx returns the sorted unique items
   * @param idx_frq returns the according occurrence counts
   */
  template<typename C>
  void CountUniqIndex(
      const SArray<I>& idx, SArray<I>* uniq_idx, SArray<C>* idx_frq);

  /**
   * @brief count unique items in mat->index()
   *
   * @param mat should be a sparse matrix whose index has type I
   * @param uniq_idx
   * @param idx_frq
   */
  template<typename C>
  void CountUniqIndex(
      const MatrixPtr<V>& mat, SArray<I>* uniq_idx, SArray<C>* idx_frq) {
    mat_ = std::static_pointer_cast<SparseMatrix<I,V>>(mat);
    CountUniqIndex(mat_->index(), uniq_idx, idx_frq);
  }

  /**
   * @brief Remaps the index.
   *
   * @param grp_id slot id
   * @param idx_dict the index dictionary. Any index does not exists in this
   * dictionary is dropped.
   * @param reader slot reader
   *
   * @return a matrix with index mapped: idx_dict[i] -> i.
   */
  MatrixPtr<V> RemapIndex(
      int grp_id, const SArray<I>& idx_dict, SlotReader* reader) const;

  /**
   * @brief Remaps the index. It's valid only if called CountUniqIndex(const
  MatrixPtr<V>&) before.
   *
   * @param idx_dict the index dictionary. Any index does not exists in this
   * dictionary is dropped.
   *
   * @return a matrix with index mapped: idx_dict[i] -> i.
   */
  MatrixPtr<V> RemapIndex(const SArray<I>& idx_dict);

  /**
   * @brief Clears the temporal results
   */
  void Clear() { pair_.clear(); }

  size_t MemSize() {
    return pair_.size() * sizeof(Pair) + (mat_ == nullptr ? 0 : mat_->memSize());
  }
 private:
  MatrixPtr<V> RemapIndex(
      const MatrixInfo& info, const SArray<size_t>& offset,
      const SArray<I>& index, const SArray<V>& value,
      const SArray<I>& idx_dict) const;

#pragma pack(push)
#pragma pack(4)
  struct Pair {
    I k; uint32 i;
  };
#pragma pack(pop)
  SArray<Pair> pair_;
  SparseMatrixPtr<I,V> mat_;
};



template<typename I, typename V>
template<typename C>
void Localizer<I,V>::CountUniqIndex(
    const SArray<I>& idx, SArray<I>* uniq_idx, SArray<C>* idx_frq) {
  if (idx.empty()) return;
  CHECK(uniq_idx);
  CHECK_LT(idx.size(), kuint32max)
      << "well, you need to change Pair.i from uint32 to uint64";
  CHECK_GT(FLAGS_num_threads, 0);

  pair_.resize(idx.size());
  for (size_t i = 0; i < idx.size(); ++i) {
    pair_[i].k = idx[i];
    pair_[i].i = i;
  }
  ParallelSort(&pair_, FLAGS_num_threads,
               [](const Pair& a, const Pair& b) {return a.k < b.k; });

  uniq_idx->clear();
  if (idx_frq) idx_frq->clear();

  uint32 cnt_max = static_cast<uint32>(std::numeric_limits<C>::max());
  I curr = pair_[0].k;
  uint32 cnt = 0;
  for (const Pair& v : pair_) {
    if (v.k != curr) {
      uniq_idx->push_back(curr);
      curr = v.k;
      if (idx_frq) {
        C c_cnt = static_cast<C>(std::min(cnt, cnt_max));
        idx_frq->push_back(c_cnt);
      }
      cnt = 0;
    }
    ++ cnt;
  }

  uniq_idx->push_back(curr);
  if (idx_frq) {
    C c_cnt = static_cast<C>(std::min(cnt, cnt_max));
    idx_frq->push_back(c_cnt);
  }
}

template<typename I, typename V>
MatrixPtr<V> Localizer<I,V>::RemapIndex(const SArray<I>& idx_dict) {
  CHECK(mat_);
  return RemapIndex(mat_->info(), mat_->offset(), mat_->index(), mat_->value(), idx_dict);
}

template<typename I, typename V>
MatrixPtr<V> Localizer<I, V>::RemapIndex(
    int grp_id, const SArray<I>& idx_dict, SlotReader* reader) const {
  SArray<V> val;
  auto info = reader->info<V>(grp_id);
  if (info.type() == MatrixInfo::SPARSE) val = reader->value<V>(grp_id);
  return RemapIndex(info, reader->offset(grp_id), reader->index(grp_id), val, idx_dict);
}

template<typename I, typename V>
MatrixPtr<V> Localizer<I, V>::RemapIndex(
    const MatrixInfo& info, const SArray<size_t>& offset,
    const SArray<I>& index, const SArray<V>& value,
    const SArray<I>& idx_dict) const {
  // LL << index << "\n" << idx_dict;
  if (index.empty() || idx_dict.empty()) return MatrixPtr<V>();
  CHECK_NE(info.type(), MatrixInfo::DENSE)
      << "dense matrix already have compact indeces\n" << info.DebugString();

  CHECK_LT(idx_dict.size(), kuint32max);
  CHECK_EQ(offset.back(), index.size());
  CHECK_EQ(index.size(), pair_.size());
  bool bin = value.empty();
  if (!bin) CHECK_EQ(value.size(), index.size());

  // TODO multi-thread
  uint32 matched = 0;
  SArray<uint32> remapped_idx(pair_.size(), 0);
  const I* cur_dict = idx_dict.begin();
  const Pair* cur_pair = pair_.begin();
  while (cur_dict != idx_dict.end() && cur_pair != pair_.end()) {
    if (*cur_dict < cur_pair->k) {
      ++ cur_dict;
    } else {
      if (*cur_dict == cur_pair->k) {
        remapped_idx[cur_pair->i] = (uint32)(cur_dict-idx_dict.begin()) + 1;
        ++ matched;
      }
      ++ cur_pair;
    }
  }

  // LL << crc32c::Value((char*)idx_dict.data(), idx_dict.size()*sizeof(V));
  // LL << matched << " " << index.size() << " " << pair_.size() << " " << idx_dict.size();

  // construct the new matrix
  SArray<uint32> new_index(matched);
  SArray<size_t> new_offset(offset.size()); new_offset[0] = 0;
  SArray<V> new_value(std::min(value.size(), (size_t)matched));

  size_t k = 0;
  for (size_t i = 0; i < offset.size() - 1; ++i) {
    size_t n = 0;
    for (size_t j = offset[i]; j < offset[i+1]; ++j) {
      if (remapped_idx[j] == 0) continue;
      ++ n;
      if (!bin) new_value[k] = value[j];
      new_index[k++] = remapped_idx[j] - 1;
    }
    new_offset[i+1] = new_offset[i] + n;
  }
  // LL << offset.back();
  CHECK_EQ(k, matched);

  auto new_info = info;
  new_info.set_sizeof_index(sizeof(uint32));
  new_info.set_nnz(new_index.size());
  SizeR local(0, idx_dict.size());
  if (new_info.row_major())  {
    local.To(new_info.mutable_col());
  } else {
    local.To(new_info.mutable_row());
  }
  // LL << curr_o << " " << local.end() << " " << curr_j;
  return MatrixPtr<V>(new SparseMatrix<uint32, V>(new_info, new_offset, new_index, new_value));
}

} // namespace ps
