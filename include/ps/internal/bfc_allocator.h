/**
 * Copyright 2017 Junxue Zhang
 */
#ifndef PS_INTERNAL_BFC_ALLOCATOR_H_
#define PS_INTERNAL_BFC_ALLOCATOR_H_

#include <cstdint>
#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <vector>

/**
 * @brief The memory allocator
 *
 * The allocator is used to allocate offset (addresss from 0) from a given size of memory
 * region
 */
class BFCAllocator : public std::enable_shared_from_this<BFCAllocator> {
 public:
  using Offset = int64_t;
  using MemoryChunkId = int;

  class MemoryChunk;
  class MemoryBin;

  static const size_t k_min_allocation_bits = 8;
  static const size_t k_min_allocation_size = 1 << k_min_allocation_bits;

  static const int k_num_of_bins = 21;
  static const int k_invalid_bin_num = -1;

  static const Offset k_invalid_offset = -1;

  static const int k_invalid_chunk_id = -1;

  using MemoryChunkPtr = std::shared_ptr<MemoryChunk>;

  using MemoryBinPtr = std::shared_ptr<MemoryBin>;

  /**
   * @brief Constructor
   *
   * Need to call InitMemoryBins() and InitMemoryChunks() after calling constructor
   *
   * @param id the memroy allocator id
   * @param size the size of managed memory region
   */
  BFCAllocator(int id, size_t size);

  ~BFCAllocator();

  /**
   * @brief Initialize all memory bins
   */
  void InitMemoryBins();

  /**
   * @brief Initialize a memory chunk to hold all available space
   */
  void InitMemoryChunks();

  /**
   * @brief Create memory allocator and initilize memery bins and chunks
   *
   * It is a combination call of BFCAllocator(int id, size_t size), InitMemoryBins()
   * and InitMemoryChunks()
   *
   * @param id the memroy allocator id
   * @param size the size of buffer the allocator holds
   *
   * @return std::shared_ptr<BFCAllocator> the created memory allocator
   */
  static std::shared_ptr<BFCAllocator> Create(int id, size_t size);

  /**
   * @brief Allocate space from the managed memory region
   *
   * The allocated space is (offset, offset + size)
   *
   * @param size size of the allocation
   *
   * @return Offset offset
   */
  Offset Allocate(size_t size);

  /**
   * @brief Deallocate space
   *
   * @param offset the offset of the space to deallocate
   */
  void Deallocate(Offset offset);

  /**
   * @brief Get the avaiable size from the managed region
   *
   * The size of space may not be continous.
   *
   * @return size_t available size
   */
  size_t GetTotalAvaiableSize();

  size_t RoundedBytes(size_t size);

  int BinNumberFromSize(size_t size);
  size_t SizeFromBinNumber(int bin_num);

  Offset FindChunkOffset(int bin_num, size_t size);
  void FreeAndMaybeCoalesce(MemoryChunkId id);

  MemoryChunkId AllocateChunk();
  MemoryChunkPtr GetChunk(MemoryChunkId id);
  void DeleteChunk(MemoryChunkId id);
  void DeallocateChunk(MemoryChunkId id);

  void InsertFreeChunkIntoBin(MemoryChunkId id);
  void RemoveFreeChunkFromBin(MemoryChunkId id);

  void SplitChunk(MemoryChunkId id, size_t size);
  void MergeChunk(MemoryChunkId id1, MemoryChunkId id2);

  void ShowDebug();

  /**
   * @brief The memory chunk
   */
  class MemoryChunk {
   public:
    bool is_free;
    Offset offset;
    size_t size;

    MemoryChunkId prev;
    MemoryChunkId next;

    int bin_num;
  };

  /**
   * @brief The memory bin
   */
  class MemoryBin {
   public:
    MemoryBin(size_t size, std::weak_ptr<BFCAllocator> allocator)
        : free_chunks(ChunkComparator(allocator)), size(size), parent_allocator(allocator) {}
    MemoryChunkId FindChunkIdLargerThanSize(size_t rounded_size) {
      auto allocator = parent_allocator.lock();
      for (auto itr = std::begin(free_chunks); itr != std::end(free_chunks); ++itr) {
        MemoryChunkId id = (*itr);
        MemoryChunkPtr chunk = allocator->GetChunk(id);
        if (chunk->size >= rounded_size) {
          return id;
        }
      }
      return k_invalid_chunk_id;
    }
    void InsertMemoryChunk(MemoryChunkId id) { free_chunks.insert(id); }
    void RemoveMemoryChunk(MemoryChunkId id) { free_chunks.erase(id); }

   private:
    class ChunkComparator {
     public:
      explicit ChunkComparator(std::weak_ptr<BFCAllocator> allocator)
          : parent_allocator(allocator) {}
      bool operator()(const MemoryChunkId ha, const MemoryChunkId hb) const {
        auto allocator = parent_allocator.lock();
        const MemoryChunkPtr a = allocator->GetChunk(ha);
        const MemoryChunkPtr b = allocator->GetChunk(hb);
        if (a->size != b->size) {
          return a->size < b->size;
        }
        return a->offset < b->offset;
      }

     private:
      // The memory allocator
      std::weak_ptr<BFCAllocator> parent_allocator;
    };

    using FreeChunkSet = std::set<MemoryChunkId, ChunkComparator>;
    FreeChunkSet free_chunks;

    size_t size;

    std::weak_ptr<BFCAllocator> parent_allocator;
  };

 private:
  int allocator_id;

  std::vector<MemoryBinPtr> bins;

  std::vector<MemoryChunkPtr> chunks;
  std::map<Offset, MemoryChunkId> allocated_chunks;
  MemoryChunkId free_chunks;

  size_t total_allocated_size;
  size_t total_size;

  std::mutex mutex;

  inline int Log2FloorNonZero(uint64_t n) { return 63 ^ __builtin_clzll(n); }
};

#endif  // PS_INTERNAL_BFC_ALLOCATOR_H_
