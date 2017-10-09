/**
 * Copyright 2017 Junxue Zhang, Jingrong Chen
 */
#include "ps/internal/bfc_allocator.h"

#include <cassert>
#include <iostream>

// Implementation of BFCAllocator

BFCAllocator::BFCAllocator(int id, size_t size)
    : allocator_id(id),
      bins(k_num_of_bins),
      free_chunks(k_invalid_chunk_id),
      total_allocated_size(0) {
  total_size = RoundedBytes(size);
}

BFCAllocator::~BFCAllocator() {}

void BFCAllocator::InitMemoryBins() {
  for (auto bin_num = 0; bin_num < k_num_of_bins; ++bin_num) {
    size_t bin_size = SizeFromBinNumber(bin_num);
    MemoryBinPtr bin = std::make_shared<MemoryBin>(bin_size, shared_from_this());
    bins[bin_num] = bin;
  }
}

void BFCAllocator::InitMemoryChunks() {
  MemoryChunkId chunk_id = AllocateChunk();
  MemoryChunkPtr chunk = GetChunk(chunk_id);
  chunk->is_free = true;
  chunk->offset = 0;
  chunk->size = total_size;
  chunk->prev = k_invalid_chunk_id;
  chunk->next = k_invalid_chunk_id;
  chunk->bin_num = k_invalid_bin_num;

  InsertFreeChunkIntoBin(chunk_id);
  allocated_chunks[chunk->offset] = chunk_id;
}

std::shared_ptr<BFCAllocator> BFCAllocator::Create(int id, size_t size) {
  std::shared_ptr<BFCAllocator> allocator = std::make_shared<BFCAllocator>(id, size);
  allocator->InitMemoryBins();
  allocator->InitMemoryChunks();
  return allocator;
}

BFCAllocator::Offset BFCAllocator::Allocate(size_t size) {
  if (size == 0) {
    return k_invalid_offset;
  }

  size_t rounded_size = RoundedBytes(size);
  int bin_num = BinNumberFromSize(rounded_size);

  std::lock_guard<std::mutex> lock(mutex);

  Offset offset = FindChunkOffset(bin_num, rounded_size);
  return offset;
}

void BFCAllocator::Deallocate(Offset offset) {
  if (offset == k_invalid_offset) {
    return;
  }

  std::lock_guard<std::mutex> lock(mutex);
  auto itr = allocated_chunks.find(offset);
  if (itr == std::end(allocated_chunks)) {
    assert(0);
    return;
  }
  MemoryChunkId chunk_id = itr->second;
  FreeAndMaybeCoalesce(chunk_id);
}

size_t BFCAllocator::GetTotalAvaiableSize() { return total_size - total_allocated_size; }

size_t BFCAllocator::RoundedBytes(size_t bytes) {
  size_t rounded_bytes =
      (k_min_allocation_size * ((bytes + k_min_allocation_size - 1) / k_min_allocation_size));
  return rounded_bytes;
}

int BFCAllocator::BinNumberFromSize(size_t size) {
  uint64_t v = std::max<size_t>(size, 256) >> k_min_allocation_bits;
  int b = std::min(k_num_of_bins - 1, Log2FloorNonZero(v));
  return b;
}

size_t BFCAllocator::SizeFromBinNumber(int bin_num) { return static_cast<size_t>(256) << bin_num; }

BFCAllocator::Offset BFCAllocator::FindChunkOffset(int bin_num, size_t rounded_size) {
  for (; bin_num < k_num_of_bins; bin_num++) {
    MemoryBinPtr bin = bins[bin_num];
    MemoryChunkId chunk_id = bin->FindChunkIdLargerThanSize(rounded_size);

    if (chunk_id == k_invalid_chunk_id) continue;
    MemoryChunkPtr chunk = GetChunk(chunk_id);
    assert(chunk->is_free == true);

    bin->RemoveMemoryChunk(chunk_id);
    chunk->bin_num = k_invalid_bin_num;
    if (chunk->size >= rounded_size * 2) {
      SplitChunk(chunk_id, rounded_size);
    }
    chunk->is_free = false;
    total_allocated_size += rounded_size;
    return chunk->offset;
  }
  return k_invalid_offset;
}

void BFCAllocator::FreeAndMaybeCoalesce(MemoryChunkId id) {
  MemoryChunkPtr chunk = GetChunk(id);
  assert(chunk->is_free == false && chunk->bin_num == k_invalid_bin_num);

  chunk->is_free = true;

  MemoryChunkId chunk_to_insert_back = id;

  if (chunk->next != k_invalid_chunk_id) {
    MemoryChunkPtr next_chunk = GetChunk(chunk->next);
    if (next_chunk->is_free) {
      chunk_to_insert_back = id;
      RemoveFreeChunkFromBin(chunk->next);
      MergeChunk(id, chunk->next);
    }
  }

  if (chunk->prev != k_invalid_chunk_id) {
    MemoryChunkPtr prev_chunk = GetChunk(chunk->prev);
    if (prev_chunk->is_free) {
      chunk_to_insert_back = chunk->prev;
      RemoveFreeChunkFromBin(chunk->prev);
      MergeChunk(chunk->prev, id);
    }
  }
  InsertFreeChunkIntoBin(chunk_to_insert_back);
}

BFCAllocator::MemoryChunkId BFCAllocator::AllocateChunk() {
  if (free_chunks == k_invalid_chunk_id) {
    MemoryChunkPtr chunk = std::make_shared<MemoryChunk>();
    chunks.push_back(chunk);
    return chunks.size() - 1;
  } else {
    MemoryChunkId id = free_chunks;
    MemoryChunkPtr chunk = GetChunk(id);
    free_chunks = chunk->next;
    return id;
  }
}

BFCAllocator::MemoryChunkPtr BFCAllocator::GetChunk(MemoryChunkId id) { return chunks[id]; }

void BFCAllocator::DeleteChunk(MemoryChunkId id) {
  MemoryChunkPtr chunk = GetChunk(id);
  allocated_chunks.erase(chunk->offset);
  DeallocateChunk(id);
}

void BFCAllocator::DeallocateChunk(MemoryChunkId id) {
  MemoryChunkPtr chunk = GetChunk(id);
  chunk->next = free_chunks;
  free_chunks = id;
}

void BFCAllocator::InsertFreeChunkIntoBin(MemoryChunkId id) {
  MemoryChunkPtr chunk = GetChunk(id);
  assert(chunk->is_free == true && chunk->bin_num == k_invalid_bin_num);
  int bin_num = BinNumberFromSize(chunk->size);
  MemoryBinPtr bin = bins[bin_num];
  chunk->bin_num = bin_num;
  bin->InsertMemoryChunk(id);
}

void BFCAllocator::RemoveFreeChunkFromBin(MemoryChunkId id) {
  MemoryChunkPtr chunk = GetChunk(id);
  assert(chunk->is_free == true && chunk->bin_num != k_invalid_bin_num);
  MemoryBinPtr bin = bins[chunk->bin_num];
  bin->RemoveMemoryChunk(id);
  chunk->bin_num = k_invalid_bin_num;
}

void BFCAllocator::SplitChunk(MemoryChunkId id, size_t size) {
  MemoryChunkPtr chunk = GetChunk(id);
  assert(chunk->is_free == true && chunk->bin_num == k_invalid_bin_num);

  MemoryChunkId new_chunk_id = AllocateChunk();
  MemoryChunkPtr new_chunk = GetChunk(new_chunk_id);
  new_chunk->offset = chunk->offset + size;
  new_chunk->size = chunk->size - size;
  chunk->size = size;
  new_chunk->is_free = true;
  new_chunk->bin_num = k_invalid_bin_num;

  allocated_chunks[new_chunk->offset] = new_chunk_id;

  MemoryChunkId chunk_neighbor_id = chunk->next;
  chunk->next = new_chunk_id;
  new_chunk->next = chunk_neighbor_id;
  new_chunk->prev = id;
  if (chunk_neighbor_id != k_invalid_chunk_id) {
    MemoryChunkPtr chunk_neighbor = GetChunk(chunk_neighbor_id);
    chunk_neighbor->prev = new_chunk_id;
  }
  InsertFreeChunkIntoBin(new_chunk_id);
}

void BFCAllocator::MergeChunk(MemoryChunkId id1, MemoryChunkId id2) {
  MemoryChunkPtr chunk1 = GetChunk(id1);
  MemoryChunkPtr chunk2 = GetChunk(id2);

  assert(chunk1->is_free && chunk2->is_free);

  MemoryChunkId id3 = chunk2->next;
  chunk1->next = id3;
  if (id3 != k_invalid_chunk_id) {
    MemoryChunkPtr chunk3 = GetChunk(id3);
    chunk3->prev = id1;
  }
  chunk1->size += chunk2->size;

  DeleteChunk(id2);
}

void BFCAllocator::ShowDebug() {
  std::cout << "Showing debug info for allocator: " << allocator_id << std::endl;
  for (size_t i = 0; i < chunks.size(); ++i) {
    MemoryChunkPtr ptr = chunks[i];
    std::cout << "Memory Chunk: " << i << " ["
              << "is_free: " << ptr->is_free << "\t"
              << "offset: " << ptr->offset << "\t"
              << "size: " << ptr->size << "\t"
              << "bin_num: " << ptr->bin_num << "\t"
              << "prev: " << ptr->prev << "\t"
              << "next: " << ptr->next << "\t"
              << "]" << std::endl;
  }
}
