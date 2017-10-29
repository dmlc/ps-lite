#pragma once
#include <stdlib.h>
#include <string>
#include <vector>
#include <unordered_map>
#include <xmmintrin.h>
#include <malloc.h>
#include <string.h>
#include <cassert>
#include "Verbs.hpp"
#ifndef __INTELLISENSE__
#include <signal.h>
#include "consts.h"
#endif


class NAGOptimizer;

class NAGNTNTOptimizer;
class NAGTNTOptimizer;
class NAGNTTOptimizer;
class NAGTTOptimizer;

class Optimizer
{
public:
    //name of optimizer, number of keys to be optimized, number of machines
    static Optimizer* Create(std::string name, size_t numMachines, std::unordered_map<int, int>& keySizes, const size_t prefetch_distance, Verbs* verbs);
    void PopulateTrainingParams(uint size, float* vals)
    {
        CHECK(States.size() * 2 + 2 == size);
        //CHECK(NegationOfLearningRates.size() == size);
        //CHECK(WeightDecays.size() == size);
        int keySizes = States.size();
        for (size_t i = 0; i < NegationOfLearningRates.size(); i++)
        {
            NegationOfLearningRates.at(i) = _mm_set1_ps(-1 * vals[i]);
            //printf("[note][PHUB] vkey = %d, lr = %f\n", i, vals[i]);
        }
        for (size_t i = 0; i < WeightDecays.size(); i++)
        {
            WeightDecays.at(i) = _mm_set1_ps(vals[i + keySizes]);
            //printf("[note][PHUB] vkey = %d, wd = %f\n", i, vals[i + keySizes]);
        }
        MomentumConstant = _mm_set1_ps(vals[keySizes * 2]);
        GradRescaling = _mm_set1_ps(vals[keySizes * 2 + 1]);
        printf("[note][PHUB] setting mom = %f, gradrescale = %f and learning rate and weight decays.\n", vals[keySizes * 2], vals[keySizes * 2 + 1]);
    }
    virtual void Update(size_t index, float* weightBuffer, float* gradBuffer, size_t bufferLen) {}

protected:
    Optimizer(size_t numMachines, std::unordered_map<int, int>& keySizes, const size_t prefetch_distance, Verbs* verbs)
        : prefetch_distance(prefetch_distance)
    {
        MachineCount = numMachines;
        //populate momentumconstant, learningrates, and weightdecays, gradrescaling.
        float rescale = 1.0f / 128; // 1 / batch size
        GradRescaling = _mm_set1_ps(rescale);

        //weight decays. this one we need to copy the orig from mxnet, but assign 0.00001 at first.
        auto initWd = 1.0e-5f;
        WeightDecays.resize(keySizes.size());
        for (size_t i = 0; i < WeightDecays.size(); i++)
        {
            WeightDecays[i] = _mm_set1_ps(initWd);
        }
        //learning rates.
        auto initLR = 0.1f;
        NegationOfLearningRates.resize(keySizes.size());
        for (size_t i = 0; i < NegationOfLearningRates.size(); i++)
        {
            NegationOfLearningRates[i] = _mm_set1_ps(-1 * initLR);
        }
        //momentum constants
        MomentumConstant = _mm_set1_ps(0.9f);

        //initialize current states.
        States.resize(keySizes.size());
        StateLengths.resize(keySizes.size());
        for (size_t i = 0; i < States.size(); i++)
        {
            //make this a multiple of INSTRUCTION_VECTOR_SIZE so we dont need to deal with remainder.
            auto bytes = RoundUp((size_t)keySizes[i] / sizeof(float), INSTRUCTION_VECTOR_SIZE) * sizeof(float);
            if (verbs == NULL || verbs->DirectConnect || verbs->Helper_Server_IsItMyKey(i) == false)
            {
		//verbs may be NULL if Gloo
		//if this key is not mine, i still create a buffer for it.
		//just that i don't care which node it is on.
                States[i] = (float*)memalign(INSTRUCTION_VECTOR_SIZE * sizeof(float), bytes);
            }
            else
            {
                //NUMA Aware.
                auto mySock = verbs->Helper_Server_GetEndpointFromKey(i, 0).SocketIdx;
                States[i] = (float*)AlignedAllocateUniversal(bytes, mySock);//default 2MB alignment. 
		CHECK(States[i]!=NULL) << "Cannot allocate state buffer on numa node "<< mySock <<". Too many allocations?";
            }

            StateLengths[i] = bytes;
            memset(States[i], 0, bytes);
        }
        OptimizationCounts.resize(keySizes.size());
    }

    virtual ~Optimizer() { }

    std::vector<__m128> WeightDecays; //weight decay
    std::vector<__m128> NegationOfLearningRates;
    std::vector<float*> States; // key -> expanded vectors of momentum.
    std::vector<size_t> StateLengths;
    __m128 MomentumConstant;//the expanded momentum vectors.
    __m128 GradRescaling;
    size_t NumberOfUpdates = 0;
    size_t MachineCount;
    std::string Name;
    std::vector<size_t> OptimizationCounts;
    //clipping, lr scheduler, idx2Name ignored.

    const size_t prefetch_distance;
};

class NAGTTOptimizer : public Optimizer
{
public:
    NAGTTOptimizer(size_t numMachines, std::unordered_map<int, int>& keySizes, const size_t prefetch_distance, Verbs* verbs)
        : Optimizer(numMachines, keySizes, prefetch_distance, verbs)
    {
        this->Name = "nagTT";
    }

    //index, weights and gradients (aggregated) 
    virtual void Update(size_t index, float* weightBuffer, float* gradBuffer, size_t bufferLen) override
    {
        CHECK(index < States.size());
        //CHECK(bufferLen * sizeof(float) == StateLengths[index]);

        size_t offset = 0;
        size_t prefetch_offset = prefetch_distance;

        float * momBuffer = States[index];

        __asm__ __volatile__(/* last_update = update; */
                              /* update = momentum * update - learning_rate * deltas; */
                              /* thetas += -momentum * last_update + (1 + momentum) * update */

            ".align 32 \n\t"
            "1: \n\t"

            // grad = grad * self.rescale_grad
            // ----
            // load grad
            "movaps         0x00(%[grad],%[offset],1),%%xmm0 \n\t"
            "movaps         0x10(%[grad],%[offset],1),%%xmm1 \n\t"
            "movaps         0x20(%[grad],%[offset],1),%%xmm2 \n\t"
            "movaps         0x30(%[grad],%[offset],1),%%xmm3 \n\t"
            // mul grad scalar scaling_factor
            "mulps          %[scaling_factor],%%xmm0 \n\t"
            "mulps          %[scaling_factor],%%xmm1 \n\t"
            "mulps          %[scaling_factor],%%xmm2 \n\t"
            "mulps          %[scaling_factor],%%xmm3 \n\t"

            // mom[:] *= self.momentum
            // ----
            // load mom
            "movaps         0x00(%[mom],%[offset],1),%%xmm4 \n\t"
            "movaps         0x10(%[mom],%[offset],1),%%xmm5 \n\t"
            "movaps         0x20(%[mom],%[offset],1),%%xmm6 \n\t"
            "movaps         0x30(%[mom],%[offset],1),%%xmm7 \n\t"
            // mul mom scalar momentum_factor
            "mulps          %[momentum_factor],%%xmm4 \n\t"
            "mulps          %[momentum_factor],%%xmm5 \n\t"
            "mulps          %[momentum_factor],%%xmm6 \n\t"
            "mulps          %[momentum_factor],%%xmm7 \n\t"

            // grad += wd * weight
            // ----
            // load weight
            "movaps         0x00(%[weight],%[offset],1),%%xmm8 \n\t"
            "movaps         0x10(%[weight],%[offset],1),%%xmm9 \n\t"
            "movaps         0x20(%[weight],%[offset],1),%%xmm10 \n\t"
            "movaps         0x30(%[weight],%[offset],1),%%xmm11 \n\t"
            // make copy for weight decay calculation
            "movaps          %%xmm8,%%xmm12 \n\t"
            "movaps          %%xmm9,%%xmm13 \n\t"
            "movaps          %%xmm10,%%xmm14 \n\t"
            "movaps          %%xmm11,%%xmm15 \n\t"
            // mul weight scalar weight_decay_factor
            "mulps          %[weight_decay_factor],%%xmm12 \n\t"
            "mulps          %[weight_decay_factor],%%xmm13 \n\t"
            "mulps          %[weight_decay_factor],%%xmm14 \n\t"
            "mulps          %[weight_decay_factor],%%xmm15 \n\t"
            // add grad weight
            "addps          %%xmm12,%%xmm0 \n\t"
            "addps          %%xmm13,%%xmm1 \n\t"
            "addps          %%xmm14,%%xmm2 \n\t"
            "addps          %%xmm15,%%xmm3 \n\t"

            // mom[:] += grad
            // ----
            // add mom grad
            "addps          %%xmm0,%%xmm4 \n\t"
            "addps          %%xmm1,%%xmm5 \n\t"
            "addps          %%xmm2,%%xmm6 \n\t"
            "addps          %%xmm3,%%xmm7 \n\t"
            // store mom
            "movaps %%xmm4,0x00(%[mom],%[offset],1) \n\t"
            "movaps %%xmm5,0x10(%[mom],%[offset],1) \n\t"
            "movaps %%xmm6,0x20(%[mom],%[offset],1) \n\t"
            "movaps %%xmm7,0x30(%[mom],%[offset],1) \n\t"

            // grad[:] += self.momentum * mom
            // ----
            // mul mom scalar momentum_factor
            "mulps          %[momentum_factor],%%xmm4 \n\t"
            "mulps          %[momentum_factor],%%xmm5 \n\t"
            "mulps          %[momentum_factor],%%xmm6 \n\t"
            "mulps          %[momentum_factor],%%xmm7 \n\t"
            // add grad mom
            "addps          %%xmm4,%%xmm0 \n\t"
            "addps          %%xmm5,%%xmm1 \n\t"
            "addps          %%xmm6,%%xmm2 \n\t"
            "addps          %%xmm7,%%xmm3 \n\t"

            // weight[:] += -lr * grad
            // ----
            // mul grad scalar learning_rate
            "mulps          %[learning_rate],%%xmm0 \n\t"
            "mulps          %[learning_rate],%%xmm1 \n\t"
            "mulps          %[learning_rate],%%xmm2 \n\t"
            "mulps          %[learning_rate],%%xmm3 \n\t"
            // sub weight grad (already negative)
            "addps          %%xmm0,%%xmm8 \n\t"
            "addps          %%xmm1,%%xmm9 \n\t"
            "addps          %%xmm2,%%xmm10 \n\t"
            "addps          %%xmm3,%%xmm11 \n\t"
            // store weight
            "movaps  %%xmm8,0x00(%[weight],%[offset],1) \n\t"
            "movaps  %%xmm9,0x10(%[weight],%[offset],1) \n\t"
            "movaps %%xmm10,0x20(%[weight],%[offset],1) \n\t"
            "movaps %%xmm11,0x30(%[weight],%[offset],1) \n\t"

            "add %[inc],%[offset] \n\t"
            "cmp %[end],%[offset] \n\t"
            "jb 1b \n\t"

            "4:"
            "sfence \n\t"

            : [offset] "+r" (offset)//,
              //[prefetch_offset] "+r" (prefetch_offset)
            : [grad]   "r" (gradBuffer),
            [mom]    "r" (momBuffer),
            [weight] "r" (weightBuffer),

            [learning_rate] "m" (NegationOfLearningRates[index]),
            [momentum_factor] "m" (MomentumConstant),
            [scaling_factor] "m" (GradRescaling),
            [weight_decay_factor] "m" (WeightDecays[index]),

            [inc] "i" (CACHELINE_SIZE_BYTES),

            [end] "mr" (bufferLen * sizeof(float))

            : "cc", "memory",
            "xmm0", "xmm1", "xmm2", "xmm3",
            "xmm4", "xmm5", "xmm6", "xmm7",
            "xmm8", "xmm9", "xmm10", "xmm11",
            "xmm12", "xmm13", "xmm14", "xmm15");

    }
};


class NAGTNTOptimizer : public Optimizer
{
public:
    NAGTNTOptimizer(size_t numMachines, std::unordered_map<int, int>& keySizes, const size_t prefetch_distance, Verbs* verbs)
        : Optimizer(numMachines, keySizes, prefetch_distance, verbs)
    {
        this->Name = "nagTNT";
    }

    //index, weights and gradients (aggregated) 
    virtual void Update(size_t index, float* weightBuffer, float* gradBuffer, size_t bufferLen) override
    {
        CHECK(index < States.size());
        CHECK(bufferLen * sizeof(float) == StateLengths[index]);

        size_t offset = 0;
        size_t prefetch_offset = prefetch_distance;

        float * momBuffer = States[index];

        __asm__ __volatile__(/* last_update = update; */
                              /* update = momentum * update - learning_rate * deltas; */
                              /* thetas += -momentum * last_update + (1 + momentum) * update */

            ".align 32 \n\t"
            "1: \n\t"

            // grad = grad * self.rescale_grad
            // ----
            // load grad
            "movaps         0x00(%[grad],%[offset],1),%%xmm0 \n\t"
            "movaps         0x10(%[grad],%[offset],1),%%xmm1 \n\t"
            "movaps         0x20(%[grad],%[offset],1),%%xmm2 \n\t"
            "movaps         0x30(%[grad],%[offset],1),%%xmm3 \n\t"
            // mul grad scalar scaling_factor
            "mulps          %[scaling_factor],%%xmm0 \n\t"
            "mulps          %[scaling_factor],%%xmm1 \n\t"
            "mulps          %[scaling_factor],%%xmm2 \n\t"
            "mulps          %[scaling_factor],%%xmm3 \n\t"

            // mom[:] *= self.momentum
            // ----
            // load mom
            "movaps         0x00(%[mom],%[offset],1),%%xmm4 \n\t"
            "movaps         0x10(%[mom],%[offset],1),%%xmm5 \n\t"
            "movaps         0x20(%[mom],%[offset],1),%%xmm6 \n\t"
            "movaps         0x30(%[mom],%[offset],1),%%xmm7 \n\t"
            // mul mom scalar momentum_factor
            "mulps          %[momentum_factor],%%xmm4 \n\t"
            "mulps          %[momentum_factor],%%xmm5 \n\t"
            "mulps          %[momentum_factor],%%xmm6 \n\t"
            "mulps          %[momentum_factor],%%xmm7 \n\t"

            // grad += wd * weight
            // ----
            // load weight
            "movaps         0x00(%[weight],%[offset],1),%%xmm8 \n\t"
            "movaps         0x10(%[weight],%[offset],1),%%xmm9 \n\t"
            "movaps         0x20(%[weight],%[offset],1),%%xmm10 \n\t"
            "movaps         0x30(%[weight],%[offset],1),%%xmm11 \n\t"
            // make copy for weight decay calculation
            "movaps          %%xmm8,%%xmm12 \n\t"
            "movaps          %%xmm9,%%xmm13 \n\t"
            "movaps          %%xmm10,%%xmm14 \n\t"
            "movaps          %%xmm11,%%xmm15 \n\t"
            // mul weight scalar weight_decay_factor
            "mulps          %[weight_decay_factor],%%xmm12 \n\t"
            "mulps          %[weight_decay_factor],%%xmm13 \n\t"
            "mulps          %[weight_decay_factor],%%xmm14 \n\t"
            "mulps          %[weight_decay_factor],%%xmm15 \n\t"
            // add grad weight
            "addps          %%xmm12,%%xmm0 \n\t"
            "addps          %%xmm13,%%xmm1 \n\t"
            "addps          %%xmm14,%%xmm2 \n\t"
            "addps          %%xmm15,%%xmm3 \n\t"

            // mom[:] += grad
            // ----
            // add mom grad
            "addps          %%xmm0,%%xmm4 \n\t"
            "addps          %%xmm1,%%xmm5 \n\t"
            "addps          %%xmm2,%%xmm6 \n\t"
            "addps          %%xmm3,%%xmm7 \n\t"
            // store mom
            "movntps %%xmm4,0x00(%[mom],%[offset],1) \n\t"
            "movntps %%xmm5,0x10(%[mom],%[offset],1) \n\t"
            "movntps %%xmm6,0x20(%[mom],%[offset],1) \n\t"
            "movntps %%xmm7,0x30(%[mom],%[offset],1) \n\t"

            // grad[:] += self.momentum * mom
            // ----
            // mul mom scalar momentum_factor
            "mulps          %[momentum_factor],%%xmm4 \n\t"
            "mulps          %[momentum_factor],%%xmm5 \n\t"
            "mulps          %[momentum_factor],%%xmm6 \n\t"
            "mulps          %[momentum_factor],%%xmm7 \n\t"
            // add grad mom
            "addps          %%xmm4,%%xmm0 \n\t"
            "addps          %%xmm5,%%xmm1 \n\t"
            "addps          %%xmm6,%%xmm2 \n\t"
            "addps          %%xmm7,%%xmm3 \n\t"

            // weight[:] += -lr * grad
            // ----
            // mul grad scalar learning_rate
            "mulps          %[learning_rate],%%xmm0 \n\t"
            "mulps          %[learning_rate],%%xmm1 \n\t"
            "mulps          %[learning_rate],%%xmm2 \n\t"
            "mulps          %[learning_rate],%%xmm3 \n\t"
            // sub weight grad (already negative)
            "addps          %%xmm0,%%xmm8 \n\t"
            "addps          %%xmm1,%%xmm9 \n\t"
            "addps          %%xmm2,%%xmm10 \n\t"
            "addps          %%xmm3,%%xmm11 \n\t"
            // store weight
            "movntps  %%xmm8,0x00(%[weight],%[offset],1) \n\t"
            "movntps  %%xmm9,0x10(%[weight],%[offset],1) \n\t"
            "movntps %%xmm10,0x20(%[weight],%[offset],1) \n\t"
            "movntps %%xmm11,0x30(%[weight],%[offset],1) \n\t"

            "add %[inc],%[offset] \n\t"
            "cmp %[end],%[offset] \n\t"
            "jb 1b \n\t"

            "4:"
            "sfence \n\t"

            : [offset] "+r" (offset)//,
              //[prefetch_offset] "+r" (prefetch_offset)
            : [grad]   "r" (gradBuffer),
            [mom]    "r" (momBuffer),
            [weight] "r" (weightBuffer),

            [learning_rate] "m" (NegationOfLearningRates[index]),
            [momentum_factor] "m" (MomentumConstant),
            [scaling_factor] "m" (GradRescaling),
            [weight_decay_factor] "m" (WeightDecays[index]),

            [inc] "i" (CACHELINE_SIZE_BYTES),

            [end] "mr" (bufferLen * sizeof(float))

            : "cc", "memory",
            "xmm0", "xmm1", "xmm2", "xmm3",
            "xmm4", "xmm5", "xmm6", "xmm7",
            "xmm8", "xmm9", "xmm10", "xmm11",
            "xmm12", "xmm13", "xmm14", "xmm15");

    }
};

class NAGNTTOptimizer : public Optimizer
{
public:
    NAGNTTOptimizer(size_t numMachines, std::unordered_map<int, int>& keySizes, const size_t prefetch_distance, Verbs* verbs)
        : Optimizer(numMachines, keySizes, prefetch_distance, verbs)
    {
        this->Name = "nagNTT";
    }

    //index, weights and gradients (aggregated) 
    virtual void Update(size_t index, float* weightBuffer, float* gradBuffer, size_t bufferLen) override
    {
        CHECK(index < States.size());
        CHECK(bufferLen * sizeof(float) == StateLengths[index]);

        size_t offset = 0;
        size_t prefetch_offset = prefetch_distance;

        float * momBuffer = States[index];

        __asm__ __volatile__("prefetchnta 0x000(%[grad],%[offset],1) \n\t"
            "prefetchnta 0x040(%[grad],%[offset],1) \n\t"
            "prefetchnta 0x080(%[grad],%[offset],1) \n\t"
            "prefetchnta 0x100(%[grad],%[offset],1) \n\t"
            "prefetchnta 0x140(%[grad],%[offset],1) \n\t"
            "prefetchnta 0x180(%[grad],%[offset],1) \n\t"
            "prefetchnta 0x1C0(%[grad],%[offset],1) \n\t"
            "prefetchnta 0x200(%[weight],%[offset],1) \n\t"
            "prefetchnta 0x240(%[weight],%[offset],1) \n\t"

            /* last_update = update; */
            /* update = momentum * update - learning_rate * deltas; */
            /* thetas += -momentum * last_update + (1 + momentum) * update */

            ".align 32 \n\t"
            "1: \n\t"

            // grad = grad * self.rescale_grad
            // ----
            // load grad
            "movaps         0x00(%[grad],%[offset],1),%%xmm0 \n\t"
            "movaps         0x10(%[grad],%[offset],1),%%xmm1 \n\t"
            "movaps         0x20(%[grad],%[offset],1),%%xmm2 \n\t"
            "movaps         0x30(%[grad],%[offset],1),%%xmm3 \n\t"
            "prefetchnta    0x00(%[grad],%[prefetch_offset],1) \n\t"
            // mul grad scalar scaling_factor
            "mulps          %[scaling_factor],%%xmm0 \n\t"
            "mulps          %[scaling_factor],%%xmm1 \n\t"
            "mulps          %[scaling_factor],%%xmm2 \n\t"
            "mulps          %[scaling_factor],%%xmm3 \n\t"

            // mom[:] *= self.momentum
            // ----
            // load mom
            "movaps         0x00(%[mom],%[offset],1),%%xmm4 \n\t"
            "movaps         0x10(%[mom],%[offset],1),%%xmm5 \n\t"
            "movaps         0x20(%[mom],%[offset],1),%%xmm6 \n\t"
            "movaps         0x30(%[mom],%[offset],1),%%xmm7 \n\t"
            // mul mom scalar momentum_factor
            "mulps          %[momentum_factor],%%xmm4 \n\t"
            "mulps          %[momentum_factor],%%xmm5 \n\t"
            "mulps          %[momentum_factor],%%xmm6 \n\t"
            "mulps          %[momentum_factor],%%xmm7 \n\t"

            // grad += wd * weight
            // ----
            // load weight
            "movaps         0x00(%[weight],%[offset],1),%%xmm8 \n\t"
            "movaps         0x10(%[weight],%[offset],1),%%xmm9 \n\t"
            "movaps         0x20(%[weight],%[offset],1),%%xmm10 \n\t"
            "movaps         0x30(%[weight],%[offset],1),%%xmm11 \n\t"
            // make copy for weight decay calculation
            "movaps          %%xmm8,%%xmm12 \n\t"
            "movaps          %%xmm9,%%xmm13 \n\t"
            "movaps          %%xmm10,%%xmm14 \n\t"
            "movaps          %%xmm11,%%xmm15 \n\t"
            // mul weight scalar weight_decay_factor
            "mulps          %[weight_decay_factor],%%xmm12 \n\t"
            "mulps          %[weight_decay_factor],%%xmm13 \n\t"
            "mulps          %[weight_decay_factor],%%xmm14 \n\t"
            "mulps          %[weight_decay_factor],%%xmm15 \n\t"
            // add grad weight
            "addps          %%xmm12,%%xmm0 \n\t"
            "addps          %%xmm13,%%xmm1 \n\t"
            "addps          %%xmm14,%%xmm2 \n\t"
            "addps          %%xmm15,%%xmm3 \n\t"

            // mom[:] += grad
            // ----
            // add mom grad
            "addps          %%xmm0,%%xmm4 \n\t"
            "addps          %%xmm1,%%xmm5 \n\t"
            "addps          %%xmm2,%%xmm6 \n\t"
            "addps          %%xmm3,%%xmm7 \n\t"
            // store mom
            "movaps %%xmm4,0x00(%[mom],%[offset],1) \n\t"
            "movaps %%xmm5,0x10(%[mom],%[offset],1) \n\t"
            "movaps %%xmm6,0x20(%[mom],%[offset],1) \n\t"
            "movaps %%xmm7,0x30(%[mom],%[offset],1) \n\t"

            // grad[:] += self.momentum * mom
            // ----
            // mul mom scalar momentum_factor
            "mulps          %[momentum_factor],%%xmm4 \n\t"
            "mulps          %[momentum_factor],%%xmm5 \n\t"
            "mulps          %[momentum_factor],%%xmm6 \n\t"
            "mulps          %[momentum_factor],%%xmm7 \n\t"
            // add grad mom
            "addps          %%xmm4,%%xmm0 \n\t"
            "addps          %%xmm5,%%xmm1 \n\t"
            "addps          %%xmm6,%%xmm2 \n\t"
            "addps          %%xmm7,%%xmm3 \n\t"

            // weight[:] += -lr * grad
            // ----
            // mul grad scalar learning_rate
            "mulps          %[learning_rate],%%xmm0 \n\t"
            "mulps          %[learning_rate],%%xmm1 \n\t"
            "mulps          %[learning_rate],%%xmm2 \n\t"
            "mulps          %[learning_rate],%%xmm3 \n\t"
            // sub weight grad (already negative)
            "addps          %%xmm0,%%xmm8 \n\t"
            "addps          %%xmm1,%%xmm9 \n\t"
            "addps          %%xmm2,%%xmm10 \n\t"
            "addps          %%xmm3,%%xmm11 \n\t"
            // store weight
            "movaps  %%xmm8,0x00(%[weight],%[offset],1) \n\t"
            "movaps  %%xmm9,0x10(%[weight],%[offset],1) \n\t"
            "movaps %%xmm10,0x20(%[weight],%[offset],1) \n\t"
            "movaps %%xmm11,0x30(%[weight],%[offset],1) \n\t"

            "add %[inc],%[prefetch_offset] \n\t"
            "add %[inc],%[offset] \n\t"
            "cmp %[end],%[offset] \n\t"
            "jb 1b \n\t"

            "4:"
            "sfence \n\t"

            : [offset] "+r" (offset),
            [prefetch_offset] "+r" (prefetch_offset)
            : [grad]   "r" (gradBuffer),
            [mom]    "r" (momBuffer),
            [weight] "r" (weightBuffer),

            [learning_rate] "m" (NegationOfLearningRates[index]),
            [momentum_factor] "m" (MomentumConstant),
            [scaling_factor] "m" (GradRescaling),
            [weight_decay_factor] "m" (WeightDecays[index]),

            [inc] "i" (CACHELINE_SIZE_BYTES),

            [end] "mr" (bufferLen * sizeof(float))

            : "cc", "memory",
            "xmm0", "xmm1", "xmm2", "xmm3",
            "xmm4", "xmm5", "xmm6", "xmm7",
            "xmm8", "xmm9", "xmm10", "xmm11",
            "xmm12", "xmm13", "xmm14", "xmm15");

    }
};

class NAGNTNTOptimizer : public Optimizer
{
public:
    NAGNTNTOptimizer(size_t numMachines, std::unordered_map<int, int>& keySizes, const size_t prefetch_distance, Verbs* verbs)
        : Optimizer(numMachines, keySizes, prefetch_distance, verbs)
    {
        this->Name = "nagNTNT";
    }

    //index, weights and gradients (aggregated) 
    virtual void Update(size_t index, float* weightBuffer, float* gradBuffer, size_t bufferLen) override
    {
        CHECK(index < States.size());
        CHECK(bufferLen * sizeof(float) == StateLengths[index]);

        size_t offset = 0;
        size_t prefetch_offset = prefetch_distance;

        float * momBuffer = States[index];

        __asm__ __volatile__("prefetchnta 0x000(%[grad],%[offset],1) \n\t"
            "prefetchnta 0x040(%[grad],%[offset],1) \n\t"
            "prefetchnta 0x080(%[grad],%[offset],1) \n\t"
            "prefetchnta 0x100(%[grad],%[offset],1) \n\t"

            "prefetchnta 0x000(%[mom],%[offset],1) \n\t"
            "prefetchnta 0x040(%[mom],%[offset],1) \n\t"
            "prefetchnta 0x080(%[mom],%[offset],1) \n\t"

            "prefetchnta 0x000(%[weight],%[offset],1) \n\t"
            "prefetchnta 0x040(%[weight],%[offset],1) \n\t"
            "prefetchnta 0x080(%[weight],%[offset],1) \n\t"

            /* last_update = update; */
            /* update = momentum * update - learning_rate * deltas; */
            /* thetas += -momentum * last_update + (1 + momentum) * update */

            ".align 32 \n\t"
            "1: \n\t"

            // grad = grad * self.rescale_grad
            // ----
            // load grad
            "movaps         0x00(%[grad],%[offset],1),%%xmm0 \n\t"
            "movaps         0x10(%[grad],%[offset],1),%%xmm1 \n\t"
            "movaps         0x20(%[grad],%[offset],1),%%xmm2 \n\t"
            "movaps         0x30(%[grad],%[offset],1),%%xmm3 \n\t"
            "prefetchnta    0x00(%[grad],%[prefetch_offset],1) \n\t"
            // mul grad scalar scaling_factor
            "mulps          %[scaling_factor],%%xmm0 \n\t"
            "mulps          %[scaling_factor],%%xmm1 \n\t"
            "mulps          %[scaling_factor],%%xmm2 \n\t"
            "mulps          %[scaling_factor],%%xmm3 \n\t"

            // mom[:] *= self.momentum
            // ----
            // load mom
            "movaps         0x00(%[mom],%[offset],1),%%xmm4 \n\t"
            "movaps         0x10(%[mom],%[offset],1),%%xmm5 \n\t"
            "movaps         0x20(%[mom],%[offset],1),%%xmm6 \n\t"
            "movaps         0x30(%[mom],%[offset],1),%%xmm7 \n\t"
            "prefetchnta    0x00(%[mom],%[prefetch_offset],1) \n\t"
            // mul mom scalar momentum_factor
            "mulps          %[momentum_factor],%%xmm4 \n\t"
            "mulps          %[momentum_factor],%%xmm5 \n\t"
            "mulps          %[momentum_factor],%%xmm6 \n\t"
            "mulps          %[momentum_factor],%%xmm7 \n\t"

            // grad += wd * weight
            // ----
            // load weight
            "movaps         0x00(%[weight],%[offset],1),%%xmm8 \n\t"
            "movaps         0x10(%[weight],%[offset],1),%%xmm9 \n\t"
            "movaps         0x20(%[weight],%[offset],1),%%xmm10 \n\t"
            "movaps         0x30(%[weight],%[offset],1),%%xmm11 \n\t"
            "prefetchnta    0x00(%[weight],%[prefetch_offset],1) \n\t"
            // make copy for weight decay calculation
            "movaps          %%xmm8,%%xmm12 \n\t"
            "movaps          %%xmm9,%%xmm13 \n\t"
            "movaps          %%xmm10,%%xmm14 \n\t"
            "movaps          %%xmm11,%%xmm15 \n\t"
            // mul weight scalar weight_decay_factor
            "mulps          %[weight_decay_factor],%%xmm12 \n\t"
            "mulps          %[weight_decay_factor],%%xmm13 \n\t"
            "mulps          %[weight_decay_factor],%%xmm14 \n\t"
            "mulps          %[weight_decay_factor],%%xmm15 \n\t"
            // add grad weight
            "addps          %%xmm12,%%xmm0 \n\t"
            "addps          %%xmm13,%%xmm1 \n\t"
            "addps          %%xmm14,%%xmm2 \n\t"
            "addps          %%xmm15,%%xmm3 \n\t"

            // mom[:] += grad
            // ----
            // add mom grad
            "addps          %%xmm0,%%xmm4 \n\t"
            "addps          %%xmm1,%%xmm5 \n\t"
            "addps          %%xmm2,%%xmm6 \n\t"
            "addps          %%xmm3,%%xmm7 \n\t"
            // store mom
            "movntps %%xmm4,0x00(%[mom],%[offset],1) \n\t"
            "movntps %%xmm5,0x10(%[mom],%[offset],1) \n\t"
            "movntps %%xmm6,0x20(%[mom],%[offset],1) \n\t"
            "movntps %%xmm7,0x30(%[mom],%[offset],1) \n\t"

            // grad[:] += self.momentum * mom
            // ----
            // mul mom scalar momentum_factor
            "mulps          %[momentum_factor],%%xmm4 \n\t"
            "mulps          %[momentum_factor],%%xmm5 \n\t"
            "mulps          %[momentum_factor],%%xmm6 \n\t"
            "mulps          %[momentum_factor],%%xmm7 \n\t"
            // add grad mom
            "addps          %%xmm4,%%xmm0 \n\t"
            "addps          %%xmm5,%%xmm1 \n\t"
            "addps          %%xmm6,%%xmm2 \n\t"
            "addps          %%xmm7,%%xmm3 \n\t"

            // weight[:] += -lr * grad
            // ----
            // mul grad scalar learning_rate
            "mulps          %[learning_rate],%%xmm0 \n\t"
            "mulps          %[learning_rate],%%xmm1 \n\t"
            "mulps          %[learning_rate],%%xmm2 \n\t"
            "mulps          %[learning_rate],%%xmm3 \n\t"
            // sub weight grad (already negative)
            "addps          %%xmm0,%%xmm8 \n\t"
            "addps          %%xmm1,%%xmm9 \n\t"
            "addps          %%xmm2,%%xmm10 \n\t"
            "addps          %%xmm3,%%xmm11 \n\t"
            // store weight
            "movntps  %%xmm8,0x00(%[weight],%[offset],1) \n\t"
            "movntps  %%xmm9,0x10(%[weight],%[offset],1) \n\t"
            "movntps %%xmm10,0x20(%[weight],%[offset],1) \n\t"
            "movntps %%xmm11,0x30(%[weight],%[offset],1) \n\t"

            "add %[inc],%[prefetch_offset] \n\t"
            "add %[inc],%[offset] \n\t"
            "cmp %[end],%[offset] \n\t"
            "jb 1b \n\t"

            "4:"
            "sfence \n\t"

            : [offset] "+r" (offset),
            [prefetch_offset] "+r" (prefetch_offset)
            : [grad]   "r" (gradBuffer),
            [mom]    "r" (momBuffer),
            [weight] "r" (weightBuffer),

            [learning_rate] "m" (NegationOfLearningRates[index]),
            [momentum_factor] "m" (MomentumConstant),
            [scaling_factor] "m" (GradRescaling),
            [weight_decay_factor] "m" (WeightDecays[index]),

            [inc] "i" (CACHELINE_SIZE_BYTES),

            [end] "mr" (bufferLen * sizeof(float))

            : "cc", "memory",
            "xmm0", "xmm1", "xmm2", "xmm3",
            "xmm4", "xmm5", "xmm6", "xmm7",
            "xmm8", "xmm9", "xmm10", "xmm11",
            "xmm12", "xmm13", "xmm14", "xmm15");

    }
};

class SGDNTNTOptimizer : public Optimizer
{
public:
    SGDNTNTOptimizer(size_t numMachines, std::unordered_map<int, int>& keySizes, const size_t prefetch_distance, Verbs* verbs)
        : Optimizer(numMachines, keySizes, prefetch_distance, verbs)
    {
        this->Name = "sgdntnt";
    }

    //index, weights and gradients (aggregated) 
    virtual void Update(size_t index, float* weightBuffer, float* gradBuffer, size_t bufferLen) override
    {
        CHECK(index < States.size());
        CHECK(bufferLen * sizeof(float) == StateLengths[index]);

        size_t offset = 0;
        size_t prefetch_offset = prefetch_distance;

        float * momBuffer = States[index];

        __asm__ __volatile__("prefetchnta 0x000(%[grad],%[offset],1) \n\t"
            "prefetchnta 0x040(%[grad],%[offset],1) \n\t"
            "prefetchnta 0x080(%[grad],%[offset],1) \n\t"
            "prefetchnta 0x0C0(%[grad],%[offset],1) \n\t"
            "prefetchnta 0x100(%[grad],%[offset],1) \n\t"

            "prefetchnta 0x000(%[weight],%[offset],1) \n\t"
            "prefetchnta 0x040(%[weight],%[offset],1) \n\t"
            "prefetchnta 0x080(%[weight],%[offset],1) \n\t"
            "prefetchnta 0x0C0(%[weight],%[offset],1) \n\t"
            "prefetchnta 0x100(%[weight],%[offset],1) \n\t"

            ".align 32 \n\t"
            "1: \n\t"

            // load gradient
            "movaps      0x00(%[grad],%[offset],1),%%xmm0 \n\t"
            "movaps      0x10(%[grad],%[offset],1),%%xmm1 \n\t"
            "movaps      0x20(%[grad],%[offset],1),%%xmm2 \n\t"
            "movaps      0x30(%[grad],%[offset],1),%%xmm3 \n\t"
            "prefetchnta 0x00(%[grad],%[prefetch_offset],1) \n\t"

            // multiply gradient by learning rate
            "mulps %[learning_rate],%%xmm0  \n\t"
            "mulps %[learning_rate],%%xmm1  \n\t"
            "mulps %[learning_rate],%%xmm2  \n\t"
            "mulps %[learning_rate],%%xmm3  \n\t"

            // subtract from weight
            "addps       0x00(%[weight],%[offset],1),%%xmm0   \n\t"
            "addps       0x10(%[weight],%[offset],1),%%xmm1   \n\t"
            "addps       0x20(%[weight],%[offset],1),%%xmm2   \n\t"
            "addps       0x30(%[weight],%[offset],1),%%xmm3   \n\t"
            "prefetchnta 0x00(%[weight],%[prefetch_offset],1) \n\t"

            // store
            "movntps %%xmm0,0x00(%[weight],%[offset],1)      \n\t"
            "movntps %%xmm1,0x10(%[weight],%[offset],1)      \n\t"
            "movntps %%xmm2,0x20(%[weight],%[offset],1)      \n\t"
            "movntps %%xmm3,0x30(%[weight],%[offset],1)      \n\t"

            // increment, check if done, and loop
            "add %[inc],%[prefetch_offset] \n\t"
            "add %[inc],%[offset] \n\t"
            "cmp %[end],%[offset] \n\t"
            "jb 1b \n\t"

            "4:"
            "sfence \n\t"

            : [offset] "+r" (offset),
            [prefetch_offset] "+r" (prefetch_offset)
            : [grad]   "r" (gradBuffer),
            [mom]    "r" (momBuffer),
            [weight] "r" (weightBuffer),

            [learning_rate] "mr" (NegationOfLearningRates[index]),
            [momentum_factor] "mr" (MomentumConstant),
            [scaling_factor] "mr" (GradRescaling),
            [weight_decay_factor] "mr" (WeightDecays[index]),

            [inc] "i" (CACHELINE_SIZE_BYTES),

            [end] "mr" (bufferLen * sizeof(float))
            : "cc", "memory",
            "xmm0", "xmm1", "xmm2", "xmm3",
            "xmm4", "xmm5", "xmm6", "xmm7",
            "xmm8", "xmm9", "xmm10", "xmm11",
            "xmm12", "xmm13", "xmm14", "xmm15");

    }
};


class SGDNTTOptimizer : public Optimizer
{
public:
    SGDNTTOptimizer(size_t numMachines, std::unordered_map<int, int>& keySizes, const size_t prefetch_distance, Verbs* verbs)
        : Optimizer(numMachines, keySizes, prefetch_distance, verbs)
    {
        this->Name = "sgdntnt";
    }

    //index, weights and gradients (aggregated) 
    virtual void Update(size_t index, float* weightBuffer, float* gradBuffer, size_t bufferLen) override
    {
        CHECK(index < States.size());
        CHECK(bufferLen * sizeof(float) == StateLengths[index]);

        size_t offset = 0;
        size_t prefetch_offset = prefetch_distance;

        float * momBuffer = States[index];

        __asm__ __volatile__("prefetchnta 0x000(%[grad],%[offset],1) \n\t"
            "prefetchnta 0x040(%[grad],%[offset],1) \n\t"
            "prefetchnta 0x080(%[grad],%[offset],1) \n\t"
            "prefetchnta 0x0C0(%[grad],%[offset],1) \n\t"
            "prefetchnta 0x100(%[grad],%[offset],1) \n\t"
            "prefetchnta 0x140(%[grad],%[offset],1) \n\t"
            "prefetchnta 0x180(%[grad],%[offset],1) \n\t"
            "prefetchnta 0x1C0(%[grad],%[offset],1) \n\t"
            "prefetchnta 0x200(%[grad],%[offset],1) \n\t"

            ".align 32 \n\t"
            "1: \n\t"

            // load gradient
            "movaps      0x00(%[grad],%[offset],1),%%xmm0 \n\t"
            "movaps      0x10(%[grad],%[offset],1),%%xmm1 \n\t"
            "movaps      0x20(%[grad],%[offset],1),%%xmm2 \n\t"
            "movaps      0x30(%[grad],%[offset],1),%%xmm3 \n\t"
            "prefetchnta 0x00(%[grad],%[prefetch_offset],1) \n\t"

            // multiply gradient by learning rate
            "mulps %[learning_rate],%%xmm0  \n\t"
            "mulps %[learning_rate],%%xmm1  \n\t"
            "mulps %[learning_rate],%%xmm2  \n\t"
            "mulps %[learning_rate],%%xmm3  \n\t"

            // subtract from weight
            "addps       0x00(%[weight],%[offset],1),%%xmm0   \n\t"
            "addps       0x10(%[weight],%[offset],1),%%xmm1   \n\t"
            "addps       0x20(%[weight],%[offset],1),%%xmm2   \n\t"
            "addps       0x30(%[weight],%[offset],1),%%xmm3   \n\t"

            // store
            "movaps %%xmm0,0x00(%[weight],%[offset],1)      \n\t"
            "movaps %%xmm1,0x10(%[weight],%[offset],1)      \n\t"
            "movaps %%xmm2,0x20(%[weight],%[offset],1)      \n\t"
            "movaps %%xmm3,0x30(%[weight],%[offset],1)      \n\t"

            // increment, check if done, and loop
            "add %[inc],%[prefetch_offset] \n\t"
            "add %[inc],%[offset] \n\t"
            "cmp %[end],%[offset] \n\t"
            "jb 1b \n\t"

            "4:"
            "sfence \n\t"

            : [offset] "+r" (offset),
            [prefetch_offset] "+r" (prefetch_offset)
            : [grad]   "r" (gradBuffer),
            [mom]    "r" (momBuffer),
            [weight] "r" (weightBuffer),

            [learning_rate] "mr" (NegationOfLearningRates[index]),
            [momentum_factor] "mr" (MomentumConstant),
            [scaling_factor] "mr" (GradRescaling),
            [weight_decay_factor] "mr" (WeightDecays[index]),

            [inc] "i" (CACHELINE_SIZE_BYTES),

            [end] "mr" (bufferLen * sizeof(float))
            : "cc", "memory",
            "xmm0", "xmm1", "xmm2", "xmm3",
            "xmm4", "xmm5", "xmm6", "xmm7",
            "xmm8", "xmm9", "xmm10", "xmm11",
            "xmm12", "xmm13", "xmm14", "xmm15");

    }
};


class SGDTNTOptimizer : public Optimizer
{
public:
    SGDTNTOptimizer(size_t numMachines, std::unordered_map<int, int>& keySizes, const size_t prefetch_distance, Verbs* verbs)
        : Optimizer(numMachines, keySizes, prefetch_distance, verbs)
    {
        this->Name = "sgdtnt";
    }

    //index, weights and gradients (aggregated) 
    virtual void Update(size_t index, float* weightBuffer, float* gradBuffer, size_t bufferLen) override
    {
        CHECK(index < States.size());
        CHECK(bufferLen * sizeof(float) == StateLengths[index]);

        size_t offset = 0;
        size_t prefetch_offset = prefetch_distance;

        float * momBuffer = States[index];

        __asm__ __volatile__(".align 32 \n\t"
            "1: \n\t"

            // load gradient
            "movaps     0x00(%[grad],%[offset],1),%%xmm0 \n\t"
            "movaps     0x10(%[grad],%[offset],1),%%xmm1 \n\t"
            "movaps     0x20(%[grad],%[offset],1),%%xmm2 \n\t"
            "movaps     0x30(%[grad],%[offset],1),%%xmm3 \n\t"

            // multiply gradient by learning rate
            "mulps %[learning_rate],%%xmm0  \n\t"
            "mulps %[learning_rate],%%xmm1  \n\t"
            "mulps %[learning_rate],%%xmm2  \n\t"
            "mulps %[learning_rate],%%xmm3  \n\t"

            // subtract from weight
            "addps      0x00(%[weight],%[offset],1),%%xmm0   \n\t"
            "addps      0x10(%[weight],%[offset],1),%%xmm1   \n\t"
            "addps      0x20(%[weight],%[offset],1),%%xmm2   \n\t"
            "addps      0x30(%[weight],%[offset],1),%%xmm3   \n\t"

            // store
            "movntps %%xmm0,0x00(%[weight],%[offset],1)      \n\t"
            "movntps %%xmm1,0x10(%[weight],%[offset],1)      \n\t"
            "movntps %%xmm2,0x20(%[weight],%[offset],1)      \n\t"
            "movntps %%xmm3,0x30(%[weight],%[offset],1)      \n\t"

            // increment, check if done, and loop
            "add %[inc],%[offset] \n\t"
            "cmp %[end],%[offset] \n\t"
            "jb 1b \n\t"

            "4:"
            "sfence \n\t"

            : [offset] "+r" (offset)//,
              //[prefetch_offset] "+r" (prefetch_offset)
            : [grad]   "r" (gradBuffer),
            [mom]    "r" (momBuffer),
            [weight] "r" (weightBuffer),

            [learning_rate] "mr" (NegationOfLearningRates[index]),
            [momentum_factor] "mr" (MomentumConstant),
            [scaling_factor] "mr" (GradRescaling),
            [weight_decay_factor] "mr" (WeightDecays[index]),

            [inc] "i" (CACHELINE_SIZE_BYTES),

            [end] "mr" (bufferLen * sizeof(float))
            : "cc", "memory",
            "xmm0", "xmm1", "xmm2", "xmm3",
            "xmm4", "xmm5", "xmm6", "xmm7",
            "xmm8", "xmm9", "xmm10", "xmm11",
            "xmm12", "xmm13", "xmm14", "xmm15");

    }
};

class SGDTTOptimizer : public Optimizer
{
public:
    SGDTTOptimizer(size_t numMachines, std::unordered_map<int, int>& keySizes, const size_t prefetch_distance, Verbs* verbs)
        : Optimizer(numMachines, keySizes, prefetch_distance, verbs)
    {
        this->Name = "sgdTT";
    }

    //index, weights and gradients (aggregated) 
    virtual void Update(size_t index, float* weightBuffer, float* gradBuffer, size_t bufferLen) override
    {
        CHECK(index < States.size());
        CHECK(bufferLen * sizeof(float) == StateLengths[index]);

        size_t offset = 0;
        size_t prefetch_offset = prefetch_distance;

        float * momBuffer = States[index];

        __asm__ __volatile__(".align 32 \n\t"
            "1: \n\t"

            // load gradient
            "movaps     0x00(%[grad],%[offset],1),%%xmm0 \n\t"
            "movaps     0x10(%[grad],%[offset],1),%%xmm1 \n\t"
            "movaps     0x20(%[grad],%[offset],1),%%xmm2 \n\t"
            "movaps     0x30(%[grad],%[offset],1),%%xmm3 \n\t"

            // multiply gradient by learning rate
            "mulps %[learning_rate],%%xmm0  \n\t"
            "mulps %[learning_rate],%%xmm1  \n\t"
            "mulps %[learning_rate],%%xmm2  \n\t"
            "mulps %[learning_rate],%%xmm3  \n\t"

            // subtract from weight
            "addps      0x00(%[weight],%[offset],1),%%xmm0   \n\t"
            "addps      0x10(%[weight],%[offset],1),%%xmm1   \n\t"
            "addps      0x20(%[weight],%[offset],1),%%xmm2   \n\t"
            "addps      0x30(%[weight],%[offset],1),%%xmm3   \n\t"

            // store
            "movaps %%xmm0,0x00(%[weight],%[offset],1)      \n\t"
            "movaps %%xmm1,0x10(%[weight],%[offset],1)      \n\t"
            "movaps %%xmm2,0x20(%[weight],%[offset],1)      \n\t"
            "movaps %%xmm3,0x30(%[weight],%[offset],1)      \n\t"

            // increment, check if done, and loop
            "add %[inc],%[offset] \n\t"
            "cmp %[end],%[offset] \n\t"
            "jb 1b \n\t"

            "4:"
            "sfence \n\t"

            : [offset] "+r" (offset)//,
              //[prefetch_offset] "+r" (prefetch_offset)
            : [grad]   "r" (gradBuffer),
            [mom]    "r" (momBuffer),
            [weight] "r" (weightBuffer),

            [learning_rate] "mr" (NegationOfLearningRates[index]),
            [momentum_factor] "mr" (MomentumConstant),
            [scaling_factor] "mr" (GradRescaling),
            [weight_decay_factor] "mr" (WeightDecays[index]),

            [inc] "i" (CACHELINE_SIZE_BYTES),

            [end] "mr" (bufferLen * sizeof(float))
            : "cc", "memory",
            "xmm0", "xmm1", "xmm2", "xmm3",
            "xmm4", "xmm5", "xmm6", "xmm7",
            "xmm8", "xmm9", "xmm10", "xmm11",
            "xmm12", "xmm13", "xmm14", "xmm15");

    }
};

inline Optimizer* Optimizer::Create(std::string name, size_t numMachines, std::unordered_map<int, int>& keySizes, const size_t prefetch_distance, Verbs* verbs)
{
    for (auto & c : name) c = tolower(c);
    if (name == "") // default
    {
        return new NAGNTNTOptimizer(numMachines, keySizes, prefetch_distance, verbs);
    }
    else if (name == "sgdtt")
    {
        return new SGDTTOptimizer(numMachines, keySizes, prefetch_distance, verbs);
    }
    else if (name == "sgdtnt")
    {
        return new SGDNTTOptimizer(numMachines, keySizes, prefetch_distance, verbs);
    }
    else if (name == "sgdntt")
    {
        return new SGDTNTOptimizer(numMachines, keySizes, prefetch_distance, verbs);
    }
    else if (name == "sgdntnt")
    {
        return new SGDTNTOptimizer(numMachines, keySizes, prefetch_distance, verbs);
    }
    else if (name == "nagtt")
    {
        return new NAGTTOptimizer(numMachines, keySizes, prefetch_distance, verbs);
    }
    else if (name == "nagtnt")
    {
        return new NAGNTTOptimizer(numMachines, keySizes, prefetch_distance, verbs);
    }
    else if (name == "nagntt")
    {
        return new NAGTNTOptimizer(numMachines, keySizes, prefetch_distance, verbs);
    }
    else if (name == "nagntnt")
    {
        return new NAGTNTOptimizer(numMachines, keySizes, prefetch_distance, verbs);
    }
    else
    {
        assert(false);
    }
}














class Aggregator;

class NTNTAggregator;
class TNTAggregator;
class NTTAggregator;
class TTAggregator;

class Aggregator
{
public:
    static Aggregator* Create(std::string name, const size_t prefetch_dist);

    //add vector 2 (src) to vector 1 (dst)
    virtual void VectorVectorAdd(float* dst, size_t len, float* src)
    {
        assert((len & INSTRUCTION_VECTOR_SIZE_MASK) == 0);
        for (size_t m = 0; m < len; m += INSTRUCTION_VECTOR_SIZE) {
            _mm_store_ps(dst + m, _mm_add_ps(_mm_load_ps(dst + m), _mm_load_ps(src + m)));
        }
    }

protected:
    const size_t prefetch_distance;

    Aggregator(const size_t prefetch_distance = 0x240)
        : prefetch_distance(prefetch_distance)
    { }

    virtual ~Aggregator() { }
};

class TTAggregator : public Aggregator
{
public:
    TTAggregator(const size_t prefetch_distance = 0x240)
        : Aggregator(prefetch_distance)
    { }

    virtual void VectorVectorAdd(float* dst, size_t len, float* src)
    {
        assert((len & INSTRUCTION_VECTOR_SIZE_MASK) == 0);
        size_t offset = 0;
        size_t prefetch_offset = prefetch_distance;
        __asm__ __volatile__(".align 32 \n\t"
            "1: \n\t"

            "movaps 0x00(%[dst],%[offset],1),%%xmm0 \n\t"
            "movaps 0x10(%[dst],%[offset],1),%%xmm1 \n\t"
            "movaps 0x20(%[dst],%[offset],1),%%xmm2 \n\t"
            "movaps 0x30(%[dst],%[offset],1),%%xmm3 \n\t"

            "addps  0x00(%[src],%[offset],1),%%xmm0 \n\t"
            "addps  0x10(%[src],%[offset],1),%%xmm1 \n\t"
            "addps  0x20(%[src],%[offset],1),%%xmm2 \n\t"
            "addps  0x30(%[src],%[offset],1),%%xmm3 \n\t"

            "movaps  %%xmm0,0x00(%[dst],%[offset],1) \n\t"
            "movaps  %%xmm1,0x10(%[dst],%[offset],1) \n\t"
            "movaps  %%xmm2,0x20(%[dst],%[offset],1) \n\t"
            "movaps  %%xmm3,0x30(%[dst],%[offset],1) \n\t"

            "add %[inc],%[offset] \n\t"

            "cmp %[end], %[offset] \n\t"
            "jb 1b \n\t"

            "sfence \n\t"

            : [offset] "+mr" (offset),
            [prefetch_offset] "+mr" (prefetch_offset)
            : [dst] "c" (dst), // Use rcx to keep insns small
            [src] "D" (src), // Use rdi to keep insns small
            [inc] "i" (CACHELINE_SIZE_BYTES),
            [end] "mr" (len * sizeof(float))
            : "cc", "memory",
            "xmm0", "xmm1", "xmm2", "xmm3",
            "xmm4", "xmm5", "xmm6", "xmm7");
    }
};

class TNTAggregator : public Aggregator
{
public:
    TNTAggregator(const size_t prefetch_distance = 0x240)
        : Aggregator(prefetch_distance)
    { }

    virtual void VectorVectorAdd(float* dst, size_t len, float* src)
    {
        assert((len & INSTRUCTION_VECTOR_SIZE_MASK) == 0);
        size_t offset = 0;
        size_t prefetch_offset = prefetch_distance;
        __asm__ __volatile__(".align 32 \n\t"
            "1: \n\t"

            "movaps 0x00(%[dst],%[offset],1),%%xmm0 \n\t"
            "movaps 0x10(%[dst],%[offset],1),%%xmm1 \n\t"
            "movaps 0x20(%[dst],%[offset],1),%%xmm2 \n\t"
            "movaps 0x30(%[dst],%[offset],1),%%xmm3 \n\t"

            "addps  0x00(%[src],%[offset],1),%%xmm0 \n\t"
            "addps  0x10(%[src],%[offset],1),%%xmm1 \n\t"
            "addps  0x20(%[src],%[offset],1),%%xmm2 \n\t"
            "addps  0x30(%[src],%[offset],1),%%xmm3 \n\t"

            "movntps  %%xmm0,0x00(%[dst],%[offset],1) \n\t"
            "movntps  %%xmm1,0x10(%[dst],%[offset],1) \n\t"
            "movntps  %%xmm2,0x20(%[dst],%[offset],1) \n\t"
            "movntps  %%xmm3,0x30(%[dst],%[offset],1) \n\t"

            "add %[inc],%[prefetch_offset] \n\t"
            "add %[inc],%[offset] \n\t"

            "cmp %[end], %[offset] \n\t"
            "jb 1b \n\t"

            "sfence \n\t"

            : [offset] "+mr" (offset),
            [prefetch_offset] "+mr" (prefetch_offset)
            : [dst] "c" (dst), // Use rcx to keep insns small
            [src] "D" (src), // Use rdi to keep insns small
            [inc] "i" (CACHELINE_SIZE_BYTES),
            [end] "mr" (len * sizeof(float))
            : "cc", "memory",
            "xmm0", "xmm1", "xmm2", "xmm3",
            "xmm4", "xmm5", "xmm6", "xmm7");
    }
};

class NTTAggregator : public Aggregator
{
public:
    NTTAggregator(const size_t prefetch_distance = 0x240)
        : Aggregator(prefetch_distance)
    { }

    virtual void VectorVectorAdd(float* dst, size_t len, float* src)
    {
        assert((len & INSTRUCTION_VECTOR_SIZE_MASK) == 0);
        size_t offset = 0;
        size_t prefetch_offset = prefetch_distance;
        __asm__ __volatile__("prefetchnta 0x000(%[src],%[offset],1) \n\t"
            "prefetchnta 0x040(%[src],%[offset],1) \n\t"
            "prefetchnta 0x080(%[src],%[offset],1) \n\t"
            "prefetchnta 0x0C0(%[src],%[offset],1) \n\t"
            "prefetchnta 0x100(%[src],%[offset],1) \n\t"
            "prefetchnta 0x140(%[src],%[offset],1) \n\t"
            "prefetchnta 0x180(%[src],%[offset],1) \n\t"
            "prefetchnta 0x1C0(%[src],%[offset],1) \n\t"
            "prefetchnta 0x200(%[src],%[offset],1) \n\t"

            ".align 32 \n\t"
            "1: \n\t"

            "movaps 0x00(%[dst],%[offset],1),%%xmm0 \n\t"
            "movaps 0x10(%[dst],%[offset],1),%%xmm1 \n\t"
            "movaps 0x20(%[dst],%[offset],1),%%xmm2 \n\t"
            "movaps 0x30(%[dst],%[offset],1),%%xmm3 \n\t"

            "addps  0x00(%[src],%[offset],1),%%xmm0 \n\t"
            "addps  0x10(%[src],%[offset],1),%%xmm1 \n\t"
            "addps  0x20(%[src],%[offset],1),%%xmm2 \n\t"
            "addps  0x30(%[src],%[offset],1),%%xmm3 \n\t"
            "prefetchnta 0x00(%[src],%[prefetch_offset],1) \n\t"

            "movaps  %%xmm0,0x00(%[dst],%[offset],1) \n\t"
            "movaps  %%xmm1,0x10(%[dst],%[offset],1) \n\t"
            "movaps  %%xmm2,0x20(%[dst],%[offset],1) \n\t"
            "movaps  %%xmm3,0x30(%[dst],%[offset],1) \n\t"

            "add %[inc],%[prefetch_offset] \n\t"
            "add %[inc],%[offset] \n\t"

            "cmp %[end], %[offset] \n\t"
            "jb 1b \n\t"

            "sfence \n\t"

            : [offset] "+mr" (offset),
            [prefetch_offset] "+mr" (prefetch_offset)
            : [dst] "c" (dst), // Use rcx to keep insns small
            [src] "D" (src), // Use rdi to keep insns small
            [inc] "i" (CACHELINE_SIZE_BYTES),
            [end] "mr" (len * sizeof(float))
            : "cc", "memory",
            "xmm0", "xmm1", "xmm2", "xmm3",
            "xmm4", "xmm5", "xmm6", "xmm7");
    }
};

class NTNTAggregator : public Aggregator
{
public:
    NTNTAggregator(const size_t prefetch_distance = 0x240)
        : Aggregator(prefetch_distance)
    { }

    virtual void VectorVectorAdd(float* dst, size_t len, float* src)
    {
        assert((len & INSTRUCTION_VECTOR_SIZE_MASK) == 0);
        size_t offset = 0;
        size_t prefetch_offset = prefetch_distance;
        __asm__ __volatile__("prefetchnta 0x000(%[dst],%[offset],1) \n\t"
            "prefetchnta 0x040(%[dst],%[offset],1) \n\t"
            "prefetchnta 0x080(%[dst],%[offset],1) \n\t"
            "prefetchnta 0x0C0(%[dst],%[offset],1) \n\t"
            "prefetchnta 0x100(%[dst],%[offset],1) \n\t"

            "prefetchnta 0x000(%[src],%[offset],1) \n\t"
            "prefetchnta 0x040(%[src],%[offset],1) \n\t"
            "prefetchnta 0x080(%[src],%[offset],1) \n\t"
            "prefetchnta 0x0C0(%[src],%[offset],1) \n\t"
            "prefetchnta 0x100(%[src],%[offset],1) \n\t"

            ".align 32 \n\t"
            "1: \n\t"

            "movaps 0x00(%[dst],%[offset],1),%%xmm0 \n\t"
            "movaps 0x10(%[dst],%[offset],1),%%xmm1 \n\t"
            "movaps 0x20(%[dst],%[offset],1),%%xmm2 \n\t"
            "movaps 0x30(%[dst],%[offset],1),%%xmm3 \n\t"
            "prefetchnta 0x00(%[dst],%[prefetch_offset],1) \n\t"

            "addps  0x00(%[src],%[offset],1),%%xmm0 \n\t"
            "addps  0x10(%[src],%[offset],1),%%xmm1 \n\t"
            "addps  0x20(%[src],%[offset],1),%%xmm2 \n\t"
            "addps  0x30(%[src],%[offset],1),%%xmm3 \n\t"
            "prefetchnta 0x00(%[src],%[prefetch_offset],1) \n\t"

            "movntps  %%xmm0,0x00(%[dst],%[offset],1) \n\t"
            "movntps  %%xmm1,0x10(%[dst],%[offset],1) \n\t"
            "movntps  %%xmm2,0x20(%[dst],%[offset],1) \n\t"
            "movntps  %%xmm3,0x30(%[dst],%[offset],1) \n\t"

            "add %[inc],%[prefetch_offset] \n\t"
            "add %[inc],%[offset] \n\t"

            "cmp %[end], %[offset] \n\t"
            "jb 1b \n\t"

            "sfence \n\t"

            : [offset] "+mr" (offset),
            [prefetch_offset] "+mr" (prefetch_offset)
            : [dst] "c" (dst), // Use rcx to keep insns small
            [src] "D" (src), // Use rdi to keep insns small
            [inc] "i" (CACHELINE_SIZE_BYTES),
            [end] "mr" (len * sizeof(float))
            : "cc", "memory",
            "xmm0", "xmm1", "xmm2", "xmm3",
            "xmm4", "xmm5", "xmm6", "xmm7");
    }
};


inline Aggregator* Aggregator::Create(std::string name, const size_t prefetch_dist = 0x240)
{
    for (auto & c : name) c = tolower(c);
    if (name == "")
    {
        return new NTNTAggregator(prefetch_dist);
    }
    else if (name == "tt")
    {
        return new TTAggregator();
    }
    else if (name == "tnt")
    {
        return new TNTAggregator();
    }
    else if (name == "ntt")
    {
        return new NTTAggregator();
    }
    else if (name == "ntnt")
    {
        return new NTNTAggregator();
    }
    else
    {
        assert(false);
    }
}
