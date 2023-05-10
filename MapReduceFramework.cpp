#include <iostream>
#include "MapReduceFramework.h"
#include <atomic>
#include <algorithm>
#include "Barrier.h"


#define SYS_ERR "system error: "
#define JOB_SIZE 4611686016279904256UL
#define JOB_STATE 13835058055282163712UL
#define JOB_PROGRESS 2147483647UL



class JobContext {

public:
    const MapReduceClient* client;
    const InputVec* inputVec;
    OutputVec* outputVec;
    Barrier* barrier;
    IntermediateVec intermediateVec;
    std::atomic<uint64_t>* data; // stage, work_did, work_all
    int multiThreadLevel;
    std::atomic<int>* assignInput;
    pthread_t* threads;
    int thread_id; // TODO delete
};


void updateJobSize(std::atomic<uint64_t>* data, unsigned long size)
{
    uint64_t num1 = 0 | (size << 31);
    (*(data)) = (data->load() & ~(JOB_SIZE)) | (num1);
}

void updateJobProgress(std::atomic<uint64_t>* data)
{
    (*(data))++;
}
void setJobState(JobHandle job, stage_t stage)
{
    // Change state and make everything else 0
    std::atomic<uint64_t>*data = ((JobContext*) job)->data;
    (*(data)) = ((static_cast<unsigned long>(stage) << 62));
}

void getJobState(JobHandle job, JobState* state)
{
    // Change state and change percentage
    auto* jc = static_cast<JobContext*>(job);
    const unsigned long cur_data = jc->data->load();
    state->stage = (stage_t) ((cur_data & JOB_STATE) >> 62);
    state->percentage = 100*((float)(cur_data & JOB_PROGRESS)
            / (float)((cur_data & JOB_SIZE) >> 31));
}

void mapPhase(void* context)
{
    // Map Phase -- Critical Section
    auto* jc = static_cast<JobContext*>(context);
    unsigned long inputVecSize = (*(jc->inputVec)).size();
    while (*(jc->assignInput) < inputVecSize)
    {
        int old_value = (*(jc->assignInput))++; // old value will always be unique
        if (old_value < inputVecSize) // in case old_value bigger than inputVecSize
        {
            // Give a map job to a thread, using old_value
            jc->client->map((*(jc->inputVec))[old_value].first,
                            (*(jc->inputVec))[old_value].second, jc);
            (*(jc->data))++;
        }
    }
}

bool compareIntermediatePairs(const IntermediatePair& pair1,
                              const IntermediatePair& pair2)
{
  return (*(pair1.first)) < (*(pair2.first));
}

void sortPhase(void* context) // TODO first letter is not sorted
{
    auto* jc = static_cast<JobContext*>(context);
    std::sort(jc->intermediateVec.begin(),jc->intermediateVec.end(),
              compareIntermediatePairs);
}

void shufflePhase(void* context)
{
    auto* jc = static_cast<JobContext*>(context);
    jc->barrier->barrier(); // Wait for everyone - so all threads are organized
    // Mutex - Shuffle


}

void* threadEntryPoint(void* context) {
    auto* jc = static_cast<JobContext*>(context);
    mapPhase(context);
    sortPhase(context);
    jc->barrier->barrier();

    // Only one thread can enter this block at a time
    pthread_mutex_lock(jc->shuffle_mutex);
    if (!(*(jc->shuffled))) {
        std::cout << "Certain Job: Thread ID - " << jc->thread_id << std::endl;
        fflush(stdout);
        // Perform shuffle
        shufflePhase(context);
        (*(jc->shuffled)) = true;
        pthread_mutex_unlock(jc->shuffle_mutex);
        pthread_mutex_destroy(jc->shuffle_mutex); // TODO can cause bug?
    }
    reducePhase(context);


    pthread_exit(NULL);
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel)
{
    // Initialize - Undefined
    auto* data = static_cast<std::atomic<uint64_t> *>(malloc(sizeof(std::atomic<uint64_t>))); // state (2), work_did (31), work_all (31)
    *data = 0;
    auto* assign_input = static_cast<std::atomic<int> *>(malloc(sizeof(std::atomic<int>)));
    *assign_input = 0;
    auto* threads = static_cast<pthread_t*>(malloc(sizeof(pthread_t) * multiThreadLevel));
    auto* contexts = static_cast<JobContext*>(malloc(sizeof(JobContext) * multiThreadLevel));
    auto* barrier =(Barrier*) malloc(sizeof(Barrier));
    *barrier = Barrier(multiThreadLevel);
    // TODO do we need to create min(multiThreadLevel, inputVecSize)?
    // Make threads
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        contexts[i] = ((JobContext) {&client, &inputVec, &outputVec, barrier,
                                     std::move(IntermediateVec()), data,
                                     multiThreadLevel,assign_input,
                                     threads, i});
    }

    // Beginning Map Phase
    setJobState((void*)contexts, MAP_STAGE);
    updateJobSize(contexts[0].data, inputVec.size());

    for (int i = 0; i < multiThreadLevel; ++i)
    {
      pthread_create (
          threads + i, nullptr, threadEntryPoint, (void *) (contexts + i));
    }
    return (void*) contexts;
}

void emit2 (K2* key, V2* value, void* context)
{
    auto* jc = static_cast<JobContext*>(context);
    jc->intermediateVec.emplace_back(key,value); // Saves the intermediary element in the context data structures
     // update the number of intermediary elements using atomic counter
}
void emit3 (K3* key, V3* value, void* context)
{

}


void waitForJob(JobHandle job)
{

}
void closeJobHandle(JobHandle job)
{
    auto* jc = static_cast<JobContext*>(job);
    for (int i = 0; i < jc->multiThreadLevel; ++i) {
        pthread_join((jc->threads)[i], NULL);
    }
}

