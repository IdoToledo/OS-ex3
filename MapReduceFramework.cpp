#include <iostream>
#include "MapReduceFramework.h"
#include <atomic>

#define SYS_ERR "system error: "

typedef struct
{
    std::atomic<uint64_t>* data; // stage, work_did, work_all
    int multiThreadLevel;
    std::atomic<int>* assignInput;
    pthread_t* threads;

}JobContext;


void getJobState(JobHandle job, JobState* state) //TODO add semaphore?
{
    // lamaaaaaaaa
    return;
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel)
{
    // Initialize - Undefined
    std::atomic<uint64_t> data(0);
    std::atomic<int> assign_input(0);
    auto* threads = static_cast<pthread_t *>(malloc(sizeof(pthread_t) * multiThreadLevel));
    auto* contexts = static_cast<JobContext*>(malloc(sizeof(JobContext) * multiThreadLevel));
//    JobContext jobHandler = (JobContext) {client, inputVec, outputVec,
//                                          &data, multiThreadLevel,
//                                          &assign_input, threads};
    JobState job_state = (JobState) {MAP_STAGE, 0};


    // Make threads
//    getJobState(&jobHandler, &job_state);
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        auto a = (KChar)i;
        auto b = (VCount)i;
        auto d = IntermediateVec();
        d.push_back(IntermediatePair(&a, &b));
        JobContext jobHandler = (JobContext) {&client, &inputVec, outputVec,
                                              d,
                                          &data, multiThreadLevel,
                                          &assign_input, threads};

        contexts[i] = jobHandler;
    }

    for (int i = 0; i < multiThreadLevel; ++i)
    {
        pthread_create(threads + i, nullptr, thread_func, (void*)(contexts+i));
    }

    // Map Phase -

//    client.map(K1, V1, void*);

}

void emit2 (K2* key, V2* value, void* context);
void emit3 (K3* key, V3* value, void* context);



void waitForJob(JobHandle job);
void closeJobHandle(JobHandle job);

//int main() {
//    std::cout << "Hello, World!" << std::endl;
//    return 0;
//}
