#include <iostream>
#include "MapReduceFramework.h"
#include <atomic>

#define SYS_ERR "system error: "

typedef struct
{
    std::atomic<uint64_t>* data; // stage, work_did, work_all
    int multiThreadLevel;
    std::atomic<long>* assign_input;
}JobContext;


void getJobState(JobHandle job, JobState* state)
{
    // lamaaaaaaaa
    return;
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel)
{
    // Initialize - Undefined
    JobState job_manager = (JobState) {UNDEFINED_STAGE, 0};
    std::atomic<uint64_t> data(0);
    std::atomic<long> assign_input(0);
    JobContext jobHandler = (JobContext) {&data, multiThreadLevel, &assign_input};

    // Map Phase -
    getJobState((static_cast<void*> (&jobHandler)), (JobState) {MAP_STAGE, 0});


}

void emit2 (K2* key, V2* value, void* context);
void emit3 (K3* key, V3* value, void* context);



void waitForJob(JobHandle job);
void getJobState(JobHandle job, JobState* state);
void closeJobHandle(JobHandle job);

//int main() {
//    std::cout << "Hello, World!" << std::endl;
//    return 0;
//}
