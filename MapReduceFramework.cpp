#include <iostream>
#include "MapReduceFramework.h"
#include <atomic>
#include <algorithm>
#include "Barrier.h"
#include <memory>
#include <mutex>

#define SYS_ERR "system error: "
#define JOB_SIZE 4611686016279904256UL
#define JOB_STATE 13835058055282163712UL
#define JOB_PROGRESS 2147483647UL


typedef std::vector<std::vector<IntermediatePair>> ShuffleVec;

class SharedJobContext;

class JobContext {

public:
    // Don't Delete!
    const MapReduceClient* client;
    const InputVec* inputVec;
    OutputVec* outputVec;

    // Shared pointers - and dont delete here
    SharedJobContext* sjc;

    // Unique data members
    IntermediateVec intermediateVec;
    int multiThreadLevel;

//    JobContext(const MapReduceClient* client,
//               const InputVec* inputVec,
//               OutputVec* outputVec) : client(client), inputVec(inputVec),
//                                       outputVec(outputVec)
//               {
//
//               }
//    ~JobContext()
//    {
//
//    }
};


class SharedJobContext
        {
        public:
    explicit SharedJobContext(int multiThreadLevel)
    {
        data = static_cast<std::atomic<uint64_t> *>(malloc(sizeof(std::atomic<uint64_t>))); // state (2), work_did (31), work_all (31)
        *data = 0;
        assignInput = static_cast<std::atomic<int> *>(malloc(sizeof(std::atomic<int>)));
        *assignInput = 0;
        threads = new pthread_t[multiThreadLevel];
         threads_context = new JobContext[multiThreadLevel];
//        threads_context = new JobContext[multiThreadLevel];
        barrier = new Barrier(multiThreadLevel);
        shuffleVec = new ShuffleVec();
        shuffled = false;
        waiting = false;
    }

            // Shared pointers
            Barrier* barrier;
            std::atomic<uint64_t>* data; // stage, work_done, total_work
            std::atomic<int>* assignInput;
            pthread_t* threads;
            JobContext* threads_context;
            pthread_mutex_t shuffle_mutex;
            bool shuffled;
            ShuffleVec* shuffleVec;
            pthread_mutex_t reduce_mutex;
            pthread_mutex_t output_mutex;
            bool waiting;

            ~SharedJobContext()
        {
            delete barrier;
            delete data;
            delete assignInput;
            delete[] threads;
            delete[] threads_context;
            if (pthread_mutex_destroy(&shuffle_mutex) != 0) {
                fprintf(stderr, "system error: [[SharedJobContext]] error on pthread_mutex_destroy\n");
                exit(1);
            }
            delete shuffleVec;
            if (pthread_mutex_destroy(&reduce_mutex) != 0) {
                fprintf(stderr, "system error: [[SharedJobContext]] error on pthread_mutex_destroy\n");
                exit(1);
            }
            if (pthread_mutex_destroy(&output_mutex) != 0) {
                fprintf(stderr, "system error: [[SharedJobContext]] error on pthread_mutex_destroy\n");
                exit(1);
            }
        }

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
    std::atomic<uint64_t>*data = (((JobContext*) job)->sjc)->data;
    (*(data)) = ((static_cast<unsigned long>(stage) << 62));
}

void getJobState(JobHandle job, JobState* state)
{
    // Change state and change percentage
    auto* jc = static_cast<JobContext*>(job);
    const unsigned long cur_data = jc->sjc->data->load();
    state->stage = (stage_t) ((cur_data & JOB_STATE) >> 62);
    state->percentage = 100*((float)(cur_data & JOB_PROGRESS)
            / (float)((cur_data & JOB_SIZE) >> 31));
}

void mapPhase(void* context)
{
    // Map Phase -- Critical Section
    auto* jc = static_cast<JobContext*>(context);
    unsigned long inputVecSize = (*(jc->inputVec)).size();
    while (*(jc->sjc->assignInput) < inputVecSize)
    {
        int old_value = (*(jc->sjc->assignInput))++; // old value will always be unique
        if (old_value < inputVecSize) // in case old_value bigger than inputVecSize
        {
            // Give a map job to a thread, using old_value
            jc->client->map((*(jc->inputVec))[old_value].first,
                            (*(jc->inputVec))[old_value].second, jc);
            (*(jc->sjc->data))++;
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

void reducePhase(void* context)
{
    auto* jc = static_cast<JobContext*>(context);
    IntermediateVec working_vec;
    while (!jc->sjc->shuffleVec->empty())
    {
        // Popping the last vec
        pthread_mutex_lock(&(jc->sjc->reduce_mutex));
        if (jc->sjc->shuffleVec->empty()) // Just one at a time so ok
        {
            return;
        }
        working_vec = jc->sjc->shuffleVec->back();
        jc->sjc->shuffleVec->pop_back();
        pthread_mutex_unlock(&(jc->sjc->reduce_mutex));

        jc->client->reduce(&working_vec, context); // Calls emit3

    }

}


void findInterPair(JobContext* jc, IntermediatePair& interPair, int& thread)
{
    for(int j = 0; j < jc->multiThreadLevel; j++)
    {
        if (!(jc->sjc->threads_context[j]).intermediateVec.empty())
        {
            interPair = (jc->sjc->threads_context[j]).intermediateVec[(jc->sjc->threads_context[j]).intermediateVec.size()-1];
            thread = j;
            return;
        }
    }
}

void findBiggestPair(JobContext* jc, IntermediatePair& biggest_pair, int& biggest_thread)
{
    for(int j = 0; j < jc->multiThreadLevel; j++)
    {
        if (!(jc->sjc->threads_context[j]).intermediateVec.empty())
        {
            IntermediatePair cur_pair = (jc->sjc->threads_context[j]).intermediateVec[
                    (jc->sjc->threads_context[j]).intermediateVec.size() - 1];

            if (compareIntermediatePairs(biggest_pair, cur_pair)) {
                biggest_pair = cur_pair;
                biggest_thread = j;
            }
        }
    }
}

void addToShuffleVec(JobContext* jc, IntermediatePair& biggest_pair, int& num_of_vec)
{
    if (jc->sjc->shuffleVec->empty() || compareIntermediatePairs(biggest_pair, jc->sjc->shuffleVec->back().back()))
    {
        jc->sjc->shuffleVec->push_back(IntermediateVec({biggest_pair}));
        num_of_vec++;
    }
    else
    {
        jc->sjc->shuffleVec->back().push_back(biggest_pair);
    }
}


void shufflePhase(void* context)
{
    auto* jc = static_cast<JobContext*>(context);
    setJobState(context, SHUFFLE_STAGE);
    unsigned int num_of_intermediate_pairs = 0;
    for (int i = 0; i < jc->multiThreadLevel; i++) // Count the amount of intermediate pairs
    {
        num_of_intermediate_pairs += (jc->sjc->threads_context[i]).intermediateVec.size();
    }
    updateJobSize(jc->sjc->data, num_of_intermediate_pairs);

    int num_of_vec = 0;
    IntermediatePair biggest_pair;
    int biggest_thread = 0;
    for (int i =0; i < num_of_intermediate_pairs; i++)
    {
        findInterPair(jc, biggest_pair, biggest_thread);

        findBiggestPair(jc, biggest_pair, biggest_thread);

        addToShuffleVec(jc, biggest_pair, num_of_vec);

        (jc->sjc->threads_context[biggest_thread]).intermediateVec.pop_back();

        updateJobProgress(jc->sjc->data);
    }

    setJobState(context, REDUCE_STAGE);
    updateJobSize(jc->sjc->data, num_of_vec);

}

void* threadEntryPoint(void* context) {
    auto* jc = static_cast<JobContext*>(context);
    mapPhase(context);
    sortPhase(context);

    jc->sjc->barrier->barrier();

    // Only one thread can enter this block at a time
    pthread_mutex_lock(&(jc->sjc->shuffle_mutex));
    if (!(jc->sjc->shuffled)) {
//        std::cout << "Certain Job: Thread ID - " << jc->thread_id << std::endl;
        fflush(stdout);
        // Perform shuffle
        shufflePhase(context);
        (jc->sjc->shuffled) = true;
        pthread_mutex_unlock(&(jc->sjc->shuffle_mutex));
        pthread_mutex_destroy(&(jc->sjc->shuffle_mutex)); // TODO can cause bug?
    }
    reducePhase(context);
    // Terminate thread
    return EXIT_SUCCESS;
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel)
{
    // Make threads
    auto* sjc = new SharedJobContext(multiThreadLevel);
    auto* contexts = sjc->threads_context;
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        contexts[i] = ((JobContext) {&client, &inputVec, &outputVec, sjc, std::move(IntermediateVec()), multiThreadLevel});
    }

    // Beginning Map Phase
    setJobState((void*)contexts, MAP_STAGE);
    updateJobSize(contexts[0].sjc->data, inputVec.size());

    for (int i = 0; i < multiThreadLevel; ++i)
    {
      pthread_create (
              (sjc->threads) + i, nullptr, threadEntryPoint, (void *) (contexts + i));
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
    auto* jc = static_cast<JobContext*>(context);
    pthread_mutex_lock(&(jc->sjc->output_mutex));
    jc->outputVec->emplace_back(key, value);
    updateJobProgress(jc->sjc->data);
    pthread_mutex_unlock(&(jc->sjc->output_mutex));

}

// we can assume it will be called only from one thread
void waitForJob(JobHandle job)
{
    auto* jc = static_cast<JobContext*>(job);
    if (!(jc->sjc->waiting))
    {
        jc->sjc->waiting = true;
        for (int i = 0; i < jc->multiThreadLevel; ++i)
        {
            pthread_join((jc->sjc->threads)[i], nullptr);
        }
    }
}

// We can assume it will be called only once
void closeJobHandle(JobHandle job)
{
    auto* jc = static_cast<JobContext*>(job);
    waitForJob(job);

    // Delete shared pointers
    auto* sjc = jc->sjc;
    delete sjc;
//    // Delete jc
//    delete[] jc;
}

