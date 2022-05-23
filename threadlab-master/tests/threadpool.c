#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdbool.h>
#include <pthread.h>
#include <threads.h>
#include "list.h"
#include "threadpool.h"

/**
 * threadpool.h
 *
 * A work-stealing, fork-join thread pool.
*/



static __thread struct wThread *current_thread;                          // thread-local variable. Points to the current thread itself.
typedef void *(*fork_join_task_t)(struct thread_pool *pool, void *data); //

/*
     struct for the threadpool
*/
struct thread_pool
{
    struct list global_list;      // global queue
    struct list threads_list;     // list of worker threads
    pthread_t *thread_IDs;        // array for thread IDs returned from pthread create
    pthread_mutex_t global_mutex; // lock for accessing global queue
    pthread_mutex_t pool_mutex;   // used to lock when acessing pool data
    pthread_cond_t to_work;       // signals a worker that there is available futures
    bool pool_down;               // conditional to see if pool is shut down(more of to see if there are active threads left, if not then its dead)

    int numTask;      // number of tasks
    int thread_count; // number of threads alive

};
typedef struct thread_pool thread_pool;

/*
    status of the furture
*/
enum future_status
{
    notStarted,
    inProgress,
    done
};

/*
    Struct for future object
*/
struct future
{
    fork_join_task_t task;     // typdef of a funtion pointer type to be executed
    thread_pool *pool;         // pool this future is under
    void *args;                // the data from thread_pool_ submit (used as args for the task i believe)
    void *result;              // stores task result once execution is completed
    sem_t task_done;           // used for ordering of tasks
    enum future_status status; // status of the future
    struct list_elem element;  // list element 
    struct wThread *parent;    // worker thread that created this future
};
typedef struct future future;

/*
    struct for a worker thread
*/
struct wThread
{
    struct list worker_task_list; // local queue of worker thread
    thread_pool *pool;            // pool this thread is under
    struct list_elem element;     // An element of the queue
    pthread_mutex_t local_mutex;  // lock for accesing the local queue
};
typedef struct wThread wThread;

static void *start_routine(void *arg);

/*
    Create a new thread pool with no more than n threads.
*/
struct thread_pool *thread_pool_new(int nthreads)
{

    thread_pool *pool = malloc(sizeof(thread_pool)); // create new pool
    list_init(&pool->global_list);                          // initialize global queue
    list_init(&pool->threads_list);                         // initialize list of threads
    pthread_mutex_init(&pool->global_mutex, NULL);          // initialize lock for aquiring global tasks
    pthread_cond_init(&pool->to_work, NULL);                // initialize future available
    pthread_mutex_init(&pool->pool_mutex, NULL);          // inititalize worker mutex
    pool->thread_count = nthreads;                          // initialize number of threads
    pool->pool_down = false;                                // pool is not down;
    pool->thread_IDs = calloc(nthreads, sizeof(pthread_t)); // initialize threads_ID array


    for (int i = 0; i < nthreads; i++)
    {
        wThread *worker = malloc(sizeof(wThread)); // create worker thread

        pthread_mutex_init(&worker->local_mutex, NULL);        // inititalize local queue lock
        list_init(&worker->worker_task_list);                  // inittialize local queue
       
        pthread_mutex_lock(&pool->pool_mutex);
        list_push_back(&pool->threads_list, &worker->element); // add to list of threads
        pthread_mutex_unlock(&pool->pool_mutex);

        pthread_create(&pool->thread_IDs[i], NULL, start_routine, pool);
    }


    return pool;
};

/*
    function that dictates what a worker thread should be doing
    Thread grabs a fututre from the available lists in the desired order and executes them
*/
static void *start_routine(void *arg)
{
    thread_pool *pool = arg;
    pthread_mutex_lock(&pool->pool_mutex); 


    // loop until the calling thread is found from the list
    struct list_elem *e;
    int i = 0;
    for (e = list_begin(&pool->threads_list); e != list_end(&pool->threads_list); e = list_next(e))
    {
        if (pthread_self() == pool->thread_IDs[i])
        {
            current_thread = list_entry(e, wThread, element); // initialize thread local variable to the current worker thread
            current_thread->pool = pool;
            break;
        }
        i++;
    }
    
    // loop in which a future is grabbed and the task is completed
    while (true)
    {
        while (pool->numTask == 0 && !pool->pool_down)
            pthread_cond_wait(&pool->to_work, &pool->pool_mutex); // wait till worker is signaled to work

        if (pool->pool_down)
        {
            break;
        }

        pthread_mutex_lock(&current_thread->local_mutex);
        struct list_elem *curr_elem = NULL;

        // check its own queue for tasks
        if (!list_empty(&current_thread->worker_task_list))
        {
            curr_elem = list_pop_front(&current_thread->worker_task_list); // get fututre from front of its own task list
            pthread_mutex_unlock(&current_thread->local_mutex);
        }
        else // worker's local queue is empty
        {
            pthread_mutex_unlock(&current_thread->local_mutex);
            pthread_mutex_lock(&pool->global_mutex);
            // check the global queue for a task
            if (!list_empty(&pool->global_list))
            {
                curr_elem = list_pop_back(&pool->global_list); // get future from back of global queue
                pthread_mutex_unlock(&pool->global_mutex);
            }
            else // global queue is also empty
            {
                pthread_mutex_unlock(&pool->global_mutex);

                wThread *w;
                struct list_elem *e;
                // loop throw the list of workers 
                for (e = list_begin(&pool->threads_list); e != list_end(&pool->threads_list); e = list_next(e))
                {
                    w = list_entry(e, wThread, element);
                    pthread_mutex_lock(&w->local_mutex);        // lock belonging to the worker grabbed from the list

                    // check other queue for work stealing
                    if (!list_empty(&w->worker_task_list))
                    {
                        curr_elem = list_pop_back(&w->worker_task_list);
                        pthread_mutex_unlock(&w->local_mutex);
                        break;
                    }
                    pthread_mutex_unlock(&w->local_mutex);
                }
            }
        }
        if (curr_elem != NULL)
        {
            
            future *curr_future = list_entry(curr_elem, future, element);
            curr_future->status = inProgress;
            pool->numTask--;
            pthread_mutex_unlock(&pool->pool_mutex);
            // do the task
            curr_future->result = curr_future->task(pool, curr_future->args);

            pthread_mutex_lock(&pool->pool_mutex);
            curr_future->status = done;
            pthread_mutex_unlock(&pool->pool_mutex);
            sem_post(&curr_future->task_done);

            pthread_mutex_lock(&pool->pool_mutex);
        }
        else{
            pthread_mutex_lock(&pool->pool_mutex);
        }
        
    }

    pthread_mutex_unlock(&pool->pool_mutex);

    return NULL;
}

/*
 * Shutdown this thread pool in an orderly fashion.
 * Tasks that have been submitted but not executed may or
 * may not be executed.
 *
 * Deallocate the thread pool object before returning.
 */
void thread_pool_shutdown_and_destroy(struct thread_pool *pool)
{
    pthread_mutex_lock(&pool->pool_mutex);   // lock pool
                                               //
    pool->pool_down = true;                    // pool is down
    pthread_cond_broadcast(&pool->to_work);    // release all workers
                                               //
    int n = pool->thread_count;
    pthread_mutex_unlock(&pool->pool_mutex); // unlock pool

    for (int i = 0; i < n; i++)
    {
        pthread_join(pool->thread_IDs[i], NULL); // joins threads
                                                 
    }
    for (int i = 0; i < n; i++)
    {
        struct list_elem *e = list_pop_front(&pool->threads_list);
        if (e != NULL)
        {
            wThread *w = list_entry(e, wThread, element); // get worker thread from the threads list in pool
            pthread_mutex_destroy(&w->local_mutex); // destroy mutex
            free(w); // free worker
        }
    }


    free(pool->thread_IDs);
    free(pool);
}

/*
 * Submit a fork join task to the thread pool and return a
 * future. The returned future can be used in future_get()
 * to obtain the result.
 * ’pool’ - the pool to which to submit
 * ’task’ - the task to be submitted.
 * ’data’ - data to be passed to the task’s function
 *
 * Returns a future representing this computation.
 */
struct future *thread_pool_submit(struct thread_pool *pool, fork_join_task_t task, void *data)
{

    pthread_mutex_lock(&pool->pool_mutex); 
    struct future *future;

    if (task == NULL)
        return NULL;

    future = malloc(sizeof(*future));   // allocate memory to future
    sem_init(&future->task_done, 0, 0); // initialize semaphore
    future->status = notStarted;        // initialize status of future
    future->task = task;                // set task
    future->args = data;                // set arguments
    future->result = NULL;              // set return value
    future->pool = pool;                // inititalize pool
                                        //
    if (current_thread != NULL)
    { // internal
        pthread_mutex_lock(&current_thread->local_mutex); // lock local lock
        list_push_front(&current_thread->worker_task_list, &future->element);
        pthread_mutex_unlock(&current_thread->local_mutex); // unlock local lock

    }
    else
    { // external
        pthread_mutex_lock(&pool->global_mutex); // lock global lock
        list_push_back(&pool->global_list, &future->element);
        pthread_mutex_unlock(&pool->global_mutex); // unlock global lock
        
    }
    pool->numTask++;                           //
    pthread_cond_signal(&pool->to_work);       // tell a worker thread there is work available
    pthread_mutex_unlock(&pool->pool_mutex); // unlock global lock

    return future;
}

/* Make sure that the thread pool has completed the execution
 * of the fork join task this future represents.
 *
 * Returns the value returned by this task.
 */
void *future_get(struct future *f)
{
    pthread_mutex_lock(&f->pool->pool_mutex);
    

    if (f->status == notStarted)
    {
        list_remove(&f->element);                     // remove from list
        f->pool->numTask--;                           // decriment number of tasks in pool
        f->status = inProgress;                       // set status to inprogress
        pthread_mutex_unlock(&f->pool->pool_mutex); // unlock pool lock
        f->result = f->task(f->pool, f->args);        // set results for f

        f->status = done; // update status
        sem_post(&f->task_done); // post that result is available
    }
    else if (f->status == inProgress)
    {
        pthread_mutex_unlock(&f->pool->pool_mutex); // unlock pool lock
        sem_wait(&f->task_done);
    }
    else
    {
        pthread_mutex_unlock(&f->pool->pool_mutex); // unlock pool lock
    }

    
    return f->result;
}

/*
    Deallocate this future. Must be called after future_get()
*/
void future_free(struct future *f)
{
    if (f == NULL)
        return;
    free(f);
}

