#include <iostream>
#include <vector>
#include <queue>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <memory>
#include <cstdint>

struct activation_record;
class execution_queue;
class allocation_queue;

struct thunk
{
    virtual void execute(std::shared_ptr<activation_record> record, execution_queue& eq, allocation_queue& aq) = 0;
};


struct activation_record
{
    virtual ~activation_record() {}

    activation_record* return_address;
    thunk* continuation;

    virtual void activate(execution_queue& eq, allocation_queue& aq) = 0;
};

class execution_queue
{
public:
    virtual void enqueue_activation_record(std::shared_ptr<activation_record>& request) = 0;
    virtual std::shared_ptr<activation_record> dequeue_activation_record() = 0;
};

class concrete_execution_queue : public execution_queue
{
    std::mutex mut;
    std::condition_variable cv;
    std::queue<std::shared_ptr<activation_record>> requests;

public:
    void enqueue_activation_record(std::shared_ptr<activation_record>& request)
    {
        std::unique_lock<std::mutex> lock(mut);
        requests.push(request);
        cv.notify_one();
    }

    std::shared_ptr<activation_record> dequeue_activation_record()
    {
        std::unique_lock<std::mutex> lock(mut);
        cv.wait(lock, [&] { return !requests.empty(); });
        return requests.front();
    }
};

enum class allocation_operation : std::uint8_t
{
    ALLOCATE,
    FREE,
    EXIT
};

struct allocation_request
{
    allocation_operation operation;
    unsigned allocation_size;
    std::vector<std::uint8_t>* allocation_location;
    execution_queue& continuation_queue;
    std::shared_ptr<activation_record> next;
};

class allocation_queue
{
public:
    virtual void enqueue_request(const allocation_request& request) = 0;
    virtual allocation_request dequeue_request() = 0;
};

class concrete_allocation_queue : public allocation_queue
{
    std::mutex mut;
    std::condition_variable cv;
    std::queue<allocation_request> requests;

public:
    void enqueue_request(const allocation_request& request)
    {
        std::unique_lock<std::mutex> lock(mut);
        requests.push(request);
        cv.notify_one();
    }
    allocation_request dequeue_request()
    {
        std::unique_lock<std::mutex> lock(mut);
        cv.wait(lock, [&] { return !requests.empty(); });
        return requests.front();
    }
};

void memory_manager_main_loop(allocation_queue& aq)
{
    for (;;)
    {
        auto request = aq.dequeue_request();
        if (request.operation == allocation_operation::EXIT)
        {
            return;
        }
        else if (request.operation == allocation_operation::ALLOCATE)
        {
            if (request.allocation_location)
            {
                *(request.allocation_location) = std::vector<uint8_t>(request.allocation_size, 0);
                request.continuation_queue.enqueue_activation_record(request.next);
            }
        }
        else if (request.operation == allocation_operation::FREE)
        {
            if (request.allocation_location)
            {
                (request.allocation_location)->resize(0);
                (request.allocation_location)->shrink_to_fit();

                request.continuation_queue.enqueue_activation_record(request.next);
            }
        }

        //otherwise who cares?
    }
}

bool g_bExit = false;

void thread_main_loop(execution_queue& eq, allocation_queue& aq)
{
    while(!g_bExit)
    {
        auto request = eq.dequeue_activation_record();
        request->activate(eq, aq);
    }
}


class program_exit : public thunk
{
    void execute(std::shared_ptr<activation_record> record, execution_queue& eq, allocation_queue& aq)
    {
        g_bExit = true;
        aq.enqueue_request({allocation_operation::EXIT, 0, nullptr, eq, });
    }
};

class program_start : public thunk
{
public:
    void execute(activation_record* record, execution_queue& eq, allocation_queue& aq)
    {
    }
};

int main()
{

    concrete_execution_queue eq;
    concrete_allocation_queue aq;
    std::thread memory_manager_thread([&]() {memory_manager_main_loop(aq); });
    thread_main_loop(eq, aq);

    memory_manager_thread.join();
}
