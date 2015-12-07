import time
import python_iproto

def benchmark_select_pack(N):
    values = [11, 12]
    for i in xrange(N):
        python_iproto.pack_select(10, values, offset=13, limit=14, index=15)


def run(func, N):
    start = time.time()
    func(N)
    end = time.time()
    print func.func_name, N, int((10**9)*(end-start)/N), "ns/op"

if __name__ == "__main__":
    run(benchmark_select_pack, 500000)
