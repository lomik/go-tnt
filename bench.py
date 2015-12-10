import time
import python_iproto

def benchmark_select_pack(N):
    values = [11, 12]
    for i in xrange(N):
        python_iproto.pack_select(10, values, offset=13, limit=14, index=15)

def benchmark_unpack_body(N):
    body = '\x00\x00\x00\x00\x01\x00\x00\x00\n\x00\x00\x00\x02\x00\x00\x00\x04\xa3QSq\x04\x02\x00\x00\x00'
    for i in xrange(N):
        python_iproto.unpack_body(body)


def run(func, N):
    start = time.time()
    func(N)
    end = time.time()
    print func.func_name, N, int((10**9)*(end-start)/N), "ns/op"

if __name__ == "__main__":
    run(benchmark_select_pack, 500000)
    run(benchmark_unpack_body, 300000)
