import simulatools
import pprint

def main():
    num_of_caches = len(simulatools.Trace.l_normal_single_32.typical_caches())
    for i in range(num_of_caches):
        retcode = simulatools.single_run('windowca', 'l_normal_single_32', size=i+1, verbose=True)
        if retcode is False:
            return 1
    # results = { simulatools.Trace.multi1.typical_caches()[i-1] :
    #             simulatools.single_run('lru', 'multi3', i)
    #             for i in range(1,1+8)
    #           }
    # pprint.pprint(results)

if __name__ == "__main__":
    main()

