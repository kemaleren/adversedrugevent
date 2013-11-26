"""Map drugs to ingredients in parallel on PiCloud.

Uses the SOAP interface of RxNav.

"""

import itertools
import cloud
from drug_names import map_drug_name


# map this many at a time
CHUNK_SIZE = 10

# picloud has a limit on the number of items that can be mapped
STEP_SIZE = 1000

# save all results as they come
ALL_RESULTS = []


with open('unique_drugs.txt') as f:
    NAMES = f.read().split('\n')

N_NAMES = len(NAMES)


def chunk_list(lst, chunk_size):
    """Split a list into multiple lists of length chunk_size"""
    return [lst[i: i + chunk_size]
            for i in range(0, len(lst), chunk_size)]


def function_dechunker(func):
    """Return a function that processes ``func`` over a list of elements"""
    def wrapper_inner(chunked_args):
        # Regular Python map runs serially within job
        return map(func, chunked_args)
    return wrapper_inner


def save_results(filename, names, results):
    with open(filename, 'w') as f:
        f.write("name	cuid	ingred_cuid	ingred\n")
        for n, m in zip(names, results):
            cuid, ingred_cuid, ingred = m
            line = map(str, [n, cuid, ingred_cuid, ingred])
            f.write("	".join(line))
            f.write("\n")


for i in range(0, N_NAMES, STEP_SIZE):
    print "processing {} to {} of {}".format(i, i + STEP_SIZE, N_NAMES)
    name_chunk = NAMES[i: i + STEP_SIZE]
    jids = cloud.map(function_dechunker(map_drug_name),
                     chunk_list(name_chunk, CHUNK_SIZE), _type="s1")

    # a list of lists is returned
    chunked_results = cloud.result(jids)

    # merge lists to mimic non-chunked example
    results = list(itertools.chain.from_iterable(chunked_results))

    # save current results, just in case
    save_results('results_{}.txt'.format(i), name_chunk, results)

    # append to running total
    ALL_RESULTS.extend(results)


save_results('results_final.txt', NAMES, ALL_RESULTS)
