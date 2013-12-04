"""Map drugs to ingredients in parallel on PiCloud.

Uses the SOAP interface of RxNav.

"""

import os
# import itertools
# import cloud
# from drug_names import map_drug_name


# # map this many at a time
# CHUNK_SIZE = 30

# # picloud has a limit on the number of items that can be mapped
# STEP_SIZE = 300


# with open('unique_drugs.txt') as f:
#     NAMES = f.read().split('\n')

# N_NAMES = len(NAMES)
# DIGITS = len(str(N_NAMES))


# def chunk_list(lst, chunk_size):
#     """Split a list into multiple lists of length chunk_size"""
#     return [lst[i: i + chunk_size]
#             for i in range(0, len(lst), chunk_size)]


# def function_dechunker(func):
#     """Return a function that processes ``func`` over a list of elements"""
#     def wrapper_inner(chunked_args):
#         # Regular Python map runs serially within job
#         return map(func, chunked_args)
#     return wrapper_inner


def save_results(filename, names, results):
    with open('./results/{}'.format(filename), 'w') as f:
        f.write("name|cuid|ingred_cuid|ingred\n")
        for n, m in zip(names, results):
            cuid, ingred_cuid, ingred = m
            line = map(str, [n, cuid, ingred_cuid, ingred])
            f.write("|".join(line))
            f.write("\n")


# PREV_DONE = list(os.path.splitext(f)[0].split('_')[1]
#                  for f in os.listdir('./results'))
# PREV_DONE.remove('final')
# PREV_DONE = map(int, PREV_DONE)


# for i in range(0, N_NAMES, STEP_SIZE):
#     if i in PREV_DONE:
#         continue
#     try:
#         print "processing {} to {} of {}".format(i, i + STEP_SIZE, N_NAMES)
#         name_chunk = NAMES[i: i + STEP_SIZE]
#         jids = cloud.map(function_dechunker(map_drug_name),
#                          chunk_list(name_chunk, CHUNK_SIZE), _type="s1")

#         # a list of lists is returned
#         chunked_results = cloud.result(jids)

#         # merge lists to mimic non-chunked example
#         results = list(itertools.chain.from_iterable(chunked_results))

#         # save current results, just in case
#         save_results('results_{}.txt'.format(str(i).zfill(DIGITS)),
#                      name_chunk, results)

#     except:
#         print "exception occurred. skipping to next chunk."


# combine individual files
DONE = list(os.path.splitext(f)[0].split('_')[1]
            for f in os.listdir('./results'))
if 'final' in DONE:
    DONE.remove('final')

FINAL_NAMES = []
ALL_RESULTS = []
for i in sorted(DONE):
    with open("results/results_{}.txt".format(i)) as f:
        for line in f.read().split('\n')[1:]:
            if len(line) == 0:
                continue
            line = line.split('\t')
            if len(line) > 4:
                a = ' '.join(line[:-3])
                b, c, d = line[-3:]
            else:
                a, b, c, d = line
            FINAL_NAMES.append(a)
            ALL_RESULTS.append((b, c, d))

save_results('results_final.txt', FINAL_NAMES, ALL_RESULTS)
