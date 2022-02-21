import os
from data_processor import DataLoader as dl
from pattern import joiner
from result_util import parser
from evaluator.JoinEval import JoinEval


import pathlib
BASE_PATH = str(pathlib.Path(__file__).absolute().parent.parent.absolute())

MIN_SUPPORT = 0.05

db_path = BASE_PATH + '/data/autojoin-Benchmark'
# db_path = BASE_PATH + '/data/four_synthetic_10_runs/'
# db_path = BASE_PATH + '/data/FlashFill/'

tr_path = BASE_PATH + "/output/coronation/PT_BM_RM"
# tr_path = BASE_PATH + "/output/coronation/PT_SYN_ALL_FOUR_RM/"
# tr_path = BASE_PATH + "/output/coronation/PT_FF_P3_RM_n3/"


# Terminal output is csv data, we can pip it to csv file
def main():
    from glob import glob
    print("Name,P,R,F1")
    for f_name in glob(tr_path + '/*.txt'):
        tbl = str(os.path.basename(f_name))[:-4]

        src, target, gt = dl.basic_loader(db_path + '/' + tbl, make_lower=True)
        avg_src = sum(len(c) for c in src) / len(src)
        avg_target = sum(len(c) for c in target) / len(target)
        is_swapped = avg_target > avg_src

        trans = parser.parse_transformations(f_name, min_support=MIN_SUPPORT)
        joins = joiner.join(src, target, trans, is_swapped)
        je = JoinEval(joins, gt)
        print(f"{tbl},{je.precision},{je.recall},{je.f1}")


if __name__ == "__main__":
    main()



## Opendata
'''
db_path = BASE_PATH + '/data/servus/proc-dataset'
tr_path = BASE_PATH + "/output/coronation/PT_Servus_Sample_P3_RM/proc-dataset_s-3000.txt"

print("Started Loading...")
src, target, gt = dl.basic_loader(db_path, make_lower=True)
print("Loading Done!")
print(len(src), len(target))

avg_src = sum(len(c) for c in src) / len(src)
avg_target = sum(len(c) for c in target) / len(target)
is_swapped = avg_target > avg_src
print("swapping done.")

trans = parser.parse_transformations(tr_path, min_support=0.002)
print("Parsing transformation done.")

joins = joiner.join(src, target, trans, is_swapped, verbose=True)
print("Joining Done.")

je = JoinEval(joins, gt)
print("Name,P,R,F1")
print(f"{str(os.path.basename(tr_path))[:-4]},{je.precision},{je.recall},{je.f1}")
'''

