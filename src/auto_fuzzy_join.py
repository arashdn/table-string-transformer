import pandas as pd
import os
from data_processor import DataLoader as dl
from evaluator.JoinEval import JoinEval
import pathlib


BASE_PATH = str(pathlib.Path(__file__).absolute().parent.parent.absolute())

# Path to dataset (in our dataset format)
DB_PATH = BASE_PATH + '/data/autojoin-Benchmark/'
VERBOSE = False

'''
Install Auto-Fuzzy-join: https://github.com/chu-data-lab/AutomaticFuzzyJoin
Terminal output is csv data, we can pip it to csv file
'''

if __name__ == "__main__":  # essential for afj

    print("Name,P,R,F1")
    dirs = [dI for dI in os.listdir(DB_PATH) if os.path.isdir(os.path.join(DB_PATH, dI))]
    for db in dirs:
        src, target, gt = dl.basic_loader(DB_PATH+db, make_lower=True)

        src_lst = [ [idx, s] for idx, s in enumerate(src)]
        src_df = pd.DataFrame(src_lst, columns=['id', 'title'])

        src_lst = [ [idx, s] for idx, s in enumerate(src)]
        src_map = {sl[0]: sl[1] for sl in src_lst}
        src_df = pd.DataFrame(src_lst, columns=['id', 'title'])

        target_lst = [ [idx, s] for idx, s in enumerate(target)]
        target_map = {sl[0]: sl[1] for sl in target_lst}
        target_df = pd.DataFrame(target_lst, columns=['id', 'title'])

        from autofj import AutoFJ
        '''
                "autofj_sm": autofj_sm,
                "autofj_md": autofj_md,
                "autofj_lg": autofj_lg,
        '''
        fj = AutoFJ(precision_target=0.9, n_jobs=1, verbose=VERBOSE)
        result = fj.join(src_df, target_df, id_column="id")

        LR_joins = result[["id_l", "id_r"]].values

        joined = [(src_map[j[0]], target_map[j[1]]) for j in LR_joins]

        je = JoinEval(joined, gt)
        print(f"{db},{je.precision},{je.recall},{je.f1}")






# Using AFJ dataset:
'''
if __name__ == '__main__':
    left_table, right_table, gt_table = load_data("Amphibian")

    fj = AutoFJ(precision_target=0.9, n_jobs=1, verbose=True)
    result = fj.join(left_table, right_table, id_column="id")


    gt_joins = gt_table[["id_l", "id_r"]].values
    LR_joins = result[["id_l", "id_r"]].values

    golden = [(gt[0], gt[1]) for gt in gt_joins]
    joined = [(j[0], j[1]) for j in LR_joins]

    je = JoinEval(joined, golden, case_sensitive=True)
    print(je)
'''